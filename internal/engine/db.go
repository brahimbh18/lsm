package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"lsm/internal/memtable"
	"lsm/internal/storage"
	"lsm/internal/storage/sstable"
	"lsm/internal/wal"
)

var ErrClosed = errors.New("database is closed")

type memTableState struct {
	id        uint64
	table     *memtable.MemTable
	wal       *wal.WAL
	walClosed bool
}

type flushTask struct {
	state *memTableState
}

type sstableStorage interface {
	WriteWithInfo(*sstable.SSTable) (storage.WriteResult, error)
}

type FlushStats struct {
	RecordsWritten     int
	BytesWritten       int64
	MemTablesCreated   int
	MemTablesFrozen    int
	MemTablesFlushed   int
	L0SSTablesCreated  int
	SSTableBytes       int64
	TotalSSTableBlocks int
}

type DB struct {
	mu              sync.RWMutex
	queueMu         sync.Mutex
	active          *memTableState
	frozenMemtables []*flushTask
	flushQueue      chan *flushTask
	flushWorker     sync.WaitGroup
	flushBuilder    *sstable.Builder
	flushStorage    sstableStorage
	flushDone       chan struct{}
	flushErr        error
	flushCond       *sync.Cond
	nextMemtableID  uint64
	stats           FlushStats
	options         Options
	closed          bool
	lastSequence    uint64
	flushPending    int
}

func Open(options Options) (*DB, error) {
	if options.DataDir == "" {
		options.DataDir = "data"
	}
	walDir := filepath.Join(options.DataDir, "wal")
	tablesDir := filepath.Join(options.DataDir, "tables")
	if options.MemTableMaxSize <= 0 {
		options.MemTableMaxSize = DefaultMemTableMaxSize
	}

	if err := os.MkdirAll(walDir, 0755); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	if err := os.MkdirAll(tablesDir, 0755); err != nil {
		return nil, fmt.Errorf("create tables directory: %w", err)
	}
	walIDs, err := walIDs(walDir)
	if err != nil {
		return nil, err
	}
	sstIDs, err := sstableIDs(tablesDir)
	if err != nil {
		return nil, err
	}
	nextID := maxID(walIDs, mapIDs(sstIDs)) + 1

	db := &DB{
		flushQueue:     make(chan *flushTask, 1),
		flushBuilder:   sstable.NewBuilder(),
		flushDone:      make(chan struct{}, 1),
		flushStorage:   nil,
		nextMemtableID: nextID,
		options:        options,
	}
	db.flushStorage, err = storage.NewStorage(tablesDir)
	if err != nil {
		return nil, err
	}
	db.flushCond = sync.NewCond(&db.mu)

	for id := range sstIDs {
		data, err := os.ReadFile(filepath.Join(tablesDir, fmt.Sprintf("%06d.sst", id)))
		if err != nil {
			return nil, err
		}
		sequence, err := sstable.MaxSequence(data)
		if err != nil {
			return nil, fmt.Errorf("read SSTable %d: %w", id, err)
		}
		if sequence > db.lastSequence {
			db.lastSequence = sequence
		}
	}

	// Replay every WAL that has no corresponding SSTable. Until SSTable reads
	// exist, replaying them into the active table is the explicit recovery limit.
	for _, id := range walIDs {
		if _, ok := sstIDs[id]; ok {
			continue
		}
		path := walPath(walDir, id)
		recovered, err := wal.Open(path)
		if err != nil {
			return nil, err
		}
		if db.active == nil {
			db.active, err = db.newState(id, recovered)
			if err != nil {
				return nil, err
			}
			err = recovered.Replay(func(entry memtable.Entry) error {
				if entry.Seq > db.lastSequence {
					db.lastSequence = entry.Seq
				}
				return db.active.table.PutEntry(entry)
			})
			if err != nil {
				_ = recovered.Close()
				return nil, err
			}
		} else {
			err = recovered.Replay(func(entry memtable.Entry) error {
				if entry.Seq > db.lastSequence {
					db.lastSequence = entry.Seq
				}
				return db.active.table.PutEntry(entry)
			})
			if err != nil {
				_ = recovered.Close()
				return nil, err
			}
			_ = recovered.Close()
		}
	}
	if db.active == nil {
		db.active, err = db.newState(nextID, nil)
		if err != nil {
			return nil, err
		}
		db.nextMemtableID = nextID + 1
	} else {
		db.nextMemtableID = maxID([]uint64{db.active.id + 1}, nil)
	}
	db.stats.MemTablesCreated = 1
	db.log(1, "[DB] OPEN")
	db.log(1, "[WAL] CREATE path=%s", db.active.wal.Path())
	db.log(1, "[MEMTABLE] CREATE id=%d wal=%s", db.active.id, db.active.wal.Path())
	db.startFlushWorker()
	return db, nil
}

func (db *DB) newState(id uint64, existing *wal.WAL) (*memTableState, error) {
	current := existing
	if current == nil {
		var err error
		current, err = wal.Open(walPath(filepath.Join(db.options.DataDir, "wal"), id))
		if err != nil {
			return nil, err
		}
	}
	return &memTableState{id: id, table: memtable.New(db.options.MemTableMaxSize), wal: current}, nil
}

func (db *DB) startFlushWorker() {
	db.flushWorker.Add(1)
	go func() {
		defer db.flushWorker.Done()
		for task := range db.flushQueue {
			db.log(1, "[FLUSH WORKER] RECEIVED memtable=%d wal=%s", task.state.id, task.state.wal.Path())
			err := db.flush(task)

			db.mu.Lock()
			if err != nil && !task.state.walClosed {
				_ = task.state.wal.Close()
				task.state.walClosed = true
			}
			if err != nil && db.flushErr == nil {
				db.flushErr = err
			}
			if err == nil {
				db.stats.MemTablesFlushed++
				db.stats.L0SSTablesCreated++
			}
			db.flushPending--
			db.flushCond.Broadcast()
			db.mu.Unlock()
			select {
			case db.flushDone <- struct{}{}:
			default:
			}
		}
	}()
}

func (db *DB) Put(key, value []byte) error {
	return db.write(key, value, false)
}

func (db *DB) Delete(key []byte) error {
	return db.write(key, nil, true)
}

func (db *DB) write(key, value []byte, tombstone bool) error {
	db.queueMu.Lock()
	defer db.queueMu.Unlock()
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrClosed
	}
	if db.flushErr != nil {
		err := db.flushErr
		db.mu.Unlock()
		return err
	}
	if db.lastSequence == ^uint64(0) {
		db.mu.Unlock()
		return ErrSequenceOverflow
	}
	db.lastSequence++
	sequence := db.lastSequence
	entry := memtable.Entry{Key: key, Value: value, Seq: sequence, Tombstone: tombstone}
	if err := db.active.wal.AppendEntry(entry); err != nil {
		db.mu.Unlock()
		return err
	}
	if db.options.SyncMode == SyncModeSync {
		if err := db.active.wal.Sync(); err != nil {
			db.mu.Unlock()
			return err
		}
	}
	if err := db.active.table.PutEntry(entry); err != nil {
		db.mu.Unlock()
		return err
	}
	db.log(2, "[WRITE] key=%s wal=%s", key, db.active.wal.Path())
	db.stats.RecordsWritten++
	db.stats.BytesWritten += int64(len(key) + len(value))
	var frozen *flushTask
	if db.active.table.IsFull() {
		frozen = &flushTask{state: db.active}
		db.active.table.Freeze()
		db.frozenMemtables = append(db.frozenMemtables, frozen)
		db.flushPending++
		db.stats.MemTablesFrozen++
		db.log(1, "[MEMTABLE] FULL id=%d size=%d threshold=%d", db.active.id, db.active.table.Size(), db.options.MemTableMaxSize)
		db.log(1, "[MEMTABLE] FREEZE id=%d wal=%s entries=%d", db.active.id, db.active.wal.Path(), len(db.active.table.Entries()))
		var err error
		db.active, err = db.newState(db.nextMemtableID, nil)
		if err != nil {
			db.mu.Unlock()
			return err
		}
		db.nextMemtableID++
		db.stats.MemTablesCreated++
		db.log(1, "[MEMTABLE] CREATE id=%d wal=%s", db.active.id, db.active.wal.Path())
	}
	db.mu.Unlock()
	if frozen != nil {
		db.log(1, "[FLUSH QUEUE] ENQUEUE memtable=%d queue_length=%d", frozen.state.id, len(db.flushQueue)+1)
		db.flushQueue <- frozen
	}
	return nil
}

func (db *DB) Get(key []byte) ([]byte, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if entry, ok := db.active.table.Lookup(key); ok {
		if entry.Tombstone {
			return nil, false
		}
		return entry.Value, true
	}
	for index := len(db.frozenMemtables) - 1; index >= 0; index-- {
		if entry, ok := db.frozenMemtables[index].state.table.Lookup(key); ok {
			if entry.Tombstone {
				return nil, false
			}
			return entry.Value, true
		}
	}
	return nil, false
}

func (db *DB) flush(task *flushTask) error {
	start := time.Now()
	db.log(1, "[FLUSH] START memtable=%d wal=%s", task.state.id, task.state.wal.Path())
	blocks := sstable.SplitBlocks(task.state.table.Entries())
	sstable, err := db.flushBuilder.Build(task.state.table)
	if err != nil {
		return err
	}
	db.log(1, "[SSTABLE BUILDER] DONE level=L0 blocks=%d size=%d", len(blocks), len(sstable.Data))
	result, err := db.flushStorage.WriteWithInfo(sstable)
	if err != nil {
		return err
	}
	if err := task.state.wal.Close(); err != nil {
		return err
	}
	db.mu.Lock()
	task.state.walClosed = true
	db.mu.Unlock()
	if err := os.Remove(task.state.wal.Path()); err != nil {
		return err
	}
	db.mu.Lock()
	db.stats.SSTableBytes += result.Size
	db.stats.TotalSSTableBlocks += len(blocks)
	db.mu.Unlock()
	db.log(1, "[SSTABLE] WRITE level=L0 file=%s size=%d", result.Path, result.Size)
	db.log(1, "[WAL] CLOSE+DELETE path=%s", task.state.wal.Path())
	db.log(1, "[FLUSH] DONE memtable=%d duration=%s", task.state.id, time.Since(start))
	return nil
}

func (db *DB) log(level int, format string, args ...any) {
	if db.options.DebugLogger != nil && db.options.DebugLevel >= level {
		db.options.DebugLogger.Printf(format, args...)
	}
}

func (db *DB) WaitForFlushes() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	for db.flushPending > 0 {
		db.flushCond.Wait()
	}
	return db.flushErr
}

func (db *DB) FlushStats() FlushStats {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.stats
}

func (db *DB) Close() error {
	db.queueMu.Lock()
	defer db.queueMu.Unlock()
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil
	}
	db.closed = true
	close(db.flushQueue)
	db.mu.Unlock()
	db.flushWorker.Wait()
	db.mu.Lock()
	flushErr := db.flushErr
	activeErr := error(nil)
	if db.active != nil && db.active.wal != nil {
		activeErr = db.active.wal.Close()
	}
	for _, task := range db.frozenMemtables {
		if task.state.walClosed {
			continue
		}
		if err := task.state.wal.Close(); activeErr == nil && err != nil {
			activeErr = err
		}
		task.state.walClosed = true
	}
	db.mu.Unlock()
	return errors.Join(flushErr, activeErr)
}

func walPath(directory string, id uint64) string {
	return filepath.Join(directory, fmt.Sprintf("wal-%06d.log", id))
}

func walIDs(directory string) ([]uint64, error) {
	return numberedFiles(directory, "wal-", ".log")
}

func sstableIDs(directory string) (map[uint64]bool, error) {
	ids, err := numberedFiles(directory, "", ".sst")
	result := make(map[uint64]bool, len(ids))
	for _, id := range ids {
		result[id] = true
	}
	return result, err
}

func numberedFiles(directory, prefix, suffix string) ([]uint64, error) {
	entries, err := os.ReadDir(directory)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	ids := make([]uint64, 0)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || filepath.Ext(name) != suffix || len(name) <= len(prefix)+len(suffix) || name[:len(prefix)] != prefix {
			continue
		}
		id, err := strconv.ParseUint(name[len(prefix):len(name)-len(suffix)], 10, 64)
		if err == nil {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

func maxID(groups ...[]uint64) uint64 {
	var max uint64
	for _, group := range groups {
		for _, id := range group {
			if id > max {
				max = id
			}
		}
	}
	return max
}

func mapIDs(ids map[uint64]bool) []uint64 {
	result := make([]uint64, 0, len(ids))
	for id := range ids {
		result = append(result, id)
	}
	return result
}
