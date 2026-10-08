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
	"lsm/internal/wal"
)

var ErrClosed = errors.New("database is closed")

type memTableState struct {
	id    uint64
	table *memtable.MemTable
	wal   *wal.WAL
}

type flushTask struct {
	state *memTableState
}

type sstableStorage interface {
	WriteWithInfo(*storage.SSTable) (storage.WriteResult, error)
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
	mu              sync.Mutex
	queueMu         sync.Mutex
	active          *memTableState
	frozenMemtables []*flushTask
	flushQueue      chan *flushTask
	flushWorker     sync.WaitGroup
	flushBuilder    *storage.Builder
	flushStorage    sstableStorage
	flushDone       chan struct{}
	flushErr        error
	flushCond       *sync.Cond
	nextMemtableID  uint64
	stats           FlushStats
	options         Options
	closed          bool
}

func Open(options Options) (*DB, error) {
	if options.WALDir == "" {
		if options.WALPath != "" {
			options.WALDir = filepath.Dir(options.WALPath)
		} else {
			options.WALDir = "data"
		}
	}
	if options.DataDir == "" {
		options.DataDir = filepath.Join(options.WALDir, "tables")
	}
	if options.MemTableMaxSize <= 0 {
		options.MemTableMaxSize = DefaultMemTableMaxSize
	}

	if err := os.MkdirAll(options.WALDir, 0755); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	if err := os.MkdirAll(options.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("create tables directory: %w", err)
	}
	walIDs, err := walIDs(options.WALDir)
	if err != nil {
		return nil, err
	}
	sstIDs, err := sstableIDs(options.DataDir)
	if err != nil {
		return nil, err
	}
	nextID := maxID(walIDs, mapIDs(sstIDs)) + 1

	db := &DB{
		flushQueue:     make(chan *flushTask, 1),
		flushBuilder:   storage.NewBuilder(),
		flushDone:      make(chan struct{}, 1),
		flushStorage:   nil,
		nextMemtableID: nextID,
		options:        options,
	}
	db.flushStorage, err = storage.NewStorage(options.DataDir)
	if err != nil {
		return nil, err
	}
	db.flushCond = sync.NewCond(&db.mu)

	// Replay every WAL that has no corresponding SSTable. Until SSTable reads
	// exist, replaying them into the active table is the explicit recovery limit.
	for _, id := range walIDs {
		if _, ok := sstIDs[id]; ok {
			continue
		}
		path := walPath(options.WALDir, id)
		recovered, err := wal.Open(path)
		if err != nil {
			return nil, err
		}
		if db.active == nil {
			db.active, err = db.newState(id, recovered)
			if err != nil {
				return nil, err
			}
			err = recovered.Replay(func(key, value []byte) error {
				return db.active.table.Put(key, value)
			})
			if err != nil {
				_ = recovered.Close()
				return nil, err
			}
		} else {
			err = recovered.Replay(func(key, value []byte) error {
				return db.active.table.Put(key, value)
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
		current, err = wal.Open(walPath(db.options.WALDir, id))
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
			if err != nil {
				_ = task.state.wal.Close()
			}

			db.mu.Lock()
			db.removeFrozenMemtable(task)
			if err != nil && db.flushErr == nil {
				db.flushErr = err
			}
			if err == nil {
				db.stats.MemTablesFlushed++
				db.stats.L0SSTablesCreated++
			}
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
	if err := db.active.wal.Append(key, value); err != nil {
		db.mu.Unlock()
		return err
	}
	if db.options.SyncMode == SyncModeSync {
		if err := db.active.wal.Sync(); err != nil {
			db.mu.Unlock()
			return err
		}
	}
	if err := db.active.table.Put(key, value); err != nil {
		db.mu.Unlock()
		return err
	}
	db.log(2, "[WRITE] key=%s wal=%s", key, db.active.wal.Path())
	db.stats.RecordsWritten++
	db.stats.BytesWritten += int64(len(key) + len(value))
	var frozen *flushTask
	if db.active.table.IsFull() {
		frozen = &flushTask{state: db.active}
		db.frozenMemtables = append(db.frozenMemtables, frozen)
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
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.active.table.Get(key)
}

func (db *DB) flush(task *flushTask) error {
	start := time.Now()
	db.log(1, "[FLUSH] START memtable=%d wal=%s", task.state.id, task.state.wal.Path())
	blocks := storage.SplitBlocks(task.state.table.Entries())
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

func (db *DB) removeFrozenMemtable(task *flushTask) {
	for i, frozen := range db.frozenMemtables {
		if frozen == task {
			db.frozenMemtables = append(db.frozenMemtables[:i], db.frozenMemtables[i+1:]...)
			return
		}
	}
}

func (db *DB) log(level int, format string, args ...any) {
	if db.options.DebugLogger != nil && db.options.DebugLevel >= level {
		db.options.DebugLogger.Printf(format, args...)
	}
}

func (db *DB) WaitForFlushes() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	for len(db.frozenMemtables) > 0 {
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
		if err := task.state.wal.Close(); activeErr == nil && err != nil {
			activeErr = err
		}
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
