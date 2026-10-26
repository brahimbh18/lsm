package engine

import (
	"bytes"
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
	"lsm/internal/storage/compaction"
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

type compactableStorage interface {
	sstableStorage
	Read(uint64) (*sstable.SSTable, error)
	Delete(uint64) error
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
	FlushDuration      time.Duration
	Compactions        int
	CompactionDuration time.Duration
	QueueWaits         int
	QueueWaitDuration  time.Duration
}

type DB struct {
	mu               sync.RWMutex
	manifestMu       sync.Mutex
	queueMu          sync.Mutex
	rotationMu       sync.Mutex
	active           *memTableState
	frozenMemtables  []*flushTask
	flushQueue       chan *flushTask
	flushWorker      sync.WaitGroup
	compactionQueue  chan struct{}
	compactionWorker sync.WaitGroup
	flushBuilder     *sstable.Builder
	flushStorage     sstableStorage
	flushDone        chan struct{}
	flushErr         error
	writeErr         error
	flushCond        *sync.Cond
	rotationCond     *sync.Cond
	nextMemtableID   uint64
	manifest         storage.Manifest
	tablesDir        string
	compaction       compaction.CompactionPolicy
	stats            FlushStats
	options          Options
	walSyncer        walSyncer
	closed           bool
	lastSequence     uint64
	flushPending     int
	rotating         bool
	newStateHook     func(uint64, *wal.WAL) (*memTableState, error)
	appendEntryHook  func(*wal.WAL, memtable.Entry) error
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
	if options.SyncPolicy < SyncPolicyAlways || options.SyncPolicy > SyncPolicyNever {
		return nil, fmt.Errorf("unknown WAL sync policy %d", options.SyncPolicy)
	}
	if options.SyncPolicy == SyncPolicyEverysec && options.SyncInterval <= 0 {
		if options.SyncInterval == 0 {
			options.SyncInterval = time.Second
		} else {
			return nil, fmt.Errorf("WAL sync interval must be positive for everysec")
		}
	}
	if options.SyncPolicy == SyncPolicyAlways && options.SyncMode == SyncModeAsync {
		options.SyncPolicy = SyncPolicyNever
	}
	if options.CompactionPolicy == (compaction.CompactionPolicy{}) {
		options.CompactionPolicy = compaction.DefaultCompactionPolicy()
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
		flushQueue:      make(chan *flushTask, 1),
		compactionQueue: make(chan struct{}, 1),
		flushBuilder:    sstable.NewBuilder(),
		flushDone:       make(chan struct{}, 1),
		flushStorage:    nil,
		nextMemtableID:  nextID,
		options:         options,
		tablesDir:       tablesDir,
		compaction:      options.CompactionPolicy,
	}
	db.flushStorage, err = storage.NewStorage(tablesDir)
	if err != nil {
		return nil, err
	}
	db.manifest, err = storage.ReadManifest(tablesDir)
	if err != nil {
		return nil, err
	}
	if len(db.manifest.Tables) == 0 && len(sstIDs) > 0 {
		for id := range sstIDs {
			table, readErr := db.flushStorage.(*storage.Storage).Read(id)
			if readErr != nil {
				return nil, readErr
			}
			meta, metaErr := tableMeta(id, 0, table)
			if metaErr != nil {
				return nil, metaErr
			}
			db.manifest.Tables = append(db.manifest.Tables, meta)
		}
		if err := storage.WriteManifest(tablesDir, db.manifest); err != nil {
			return nil, err
		}
	}
	db.flushCond = sync.NewCond(&db.mu)
	db.rotationCond = sync.NewCond(&db.mu)

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
	db.log(1, "[DB] CONFIG memtable_threshold=%d flush_queue_capacity=%d compaction_l0_bytes=%d compaction_l0_files=%d compaction_level_multiplier=%d",
		options.MemTableMaxSize, cap(db.flushQueue), options.CompactionPolicy.L0TargetBytes,
		options.CompactionPolicy.L0FileTrigger, options.CompactionPolicy.LevelMultiplier)
	db.log(1, "[WAL] CREATE path=%s", db.active.wal.Path())
	db.log(1, "[MEMTABLE] CREATE id=%d wal=%s", db.active.id, db.active.wal.Path())
	db.walSyncer = newWalSyncer(options.SyncPolicy, options.SyncInterval, db.active.wal, db.log)
	db.startFlushWorker()
	db.startCompactionWorker()
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

func (db *DB) createState(id uint64, existing *wal.WAL) (*memTableState, error) {
	if db.newStateHook != nil {
		return db.newStateHook(id, existing)
	}
	return db.newState(id, existing)
}

func (db *DB) setWriteError(err error) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.setWriteErrorLocked(err)
}

func (db *DB) setWriteErrorLocked(err error) error {
	if db.writeErr == nil {
		db.writeErr = err
	}
	return db.writeErr
}

func (db *DB) checkWritableLocked() error {
	if db.closed {
		return ErrClosed
	}
	if db.writeErr != nil {
		return db.writeErr
	}
	if db.flushErr != nil {
		return db.flushErr
	}
	if err := db.walSyncer.Err(); err != nil {
		return db.setWriteErrorLocked(err)
	}
	if db.lastSequence == ^uint64(0) {
		return ErrSequenceOverflow
	}
	return nil
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
			if err == nil {
				db.requestCompactionLocked()
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

func (db *DB) startCompactionWorker() {
	db.compactionWorker.Add(1)
	go func() {
		defer db.compactionWorker.Done()
		for range db.compactionQueue {
			for {
				compacted, err := db.compactOneTriggeredLevel()
				if err != nil {
					db.mu.Lock()
					if db.flushErr == nil {
						db.flushErr = err
					}
					db.mu.Unlock()
					break
				}
				if !compacted {
					break
				}
			}
			db.mu.Lock()
			db.flushPending--
			db.flushCond.Broadcast()
			db.mu.Unlock()
		}
	}()
}

func (db *DB) requestCompaction() {
	db.mu.Lock()
	db.requestCompactionLocked()
	db.mu.Unlock()
}

func (db *DB) requestCompactionLocked() {
	if db.closed {
		return
	}
	select {
	case db.compactionQueue <- struct{}{}:
		db.flushPending++
	default:
	}
}

func (db *DB) Put(key, value []byte) error {
	return db.write(key, value, false)
}

func (db *DB) Delete(key []byte) error {
	return db.write(key, nil, true)
}

func (db *DB) write(key, value []byte, tombstone bool) error {
	for {
		db.queueMu.Lock()
		db.mu.Lock()
		if !db.rotating {
			break
		}
		db.mu.Unlock()
		db.queueMu.Unlock()
		db.mu.Lock()
		for db.rotating {
			db.rotationCond.Wait()
		}
		db.mu.Unlock()
	}

	if err := db.checkWritableLocked(); err != nil {
		db.mu.Unlock()
		db.queueMu.Unlock()
		return err
	}
	db.lastSequence++
	sequence := db.lastSequence
	active := db.active
	db.mu.Unlock()

	entry := memtable.Entry{Key: key, Value: value, Seq: sequence, Tombstone: tombstone}
	appendEntry := active.wal.AppendEntry
	if db.appendEntryHook != nil {
		appendEntry = func(entry memtable.Entry) error {
			return db.appendEntryHook(active.wal, entry)
		}
	}
	if err := appendEntry(entry); err != nil {
		db.queueMu.Unlock()
		return db.setWriteError(err)
	}
	if err := db.walSyncer.AfterAppend(active.wal); err != nil {
		db.queueMu.Unlock()
		return db.setWriteError(err)
	}
	if err := active.table.PutEntry(entry); err != nil {
		db.queueMu.Unlock()
		return db.setWriteError(err)
	}
	db.log(2, "[WRITE] key=%s wal=%s", key, active.wal.Path())
	db.mu.Lock()
	db.stats.RecordsWritten++
	db.stats.BytesWritten += int64(len(key) + len(value))
	full := active.table.IsFull()
	nextID := db.nextMemtableID
	if full {
		db.rotationMu.Lock()
		db.rotating = true
	}
	db.mu.Unlock()
	db.queueMu.Unlock()
	if !full {
		return nil
	}

	newActive, err := db.createState(nextID, nil)
	if err == nil {
		err = db.walSyncer.OnRotate(active.wal, newActive.wal)
	}
	if err != nil {
		if newActive != nil && newActive.wal != nil {
			_ = newActive.wal.Close()
		}
		db.mu.Lock()
		db.rotating = false
		db.rotationCond.Broadcast()
		db.mu.Unlock()
		db.rotationMu.Unlock()
		return db.setWriteError(err)
	}

	db.queueMu.Lock()
	db.mu.Lock()
	active.table.Freeze()
	frozen := &flushTask{state: active}
	db.frozenMemtables = append(db.frozenMemtables, frozen)
	db.flushPending++
	db.active = newActive
	db.nextMemtableID++
	db.stats.MemTablesFrozen++
	db.stats.MemTablesCreated++
	db.mu.Unlock()

	db.log(1, "[MEMTABLE] FULL id=%d size=%d threshold=%d", active.id, active.table.Size(), db.options.MemTableMaxSize)
	db.log(1, "[MEMTABLE] FREEZE id=%d wal=%s", active.id, active.wal.Path())
	db.log(1, "[MEMTABLE] CREATE id=%d wal=%s", newActive.id, newActive.wal.Path())
	db.log(1, "[FLUSH QUEUE] ENQUEUE memtable=%d queue_length=%d", frozen.state.id, len(db.flushQueue)+1)
	enqueueStart := time.Now()
	db.flushQueue <- frozen
	enqueueDuration := time.Since(enqueueStart)
	if enqueueDuration > 10*time.Millisecond {
		db.mu.Lock()
		db.stats.QueueWaits++
		db.stats.QueueWaitDuration += enqueueDuration
		db.mu.Unlock()
		db.log(1, "[WRITE] flush queue wait=%s memtable=%d", enqueueDuration, frozen.state.id)
	}
	db.queueMu.Unlock()

	db.mu.Lock()
	db.rotating = false
	db.rotationCond.Broadcast()
	db.mu.Unlock()
	db.rotationMu.Unlock()
	return nil
}

func (db *DB) Get(key []byte) ([]byte, bool) {
	db.manifestMu.Lock()
	defer db.manifestMu.Unlock()
	db.mu.RLock()
	if entry, ok := db.active.table.Lookup(key); ok {
		db.mu.RUnlock()
		if entry.Tombstone {
			return nil, false
		}
		return entry.Value, true
	}
	for index := len(db.frozenMemtables) - 1; index >= 0; index-- {
		if entry, ok := db.frozenMemtables[index].state.table.Lookup(key); ok {
			db.mu.RUnlock()
			if entry.Tombstone {
				return nil, false
			}
			return entry.Value, true
		}
	}
	tables := append([]storage.TableMeta(nil), db.manifest.Tables...)
	db.mu.RUnlock()

	var newest memtable.Entry
	found := false
	tableStore, ok := db.flushStorage.(compactableStorage)
	if !ok {
		return nil, false
	}
	for _, meta := range tables {
		if len(meta.Smallest) > 0 && (bytes.Compare(key, meta.Smallest) < 0 || bytes.Compare(key, meta.Largest) > 0) {
			continue
		}
		table, err := tableStore.Read(meta.ID)
		if err != nil {
			db.log(1, "[GET] READ table=%06d error=%v", meta.ID, err)
			continue
		}
		entries, err := sstable.DecodeSSTable(table.Data)
		if err != nil {
			db.log(1, "[GET] DECODE table=%06d error=%v", meta.ID, err)
			continue
		}
		for _, entry := range entries {
			if bytes.Equal(entry.Key, key) && (!found || entry.Seq > newest.Seq) {
				newest = entry
				found = true
			}
		}
	}
	if found && !newest.Tombstone {
		return newest.Value, true
	}
	return nil, false
}

func (db *DB) flush(task *flushTask) error {
	start := time.Now()
	db.log(1, "[FLUSH] START memtable=%d wal=%s", task.state.id, task.state.wal.Path())
	blocks := sstable.SplitBlocks(task.state.table.Entries())
	builderStart := time.Now()
	sstable, err := db.flushBuilder.Build(task.state.table)
	if err != nil {
		return err
	}
	builderDuration := time.Since(builderStart)
	db.log(1, "[SSTABLE BUILDER] DONE level=L0 blocks=%d size=%d", len(blocks), len(sstable.Data))
	writeStart := time.Now()
	result, err := db.flushStorage.WriteWithInfo(sstable)
	if err != nil {
		return err
	}
	writeDuration := time.Since(writeStart)
	meta, err := tableMeta(result.ID, 0, sstable)
	if err != nil {
		return err
	}
	db.manifestMu.Lock()
	defer db.manifestMu.Unlock()
	db.mu.Lock()
	db.manifest.Tables = append(db.manifest.Tables, meta)
	manifest := db.manifest
	db.mu.Unlock()
	manifestStart := time.Now()
	if err := storage.WriteManifest(db.tablesDir, manifest); err != nil {
		return err
	}
	manifestDuration := time.Since(manifestStart)
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
	db.stats.FlushDuration += time.Since(start)
	db.mu.Unlock()
	db.log(1, "[FLUSH] TIMING memtable=%d builder=%s storage=%s write=%s fsync=%s manifest=%s total=%s",
		task.state.id, builderDuration, writeDuration, result.WriteTime, result.SyncTime,
		manifestDuration, time.Since(start))
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

func (db *DB) compactOneTriggeredLevel() (bool, error) {
	start := time.Now()
	tableStore, ok := db.flushStorage.(compactableStorage)
	if !ok {
		return false, nil
	}

	db.manifestMu.Lock()
	defer db.manifestMu.Unlock()
	db.mu.RLock()
	tables := append([]storage.TableMeta(nil), db.manifest.Tables...)
	db.mu.RUnlock()
	level := -1
	maxLevel := 0
	for _, table := range tables {
		if table.Level > maxLevel {
			maxLevel = table.Level
		}
	}
	for candidate := 0; candidate <= maxLevel; candidate++ {
		var bytes uint64
		var count int
		for _, table := range tables {
			if table.Level == candidate {
				bytes += table.Size
				count++
			}
		}
		triggered, err := db.compaction.ShouldCompact(candidate, bytes, count)
		if err != nil {
			return false, err
		}
		if triggered {
			level = candidate
			break
		}
	}
	if level < 0 {
		return false, nil
	}

	source := make([]compaction.SSTableMeta, 0)
	next := make([]compaction.SSTableMeta, 0)
	for _, table := range tables {
		meta := compaction.SSTableMeta{
			ID: table.ID, Level: table.Level, Size: table.Size,
			Smallest: table.Smallest, Largest: table.Largest,
		}
		if table.Level == level {
			source = append(source, meta)
		} else if table.Level == level+1 {
			next = append(next, meta)
		}
	}
	selected, overlapping := compaction.SelectCompaction(level, source, next)
	inputs := append(append([]compaction.SSTableMeta(nil), selected...), overlapping...)
	if len(inputs) == 0 {
		return false, nil
	}
	tablesToMerge := make([]*sstable.SSTable, 0, len(inputs))
	for _, input := range inputs {
		table, err := tableStore.Read(input.ID)
		if err != nil {
			return false, err
		}
		tablesToMerge = append(tablesToMerge, table)
	}
	merged, err := compaction.CompactSSTables(tablesToMerge...)
	if err != nil {
		return false, err
	}
	result, err := tableStore.WriteWithInfo(merged)
	if err != nil {
		return false, err
	}
	mergedMeta, err := tableMeta(result.ID, level+1, merged)
	if err != nil {
		return false, err
	}
	updated := storage.Manifest{Tables: make([]storage.TableMeta, 0, len(tables)-len(inputs)+1)}
	inputIDs := make(map[uint64]struct{}, len(inputs))
	for _, input := range inputs {
		inputIDs[input.ID] = struct{}{}
	}
	for _, table := range tables {
		if _, remove := inputIDs[table.ID]; !remove {
			updated.Tables = append(updated.Tables, table)
		}
	}
	updated.Tables = append(updated.Tables, mergedMeta)
	if err := storage.WriteManifest(db.tablesDir, updated); err != nil {
		return false, err
	}
	db.mu.Lock()
	db.manifest = updated
	db.mu.Unlock()
	for _, input := range inputs {
		if err := tableStore.Delete(input.ID); err != nil {
			return false, err
		}
	}
	var inputBytes uint64
	for _, input := range inputs {
		inputBytes += input.Size
	}
	db.mu.Lock()
	db.stats.Compactions++
	db.stats.CompactionDuration += time.Since(start)
	db.mu.Unlock()
	db.log(1, "[COMPACTION] level=L%d inputs=%d input_bytes=%d output_bytes=%d output=%06d.sst duration=%s",
		level, len(inputs), inputBytes, result.Size, result.ID, time.Since(start))
	return true, nil
}

func tableMeta(id uint64, level int, table *sstable.SSTable) (storage.TableMeta, error) {
	entries, err := sstable.DecodeSSTable(table.Data)
	if err != nil {
		return storage.TableMeta{}, err
	}
	meta := storage.TableMeta{ID: id, Level: level, Size: uint64(len(table.Data))}
	if len(entries) > 0 {
		meta.Smallest = append([]byte(nil), entries[0].Key...)
		meta.Largest = append([]byte(nil), entries[len(entries)-1].Key...)
	}
	return meta, nil
}

func (db *DB) Close() error {
	db.queueMu.Lock()
	defer db.queueMu.Unlock()
	db.rotationMu.Lock()
	defer db.rotationMu.Unlock()
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil
	}
	db.closed = true
	close(db.flushQueue)
	db.mu.Unlock()
	db.flushWorker.Wait()
	close(db.compactionQueue)
	db.compactionWorker.Wait()
	syncErr := db.walSyncer.Close()
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
	return errors.Join(flushErr, syncErr, activeErr)
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
