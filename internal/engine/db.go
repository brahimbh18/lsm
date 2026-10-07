package engine

import (
	"errors"
	"path/filepath"
	"sync"

	"lsm/internal/memtable"
	"lsm/internal/storage"
	"lsm/internal/wal"
)

var ErrClosed = errors.New("database is closed")

type DB struct {
	mu              sync.Mutex
	queueMu         sync.Mutex
	wal             *wal.WAL
	memtable        *memtable.MemTable
	frozenMemtables []*memtable.MemTable
	flushQueue      chan *memtable.MemTable
	flushWorker     sync.WaitGroup
	flushBuilder    *storage.Builder
	flushStorage    *storage.Storage
	flushDone       chan struct{}
	flushErr        error
	options         Options
	closed          bool
}

func Open(options Options) (*DB, error) {
	if options.WALPath == "" {
		return nil, errors.New("WAL path is required")
	}
	if options.DataDir == "" {
		options.DataDir = filepath.Dir(options.WALPath)
	}
	if options.MemTableMaxSize <= 0 {
		options.MemTableMaxSize = memtable.MaxSize
	}

	w, err := wal.Open(options.WALPath)
	if err != nil {
		return nil, err
	}

	flushStorage, err := storage.NewStorage(options.DataDir)
	if err != nil {
		_ = w.Close()
		return nil, err
	}

	db := &DB{
		wal:          w,
		memtable:     memtable.New(options.MemTableMaxSize),
		flushQueue:   make(chan *memtable.MemTable, 1),
		flushBuilder: storage.NewBuilder(),
		flushStorage: flushStorage,
		flushDone:    make(chan struct{}, 1),
		options:      options,
	}

	if err := w.Replay(func(key, value []byte) error {
		return db.memtable.Put(key, value)
	}); err != nil {
		_ = w.Close()
		return nil, err
	}

	db.startFlushWorker()
	return db, nil
}

func (db *DB) startFlushWorker() {
	db.flushWorker.Add(1)
	go func() {
		defer db.flushWorker.Done()
		for table := range db.flushQueue {
			err := db.flush(table)

			db.mu.Lock()
			db.removeFrozenMemtable(table)
			if err != nil && db.flushErr == nil {
				db.flushErr = err
			}
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
		db.mu.Unlock()
		return db.flushErr
	}

	if err := db.wal.Append(key, value); err != nil {
		db.mu.Unlock()
		return err
	}
	if db.options.SyncMode == SyncModeSync {
		if err := db.wal.Sync(); err != nil {
			db.mu.Unlock()
			return err
		}
	}
	if err := db.memtable.Put(key, value); err != nil {
		db.mu.Unlock()
		return err
	}

	var frozen *memtable.MemTable
	if db.memtable.IsFull() {
		frozen = db.memtable
		frozen.Freeze()
		db.frozenMemtables = append(db.frozenMemtables, frozen)
		db.memtable = memtable.New(db.options.MemTableMaxSize)
	}
	db.mu.Unlock()

	if frozen != nil {
		db.flushQueue <- frozen
	}
	return nil
}

func (db *DB) Get(key []byte) ([]byte, bool) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.memtable.Get(key)
}

func (db *DB) flush(table *memtable.MemTable) error {
	sstable, err := db.flushBuilder.Build(table)
	if err != nil {
		return err
	}
	return db.flushStorage.Write(sstable)
}

func (db *DB) removeFrozenMemtable(table *memtable.MemTable) {
	for index, frozen := range db.frozenMemtables {
		if frozen == table {
			db.frozenMemtables = append(db.frozenMemtables[:index], db.frozenMemtables[index+1:]...)
			return
		}
	}
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
	db.mu.Unlock()
	walErr := db.wal.Close()
	return errors.Join(flushErr, walErr)
}
