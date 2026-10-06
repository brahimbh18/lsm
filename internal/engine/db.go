package engine

import (
	"errors"

	"lsm/internal/memtable"
	"lsm/internal/wal"
)

type DB struct {
	wal               *wal.WAL
	memtable          *memtable.MemTable
	frozenMemtables []*memtable.MemTable
	options           Options
}

func Open(options Options) (*DB, error) {
	if options.WALPath == "" {
		return nil, errors.New("WAL path is required")
	}

	w, err := wal.Open(options.WALPath)
	if err != nil {
		return nil, err
	}

	db := &DB{
		wal:      w,
		memtable: memtable.New(),
		options:  options,
	}

	// Recover the MemTable from the WAL.
	err = w.Replay(func(key, value []byte) error {
		return db.memtable.Put(key, value)
	})

	if err != nil {
		w.Close()
		return nil, err
	}

	return db, nil
}

func (db *DB) Put(key, value []byte) error {
	// 1. Append to WAL.
	if err := db.wal.Append(key, value); err != nil {
		return err
	}

	// 2. Apply durability policy.
	if db.options.SyncMode == SyncModeSync {
		if err := db.wal.Sync(); err != nil {
			return err
		}
	}

	// 3. Update MemTable.
	if err := db.memtable.Put(key, value); err != nil {
		return err
	}

	// Check if the current MemTable has reached its maximum size.
	if db.memtable.IsFull() {
		// Freeze the current MemTable and create a new one.
		db.frozenMemtable = append(db.frozenMemtable, db.memtable)
		db.memtable = memtable.New()
	}
	// 4. Acknowledge.
	return nil
}

func (db *DB) Get(key []byte) ([]byte, bool) {
	return db.memtable.Get(key)
}

func (db *DB) Close() error {
	return db.wal.Close()
}
