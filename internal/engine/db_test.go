package engine

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func testOptions(directory string) Options {
	return Options{
		WALPath:         filepath.Join(directory, "wal.log"),
		DataDir:         filepath.Join(directory, "tables"),
		MemTableMaxSize: 10,
	}
}

func TestPutFlushesFrozenMemTable(t *testing.T) {
	directory := t.TempDir()
	db, err := Open(testOptions(directory))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	oldMemtable := db.memtable

	if err := db.Put([]byte("key"), []byte("value!!!")); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if db.memtable == oldMemtable {
		t.Fatal("full MemTable was not replaced")
	}

	select {
	case <-db.flushDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for flush")
	}

	db.mu.Lock()
	frozenCount := len(db.frozenMemtables)
	db.mu.Unlock()
	if frozenCount != 0 {
		t.Fatalf("frozen MemTables remaining = %d, want 0", frozenCount)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	files, err := filepath.Glob(filepath.Join(directory, "tables", "*.sst"))
	if err != nil {
		t.Fatalf("Glob() error = %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("SSTable file count = %d, want 1", len(files))
	}
}

func TestCloseDrainsQueuedMemTable(t *testing.T) {
	directory := t.TempDir()
	db, err := Open(testOptions(directory))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := db.Put([]byte("key"), []byte("value!!!")); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}

	if _, err := os.Stat(filepath.Join(directory, "tables", "000001.sst")); err != nil {
		t.Fatalf("queued SSTable was not persisted: %v", err)
	}
	if err := db.Put([]byte("after-close"), []byte("value")); err != ErrClosed {
		t.Fatalf("Put() after Close() error = %v, want %v", err, ErrClosed)
	}
}
