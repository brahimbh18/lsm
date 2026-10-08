package engine

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func testOptions(directory string) Options {
	return Options{
		WALDir:          filepath.Join(directory, "wal"),
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
	oldMemtable := db.active.table

	if err := db.Put([]byte("key"), []byte("value!!!")); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if db.active.table == oldMemtable {
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

func TestOpenCreatesDataDirectory(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)

	db, err := Open(Options{})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}()

	dataPath := filepath.Join(dir, "data")
	info, err := os.Stat(dataPath)
	if err != nil {
		t.Fatalf("data directory was not created: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("expected %s to be a directory", dataPath)
	}

	walFile := filepath.Join(dataPath, "wal-000001.log")
	walInfo, err := os.Stat(walFile)
	if err != nil {
		t.Fatalf("WAL file was not created: %v", err)
	}
	if walInfo.IsDir() {
		t.Fatalf("expected %s to be a file, got directory", walFile)
	}
}

func TestOpenPreservesExistingData(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)

	db, err := Open(Options{})
	if err != nil {
		t.Fatalf("first Open() error = %v", err)
	}

	key := []byte("persistent-key")
	value := []byte("persistent-value")
	if err := db.Put(key, value); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	walFile := filepath.Join(dir, "data", "wal-000001.log")
	walBefore, err := os.Stat(walFile)
	if err != nil {
		t.Fatalf("wal-000001.log does not exist: %v", err)
	}
	if walBefore.Size() == 0 {
		t.Fatal("wal-000001.log is unexpectedly empty")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}

	// Reopen database from the same directory
	db2, err := Open(Options{})
	if err != nil {
		t.Fatalf("second Open() error = %v", err)
	}
	defer func() {
		if err := db2.Close(); err != nil {
			t.Fatalf("second Close() error = %v", err)
		}
	}()

	// Verify wal-000001.log was not destroyed or truncated
	walAfter, err := os.Stat(walFile)
	if err != nil {
		t.Fatalf("wal-000001.log was destroyed or removed: %v", err)
	}
	if walAfter.Size() < walBefore.Size() {
		t.Fatalf("wal-000001.log was truncated: before %d, after %d", walBefore.Size(), walAfter.Size())
	}

	// Verify existing key-value is recovered
	got, ok := db2.Get(key)
	if !ok {
		t.Fatalf("key %q was not recovered", key)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("got %q, want %q", got, value)
	}

	// Verify that the active WAL remains wal-000001.log and new writes work
	newKey := []byte("another-key")
	newValue := []byte("another-value")
	if err := db2.Put(newKey, newValue); err != nil {
		t.Fatalf("second Put() error = %v", err)
	}

	gotNew, ok := db2.Get(newKey)
	if !ok {
		t.Fatalf("new key %q was not recovered", newKey)
	}
	if !bytes.Equal(gotNew, newValue) {
		t.Fatalf("got %q, want %q", gotNew, newValue)
	}
}

