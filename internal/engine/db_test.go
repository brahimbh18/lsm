package engine

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"lsm/internal/memtable"
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
	if frozenCount != 1 {
		t.Fatalf("immutable MemTables remaining = %d, want 1", frozenCount)
	}
	if got, ok := db.Get([]byte("key")); !ok || !bytes.Equal(got, []byte("value!!!")) {
		t.Fatalf("flushed key = %q, found=%v", got, ok)
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

func TestConcurrentPutsHaveUniqueSequences(t *testing.T) {
	db, err := Open(testOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const writes = 100
	var wait sync.WaitGroup
	for i := 0; i < writes; i++ {
		wait.Add(1)
		go func(i int) {
			defer wait.Done()
			if err := db.Put([]byte{byte(i)}, []byte("value")); err != nil {
				t.Errorf("Put() error = %v", err)
			}
		}(i)
	}
	wait.Wait()
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.lastSequence != writes {
		t.Fatalf("last sequence = %d, want %d", db.lastSequence, writes)
	}
	seen := make(map[uint64]bool, writes)
	for _, entry := range db.active.table.Entries() {
		if seen[entry.Seq] {
			t.Fatalf("duplicate sequence %d", entry.Seq)
		}
		seen[entry.Seq] = true
	}
}

func TestRecoveryContinuesSequenceAfterFlush(t *testing.T) {
	dir := t.TempDir()
	options := testOptions(dir)
	options.MemTableMaxSize = 1
	db, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("first"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := db.WaitForFlushes(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	options.MemTableMaxSize = 100
	db, err = Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Put([]byte("second"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.lastSequence != 2 {
		t.Fatalf("last sequence after recovery = %d, want 2", db.lastSequence)
	}
	if entries := db.active.table.Entries(); len(entries) != 1 || entries[0].Seq != 2 {
		t.Fatalf("recovered active entries = %+v", entries)
	}
}

func TestSequenceOverflow(t *testing.T) {
	db, err := Open(testOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.mu.Lock()
	db.lastSequence = ^uint64(0)
	db.mu.Unlock()
	if err := db.Put([]byte("key"), []byte("value")); err != ErrSequenceOverflow {
		t.Fatalf("Put() error = %v, want %v", err, ErrSequenceOverflow)
	}
}

func TestGetPrefersNewestVisibleMemTable(t *testing.T) {
	db, err := Open(testOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.mu.Lock()
	oldest := db.active
	oldest.table.PutEntry(memtable.Entry{Key: []byte("key"), Value: []byte("oldest"), Seq: 1})
	oldest.table.PutEntry(memtable.Entry{Key: []byte("older-only"), Value: []byte("older"), Seq: 2})
	oldest.table.PutEntry(memtable.Entry{Key: []byte("versioned"), Value: []byte("old-version"), Seq: 2})
	oldest.table.Freeze()
	oldest.walClosed = true
	db.frozenMemtables = append(db.frozenMemtables, &flushTask{state: oldest})
	newest := &memTableState{table: memtable.New(1024), walClosed: true}
	newest.table.PutEntry(memtable.Entry{Key: []byte("key"), Value: []byte("newest"), Seq: 2})
	newest.table.PutEntry(memtable.Entry{Key: []byte("newer-only"), Value: []byte("newer"), Seq: 3})
	newest.table.PutEntry(memtable.Entry{Key: []byte("versioned"), Value: []byte("new-version"), Seq: 3})
	newest.table.Freeze()
	db.frozenMemtables = append(db.frozenMemtables, &flushTask{state: newest})
	db.active = &memTableState{table: memtable.New(1024)}
	db.active.table.PutEntry(memtable.Entry{Key: []byte("key"), Value: []byte("active"), Seq: 4})
	db.mu.Unlock()

	got, ok := db.Get([]byte("key"))
	if !ok || !bytes.Equal(got, []byte("active")) {
		t.Fatalf("active Get() = %q, found=%v, want active value", got, ok)
	}
	if got, ok := db.Get([]byte("older-only")); !ok || !bytes.Equal(got, []byte("older")) {
		t.Fatalf("older immutable Get() = %q, found=%v", got, ok)
	}
	if got, ok := db.Get([]byte("newer-only")); !ok || !bytes.Equal(got, []byte("newer")) {
		t.Fatalf("newer immutable Get() = %q, found=%v", got, ok)
	}
	if got, ok := db.Get([]byte("versioned")); !ok || !bytes.Equal(got, []byte("new-version")) {
		t.Fatalf("newest immutable Get() = %q, found=%v", got, ok)
	}
	if got, ok := db.Get([]byte("missing")); ok || got != nil {
		t.Fatalf("missing Get() = %q, found=%v", got, ok)
	}
}

func TestGetConcurrentWithPutsAndFreezes(t *testing.T) {
	options := testOptions(t.TempDir())
	options.MemTableMaxSize = 8
	db, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var wait sync.WaitGroup
	wait.Add(2)
	go func() {
		defer wait.Done()
		for i := 0; i < 100; i++ {
			if err := db.Put([]byte("key"), []byte("value")); err != nil {
				t.Errorf("Put() error = %v", err)
				return
			}
		}
	}()
	go func() {
		defer wait.Done()
		for i := 0; i < 500; i++ {
			if value, ok := db.Get([]byte("key")); ok && !bytes.Equal(value, []byte("value")) {
				t.Errorf("Get() = %q, want value", value)
				return
			}
		}
	}()
	wait.Wait()
	if err := db.WaitForFlushes(); err != nil {
		t.Fatal(err)
	}
	if value, ok := db.Get([]byte("key")); !ok || !bytes.Equal(value, []byte("value")) {
		t.Fatalf("final Get() = %q, found=%v", value, ok)
	}
}
