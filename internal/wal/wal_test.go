package wal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"lsm/internal/memtable"
)

func TestAppendReplayPreservesSequence(t *testing.T) {
	w, err := Open(filepath.Join(t.TempDir(), "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if err := w.Append([]byte{}, []byte{}, 7); err != nil {
		t.Fatal(err)
	}
	if err := w.Append([]byte("key"), []byte("value"), 8); err != nil {
		t.Fatal(err)
	}

	var got []memtable.Entry
	if err := w.Replay(func(entry memtable.Entry) error {
		got = append(got, entry)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Seq != 7 || got[1].Seq != 8 || string(got[1].Value) != "value" {
		t.Fatalf("replayed entries = %+v", got)
	}
}

func TestReplayRejectsCorruptRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append([]byte("key"), []byte("value"), 1); err != nil {
		t.Fatal(err)
	}
	if err := w.file.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{0xff}, 0); err != nil {
		file.Close()
		t.Fatal(err)
	}
	file.Close()
	w, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	err = w.Replay(func(memtable.Entry) error { return nil })
	if !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("Replay() error = %v, want %v", err, ErrCorruptRecord)
	}
	w.Close()
}
