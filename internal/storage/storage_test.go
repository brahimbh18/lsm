package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"lsm/internal/storage/sstable"
)

func TestStorageWritePersistsSSTableBytes(t *testing.T) {
	directory := t.TempDir()
	storage, err := NewStorage(directory)
	if err != nil {
		t.Fatalf("NewStorage() error = %v", err)
	}

	first := &sstable.SSTable{Data: []byte("first table")}
	second := &sstable.SSTable{Data: []byte("second table")}
	if err := storage.Write(first); err != nil {
		t.Fatalf("first Write() error = %v", err)
	}
	if err := storage.Write(second); err != nil {
		t.Fatalf("second Write() error = %v", err)
	}

	for _, test := range []struct {
		name string
		want []byte
	}{
		{name: "000001.sst", want: first.Data},
		{name: "000002.sst", want: second.Data},
	} {
		got, err := os.ReadFile(filepath.Join(directory, test.name))
		if err != nil {
			t.Fatalf("ReadFile(%q) error = %v", test.name, err)
		}
		if !bytes.Equal(got, test.want) {
			t.Fatalf("%s = %q, want %q", test.name, got, test.want)
		}
	}
}

func TestStorageContinuesSequenceAcrossOpen(t *testing.T) {
	directory := t.TempDir()
	firstStorage, err := NewStorage(directory)
	if err != nil {
		t.Fatalf("first NewStorage() error = %v", err)
	}
	if err := firstStorage.Write(&sstable.SSTable{Data: []byte("first")}); err != nil {
		t.Fatalf("first Write() error = %v", err)
	}

	secondStorage, err := NewStorage(directory)
	if err != nil {
		t.Fatalf("second NewStorage() error = %v", err)
	}
	if err := secondStorage.Write(&sstable.SSTable{Data: []byte("second")}); err != nil {
		t.Fatalf("second Write() error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(directory, "000002.sst")); err != nil {
		t.Fatalf("expected second SSTable file: %v", err)
	}
}
