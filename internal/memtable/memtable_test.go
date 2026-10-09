package memtable

import (
	"testing"
)

func TestPutEntryPreservesSequence(t *testing.T) {
	table := New(1024)
	if err := table.PutEntry(Entry{Key: []byte("key"), Value: []byte("value"), Seq: 11}); err != nil {
		t.Fatal(err)
	}
	entries := table.Entries()
	if len(entries) != 1 || entries[0].Seq != 11 {
		t.Fatalf("entries = %+v", entries)
	}
}
