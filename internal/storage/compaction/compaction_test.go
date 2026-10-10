package compaction

import (
	"bytes"
	"testing"

	"lsm/internal/memtable"
	"lsm/internal/storage/sstable"
)

func TestDefaultCompactionPolicyTargets(t *testing.T) {
	policy := DefaultCompactionPolicy()
	tests := []struct {
		level int
		want  uint64
	}{
		{0, 64 * 1024 * 1024},
		{1, 640 * 1024 * 1024},
		{2, 6400 * 1024 * 1024},
	}
	for _, test := range tests {
		got, err := policy.TargetBytes(test.level)
		if err != nil {
			t.Fatalf("TargetBytes(%d) error = %v", test.level, err)
		}
		if got != test.want {
			t.Errorf("TargetBytes(%d) = %d, want %d", test.level, got, test.want)
		}
	}
}

func TestCompactionPolicyL0UsesBytesOrFiles(t *testing.T) {
	policy := DefaultCompactionPolicy()
	target := policy.L0TargetBytes
	tests := []struct {
		name         string
		bytes, files uint64
		want         bool
	}{
		{"below both", target - 1, 3, false},
		{"file trigger", target - 1, 4, true},
		{"byte trigger", target, 0, true},
	}
	for _, test := range tests {
		got, err := policy.ShouldCompact(0, test.bytes, int(test.files))
		if err != nil {
			t.Fatalf("%s: ShouldCompact() error = %v", test.name, err)
		}
		if got != test.want {
			t.Errorf("%s: ShouldCompact() = %v, want %v", test.name, got, test.want)
		}
	}
}

func TestCompactionPolicyHigherLevelsUseBytesOnly(t *testing.T) {
	policy := DefaultCompactionPolicy()
	target, err := policy.TargetBytes(1)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := policy.ShouldCompact(1, target-1, 1000); err != nil || got {
		t.Fatalf("many small L1 files should not trigger below byte target: got=%v err=%v", got, err)
	}
	if got, err := policy.ShouldCompact(1, target, 0); err != nil || !got {
		t.Fatalf("L1 byte target should trigger: got=%v err=%v", got, err)
	}
}

func TestCompactionPolicyRejectsInvalidLevelsAndOverflow(t *testing.T) {
	policy := DefaultCompactionPolicy()
	if _, err := policy.TargetBytes(-1); err != ErrInvalidCompactionPolicy {
		t.Fatalf("negative level error = %v, want %v", err, ErrInvalidCompactionPolicy)
	}
	overflow := CompactionPolicy{L0TargetBytes: ^uint64(0), L0FileTrigger: 1, LevelMultiplier: 2}
	if _, err := overflow.TargetBytes(1); err != ErrLevelOverflow {
		t.Fatalf("overflow error = %v, want %v", err, ErrLevelOverflow)
	}
}

func TestDecodeAndCompactSSTables(t *testing.T) {
	first := buildTestTable(t, []memtable.Entry{
		{Key: []byte("a"), Value: []byte("old"), Seq: 1},
		{Key: []byte("b"), Value: []byte("value"), Seq: 2},
	})
	second := buildTestTable(t, []memtable.Entry{
		{Key: []byte("a"), Value: []byte("new"), Seq: 3},
		{Key: []byte("c"), Seq: 4, Tombstone: true},
	})

	merged, err := CompactSSTables(first, second)
	if err != nil {
		t.Fatalf("CompactSSTables() error = %v", err)
	}
	entries, err := sstable.DecodeSSTable(merged.Data)
	if err != nil {
		t.Fatalf("DecodeSSTable() error = %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("merged entries = %d, want 3", len(entries))
	}
	if !bytes.Equal(entries[0].Value, []byte("new")) || entries[0].Seq != 3 {
		t.Fatalf("newest version was not retained: %+v", entries[0])
	}
	if !entries[2].Tombstone || entries[2].Seq != 4 {
		t.Fatalf("tombstone was not retained: %+v", entries[2])
	}
	if _, err := CompactSSTables(nil); err != ErrNilSSTable {
		t.Fatalf("nil table error = %v, want %v", err, ErrNilSSTable)
	}
}

func TestSelectCompaction(t *testing.T) {
	source := []SSTableMeta{
		{ID: 1, Level: 1, Smallest: []byte("a"), Largest: []byte("b")},
		{ID: 2, Level: 1, Smallest: []byte("c"), Largest: []byte("d")},
	}
	next := []SSTableMeta{
		{ID: 3, Level: 2, Smallest: []byte("b"), Largest: []byte("c")},
		{ID: 4, Level: 2, Smallest: []byte("x"), Largest: []byte("z")},
	}
	selected, overlapping := SelectCompaction(1, source, next)
	if len(selected) != 1 || selected[0].ID != 1 {
		t.Fatalf("selected = %+v, want table 1", selected)
	}
	if len(overlapping) != 1 || overlapping[0].ID != 3 {
		t.Fatalf("overlapping = %+v, want table 3", overlapping)
	}
	selected, _ = SelectCompaction(0, source, next)
	if len(selected) != 2 {
		t.Fatalf("L0 selected = %d, want 2", len(selected))
	}
}

func buildTestTable(t *testing.T, entries []memtable.Entry) *sstable.SSTable {
	t.Helper()
	table := memtable.New(1024 * 1024)
	for _, entry := range entries {
		if err := table.PutEntry(entry); err != nil {
			t.Fatal(err)
		}
	}
	result, err := sstable.NewBuilder().Build(table)
	if err != nil {
		t.Fatal(err)
	}
	return result
}
