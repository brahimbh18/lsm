package compaction

import (
	"bytes"
	"errors"
	"sort"

	"lsm/internal/memtable"
	"lsm/internal/storage/sstable"
)

var ErrNilSSTable = errors.New("nil SSTable")

// SSTableMeta contains the information needed by a level picker. It is
// intentionally independent of filenames so it can later be persisted in a
// manifest.
type SSTableMeta struct {
	ID       uint64
	Level    int
	Size     uint64
	Smallest []byte
	Largest  []byte
}

func (m SSTableMeta) Overlaps(smallest, largest []byte) bool {
	return bytes.Compare(m.Largest, smallest) >= 0 && bytes.Compare(m.Smallest, largest) <= 0
}

// SelectCompaction returns the input files for a level compaction. L0 selects
// all files because L0 ranges may overlap; higher levels select the first file
// and every overlapping file in the next level.
func SelectCompaction(level int, source, next []SSTableMeta) (selected, overlapping []SSTableMeta) {
	if len(source) == 0 {
		return nil, nil
	}
	if level == 0 {
		selected = append(selected, source...)
	} else {
		selected = append(selected, source[0])
	}
	smallest, largest := selected[0].Smallest, selected[0].Largest
	for _, table := range selected[1:] {
		if bytes.Compare(table.Smallest, smallest) < 0 {
			smallest = table.Smallest
		}
		if bytes.Compare(table.Largest, largest) > 0 {
			largest = table.Largest
		}
	}
	for _, table := range next {
		if table.Overlaps(smallest, largest) {
			overlapping = append(overlapping, table)
		}
	}
	return selected, overlapping
}

// MergeSSTables merges sorted table records and keeps only the newest version
// of each key. Tombstones are retained; dropping them is only safe when the
// destination is known to be the bottommost level.
func MergeSSTables(tables ...[]memtable.Entry) []memtable.Entry {
	byKey := make(map[string]memtable.Entry)
	for _, entries := range tables {
		for _, entry := range entries {
			key := string(entry.Key)
			current, ok := byKey[key]
			if !ok || entry.Seq > current.Seq {
				byKey[key] = entry
			}
		}
	}
	result := make([]memtable.Entry, 0, len(byKey))
	for _, entry := range byKey {
		result = append(result, entry)
	}
	sort.Slice(result, func(i, j int) bool {
		return bytes.Compare(result[i].Key, result[j].Key) < 0
	})
	return result
}

// CompactSSTables decodes, merges, and rebuilds the supplied SSTables.
func CompactSSTables(tables ...*sstable.SSTable) (*sstable.SSTable, error) {
	decoded := make([][]memtable.Entry, 0, len(tables))
	for _, table := range tables {
		if table == nil {
			return nil, ErrNilSSTable
		}
		entries, err := sstable.DecodeSSTable(table.Data)
		if err != nil {
			return nil, err
		}
		decoded = append(decoded, entries)
	}
	merged := MergeSSTables(decoded...)
	table := memtable.New(0)
	for _, entry := range merged {
		if err := table.PutEntry(entry); err != nil {
			return nil, err
		}
	}
	return sstable.NewBuilder().Build(table)
}
