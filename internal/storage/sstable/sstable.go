package sstable

import "lsm/internal/memtable"

const MaxBlockSize = 4 * 1024

type Block struct {
	Records []memtable.Entry
}

type IndexEntry struct {
	Offset uint64
	Size   uint64
}

type Index struct {
	Entries []IndexEntry
}

type Footer struct {
	IndexOffset uint64
	IndexSize   uint64
}

type SSTable struct {
	Data []byte
}
