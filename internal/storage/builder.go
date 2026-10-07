package storage

import (
	"encoding/binary"
	"errors"

	"lsm/internal/memtable"
)

var ErrRecordTooLarge = errors.New("record field exceeds uint32 length")

type Builder struct {
}

func NewBuilder() *Builder {
	return &Builder{}
}

func (b *Builder) Build(table *memtable.MemTable) (*SSTable, error) {
	entries := table.Entries()
	for _, entry := range entries {
		if uint64(len(entry.Key)) > uint64(^uint32(0)) || uint64(len(entry.Value)) > uint64(^uint32(0)) {
			return nil, ErrRecordTooLarge
		}
	}

	blocks := SplitBlocks(entries)
	data := make([]byte, 0)
	index := Index{Entries: make([]IndexEntry, 0, len(blocks))}

	for _, block := range blocks {
		blockData := EncodeBlock(block)
		index.Entries = append(index.Entries, IndexEntry{
			Offset: uint64(len(data)),
			Size:   uint64(len(blockData)),
		})
		data = append(data, blockData...)
	}

	indexOffset := uint64(len(data))
	encodedIndex := EncodeIndex(index)
	data = append(data, encodedIndex...)
	data = append(data, EncodeFooter(Footer{
		IndexOffset: indexOffset,
		IndexSize:   uint64(len(encodedIndex)),
	})...)

	return &SSTable{Data: data}, nil
}

func SplitBlocks(entries []memtable.Entry) []Block {
	blocks := make([]Block, 0)
	current := Block{}
	currentSize := 0

	for _, entry := range entries {
		recordSize := len(EncodeRecord(entry))
		if len(current.Records) > 0 && currentSize+recordSize > MaxBlockSize {
			blocks = append(blocks, current)
			current = Block{}
			currentSize = 0
		}
		current.Records = append(current.Records, entry)
		currentSize += recordSize
	}

	if len(current.Records) > 0 {
		blocks = append(blocks, current)
	}
	return blocks
}

func EncodeRecord(entry memtable.Entry) []byte {
	data := make([]byte, 8+len(entry.Key)+len(entry.Value))
	binary.BigEndian.PutUint32(data[0:4], uint32(len(entry.Key)))
	binary.BigEndian.PutUint32(data[4:8], uint32(len(entry.Value)))
	copy(data[8:], entry.Key)
	copy(data[8+len(entry.Key):], entry.Value)
	return data
}

func EncodeBlock(block Block) []byte {
	var data []byte
	for _, entry := range block.Records {
		data = append(data, EncodeRecord(entry)...)
	}
	return data
}

func EncodeIndex(index Index) []byte {
	data := make([]byte, len(index.Entries)*16)
	for entryIndex, entry := range index.Entries {
		offset := entryIndex * 16
		binary.BigEndian.PutUint64(data[offset:offset+8], entry.Offset)
		binary.BigEndian.PutUint64(data[offset+8:offset+16], entry.Size)
	}
	return data
}

func EncodeFooter(footer Footer) []byte {
	data := make([]byte, 16)
	binary.BigEndian.PutUint64(data[0:8], footer.IndexOffset)
	binary.BigEndian.PutUint64(data[8:16], footer.IndexSize)
	return data
}
