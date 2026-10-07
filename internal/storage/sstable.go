package storage

import (
	"encoding/binary"
	"errors"
)

const MaxBlockSize = 4 * 1024

var ErrRecordTooLarge = errors.New("record field exceeds uint32 length")

type Record struct {
	Key   []byte
	Value []byte
}

type Block struct {
	Records []Record
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

func EncodeRecord(record Record) []byte {
	data := make([]byte, 8+len(record.Key)+len(record.Value))
	binary.BigEndian.PutUint32(data[0:4], uint32(len(record.Key)))
	binary.BigEndian.PutUint32(data[4:8], uint32(len(record.Value)))
	copy(data[8:], record.Key)
	copy(data[8+len(record.Key):], record.Value)
	return data
}

func EncodeBlock(block Block) []byte {
	var data []byte
	for _, record := range block.Records {
		data = append(data, EncodeRecord(record)...)
	}
	return data
}

func SplitBlocks(records []Record) []Block {
	return splitBlocks(records, MaxBlockSize)
}

func splitBlocks(records []Record, maxSize int) []Block {
	blocks := make([]Block, 0)
	current := Block{}
	currentSize := 0

	for _, record := range records {
		recordSize := len(EncodeRecord(record))
		if len(current.Records) > 0 && currentSize+recordSize > maxSize {
			blocks = append(blocks, current)
			current = Block{}
			currentSize = 0
		}

		current.Records = append(current.Records, record)
		currentSize += recordSize
	}

	if len(current.Records) > 0 {
		blocks = append(blocks, current)
	}
	return blocks
}

func Build(records []Record) (*SSTable, error) {
	for _, record := range records {
		if uint64(len(record.Key)) > uint64(^uint32(0)) || uint64(len(record.Value)) > uint64(^uint32(0)) {
			return nil, ErrRecordTooLarge
		}
	}

	blocks := SplitBlocks(records)
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

func EncodeIndex(index Index) []byte {
	data := make([]byte, len(index.Entries)*16)
	for i, entry := range index.Entries {
		offset := i * 16
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
