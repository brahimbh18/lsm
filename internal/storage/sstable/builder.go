package sstable

import (
	"encoding/binary"
	"errors"

	"lsm/internal/memtable"
)

var ErrRecordTooLarge = errors.New("record field exceeds uint32 length")
var ErrMalformedRecord = errors.New("malformed SSTable record")

const tombstoneFlag = uint32(1 << 31)

type Builder struct {
}

func NewBuilder() *Builder {
	return &Builder{}
}

func (b *Builder) Build(table *memtable.MemTable) (*SSTable, error) {
	entries := table.Entries()
	for _, entry := range entries {
		if uint64(len(entry.Key)) > uint64(tombstoneFlag-1) || uint64(len(entry.Value)) > uint64(^uint32(0)) {
			return nil, ErrRecordTooLarge
		}
		if entry.Tombstone && len(entry.Value) != 0 {
			return nil, ErrMalformedRecord
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
	data := make([]byte, 16+len(entry.Key)+len(entry.Value))
	keyLength := uint32(len(entry.Key))
	if entry.Tombstone {
		keyLength |= tombstoneFlag
	}
	binary.BigEndian.PutUint32(data[0:4], keyLength)
	binary.BigEndian.PutUint32(data[4:8], uint32(len(entry.Value)))
	binary.BigEndian.PutUint64(data[8:16], entry.Seq)
	copy(data[16:], entry.Key)
	copy(data[16+len(entry.Key):], entry.Value)
	return data
}

func DecodeRecord(data []byte) (memtable.Entry, int, error) {
	if len(data) < 16 {
		return memtable.Entry{}, 0, ErrMalformedRecord
	}
	encodedKeyLen := binary.BigEndian.Uint32(data[0:4])
	tombstone := encodedKeyLen&tombstoneFlag != 0
	keyLen := uint64(encodedKeyLen &^ tombstoneFlag)
	valueLen := uint64(binary.BigEndian.Uint32(data[4:8]))
	if tombstone && valueLen != 0 {
		return memtable.Entry{}, 0, ErrMalformedRecord
	}
	recordLen := uint64(16) + keyLen + valueLen
	if recordLen > uint64(len(data)) {
		return memtable.Entry{}, 0, ErrMalformedRecord
	}
	keyEnd := 16 + int(keyLen)
	value := append([]byte(nil), data[keyEnd:int(recordLen)]...)
	if tombstone {
		value = nil
	}
	return memtable.Entry{
		Key:       append([]byte(nil), data[16:keyEnd]...),
		Value:     value,
		Seq:       binary.BigEndian.Uint64(data[8:16]),
		Tombstone: tombstone,
	}, int(recordLen), nil
}

func MaxSequence(data []byte) (uint64, error) {
	if len(data) < 16 {
		return 0, ErrMalformedRecord
	}
	footerStart := len(data) - 16
	indexOffset := binary.BigEndian.Uint64(data[footerStart : footerStart+8])
	indexSize := binary.BigEndian.Uint64(data[footerStart+8:])
	if indexOffset > uint64(footerStart) || indexSize != uint64(footerStart)-indexOffset || indexSize%16 != 0 {
		return 0, ErrMalformedRecord
	}

	var maximum uint64
	for offset := indexOffset; offset < indexOffset+indexSize; offset += 16 {
		blockOffset := binary.BigEndian.Uint64(data[offset : offset+8])
		blockSize := binary.BigEndian.Uint64(data[offset+8 : offset+16])
		if blockOffset > indexOffset || blockSize > indexOffset-blockOffset {
			return 0, ErrMalformedRecord
		}
		blockEnd := blockOffset + blockSize
		for recordOffset := blockOffset; recordOffset < blockEnd; {
			entry, recordSize, err := DecodeRecord(data[recordOffset:blockEnd])
			if err != nil || recordSize <= 0 {
				return 0, ErrMalformedRecord
			}
			if entry.Seq > maximum {
				maximum = entry.Seq
			}
			recordOffset += uint64(recordSize)
		}
	}
	return maximum, nil
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

// DecodeSSTable validates and decodes every record in an SSTable. The
// returned entries retain the table's key order.
func DecodeSSTable(data []byte) ([]memtable.Entry, error) {
	if len(data) < 16 {
		return nil, ErrMalformedRecord
	}
	footerStart := len(data) - 16
	indexOffset := binary.BigEndian.Uint64(data[footerStart : footerStart+8])
	indexSize := binary.BigEndian.Uint64(data[footerStart+8:])
	if indexOffset > uint64(footerStart) || indexSize != uint64(footerStart)-indexOffset || indexSize%16 != 0 {
		return nil, ErrMalformedRecord
	}

	entries := make([]memtable.Entry, 0)
	for indexPos := indexOffset; indexPos < indexOffset+indexSize; indexPos += 16 {
		indexEnd := indexPos + 16
		if indexEnd > uint64(footerStart) {
			return nil, ErrMalformedRecord
		}
		blockOffset := binary.BigEndian.Uint64(data[indexPos : indexPos+8])
		blockSize := binary.BigEndian.Uint64(data[indexPos+8 : indexEnd])
		if blockOffset > indexOffset || blockSize > indexOffset-blockOffset {
			return nil, ErrMalformedRecord
		}
		blockEnd := blockOffset + blockSize
		for recordPos := blockOffset; recordPos < blockEnd; {
			entry, recordSize, err := DecodeRecord(data[recordPos:blockEnd])
			if err != nil || recordSize <= 0 {
				return nil, ErrMalformedRecord
			}
			entries = append(entries, entry)
			recordPos += uint64(recordSize)
		}
	}
	return entries, nil
}
