package storage

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestEncodeRecord(t *testing.T) {
	tests := []struct {
		name   string
		record Record
	}{
		{name: "empty", record: Record{}},
		{name: "normal", record: Record{Key: []byte("cat"), Value: []byte("black")}},
		{name: "large", record: Record{Key: bytes.Repeat([]byte{'k'}, 256), Value: bytes.Repeat([]byte{'v'}, 512)}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoded := EncodeRecord(test.record)
			if len(encoded) != 8+len(test.record.Key)+len(test.record.Value) {
				t.Fatalf("encoded length = %d, want %d", len(encoded), 8+len(test.record.Key)+len(test.record.Value))
			}
			if got := binary.BigEndian.Uint32(encoded[0:4]); got != uint32(len(test.record.Key)) {
				t.Fatalf("key length = %d, want %d", got, len(test.record.Key))
			}
			if got := binary.BigEndian.Uint32(encoded[4:8]); got != uint32(len(test.record.Value)) {
				t.Fatalf("value length = %d, want %d", got, len(test.record.Value))
			}
			if !bytes.Equal(encoded[8:8+len(test.record.Key)], test.record.Key) {
				t.Fatal("encoded key does not match")
			}
			if !bytes.Equal(encoded[8+len(test.record.Key):], test.record.Value) {
				t.Fatal("encoded value does not match")
			}
		})
	}

	first := EncodeRecord(Record{Key: []byte("a"), Value: []byte("1")})
	second := EncodeRecord(Record{Key: []byte("b"), Value: []byte("22")})
	concatenated := append(first, second...)
	secondStart := 8 + len("a") + len("1")
	if !bytes.Equal(concatenated[secondStart:], second) {
		t.Fatal("concatenated records are not self-delimiting")
	}
}

func TestSplitBlocks(t *testing.T) {
	records := []Record{
		{Key: []byte("a"), Value: []byte("one")},
		{Key: []byte("b"), Value: []byte("two")},
		{Key: []byte("c"), Value: []byte("six")},
		{Key: []byte("d"), Value: []byte("new")},
	}
	maxSize := len(EncodeRecord(records[0])) + len(EncodeRecord(records[1]))
	blocks := splitBlocks(records, maxSize)

	if len(blocks) != 2 {
		t.Fatalf("block count = %d, want 2", len(blocks))
	}
	flattened := make([]Record, 0, len(records))
	for _, block := range blocks {
		if len(block.Records) == 0 {
			t.Fatal("split produced an empty block")
		}
		flattened = append(flattened, block.Records...)
	}
	if len(flattened) != len(records) {
		t.Fatalf("record count = %d, want %d", len(flattened), len(records))
	}
	for recordIndex, record := range flattened {
		if !bytes.Equal(record.Key, records[recordIndex].Key) || !bytes.Equal(record.Value, records[recordIndex].Value) {
			t.Fatalf("record %d changed during splitting", recordIndex)
		}
	}

	largeRecord := Record{Key: []byte("large"), Value: bytes.Repeat([]byte{'x'}, 32)}
	largeBlocks := splitBlocks([]Record{largeRecord}, 1)
	if len(largeBlocks) != 1 || len(largeBlocks[0].Records) != 1 {
		t.Fatal("record larger than block size was split")
	}
}

func TestEncodeBlock(t *testing.T) {
	block := Block{Records: []Record{
		{Key: []byte("a"), Value: []byte("one")},
		{Key: []byte("b"), Value: []byte("two")},
	}}
	want := append(EncodeRecord(block.Records[0]), EncodeRecord(block.Records[1])...)
	if got := EncodeBlock(block); !bytes.Equal(got, want) {
		t.Fatalf("encoded block = %x, want %x", got, want)
	}
}

func TestBuildSSTableLayout(t *testing.T) {
	records := []Record{
		{Key: []byte("a"), Value: bytes.Repeat([]byte{'a'}, MaxBlockSize)},
		{Key: []byte("b"), Value: bytes.Repeat([]byte{'b'}, MaxBlockSize)},
	}
	blocks := SplitBlocks(records)
	table, err := Build(records)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	dataEnd := 0
	for _, block := range blocks {
		dataEnd += len(EncodeBlock(block))
	}
	indexSize := len(blocks) * 16
	footerStart := len(table.Data) - 16
	if footerStart != dataEnd+indexSize {
		t.Fatalf("footer starts at %d, want %d", footerStart, dataEnd+indexSize)
	}

	footer := Footer{
		IndexOffset: binary.BigEndian.Uint64(table.Data[footerStart : footerStart+8]),
		IndexSize:   binary.BigEndian.Uint64(table.Data[footerStart+8:]),
	}
	if footer.IndexOffset != uint64(dataEnd) || footer.IndexSize != uint64(indexSize) {
		t.Fatalf("footer = %+v, want offset %d size %d", footer, dataEnd, indexSize)
	}

	for blockIndex, block := range blocks {
		entryOffset := int(footer.IndexOffset) + blockIndex*16
		entry := IndexEntry{
			Offset: binary.BigEndian.Uint64(table.Data[entryOffset : entryOffset+8]),
			Size:   binary.BigEndian.Uint64(table.Data[entryOffset+8 : entryOffset+16]),
		}
		encodedBlock := EncodeBlock(block)
		if entry.Offset+entry.Size > uint64(len(table.Data)) {
			t.Fatalf("index entry %d points outside table: %+v", blockIndex, entry)
		}
		if !bytes.Equal(table.Data[entry.Offset:entry.Offset+entry.Size], encodedBlock) {
			t.Fatalf("block %d does not match its index entry", blockIndex)
		}
	}
}
