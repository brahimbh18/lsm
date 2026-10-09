package storage

import (
	"bytes"
	"encoding/binary"
	"testing"

	"lsm/internal/memtable"
)

func TestEncodeRecord(t *testing.T) {
	tests := []struct {
		name  string
		entry memtable.Entry
	}{
		{name: "empty", entry: memtable.Entry{}},
		{name: "normal", entry: memtable.Entry{Key: []byte("cat"), Value: []byte("black"), Seq: 42}},
		{name: "large", entry: memtable.Entry{Key: bytes.Repeat([]byte{'k'}, 256), Value: bytes.Repeat([]byte{'v'}, 512)}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoded := EncodeRecord(test.entry)
			if len(encoded) != 16+len(test.entry.Key)+len(test.entry.Value) {
				t.Fatalf("encoded length = %d, want %d", len(encoded), 16+len(test.entry.Key)+len(test.entry.Value))
			}
			if got := binary.BigEndian.Uint32(encoded[0:4]); got != uint32(len(test.entry.Key)) {
				t.Fatalf("key length = %d, want %d", got, len(test.entry.Key))
			}
			if got := binary.BigEndian.Uint32(encoded[4:8]); got != uint32(len(test.entry.Value)) {
				t.Fatalf("value length = %d, want %d", got, len(test.entry.Value))
			}
			if got := binary.BigEndian.Uint64(encoded[8:16]); got != test.entry.Seq {
				t.Fatalf("sequence = %d, want %d", got, test.entry.Seq)
			}
			if !bytes.Equal(encoded[16:16+len(test.entry.Key)], test.entry.Key) {
				t.Fatal("encoded key does not match")
			}
			if !bytes.Equal(encoded[16+len(test.entry.Key):], test.entry.Value) {
				t.Fatal("encoded value does not match")
			}
			decoded, size, err := DecodeRecord(encoded)
			if err != nil || size != len(encoded) || !bytes.Equal(decoded.Key, test.entry.Key) || !bytes.Equal(decoded.Value, test.entry.Value) || decoded.Seq != test.entry.Seq {
				t.Fatalf("DecodeRecord() = %+v, size %d, error %v", decoded, size, err)
			}
		})
	}

	first := EncodeRecord(memtable.Entry{Key: []byte("a"), Value: []byte("1")})
	second := EncodeRecord(memtable.Entry{Key: []byte("b"), Value: []byte("22")})
	concatenated := append(first, second...)
	secondStart := 16 + len("a") + len("1")
	if !bytes.Equal(concatenated[secondStart:], second) {
		t.Fatal("concatenated records are not self-delimiting")
	}
}

func TestBuilderSplitsBlocks(t *testing.T) {
	entries := []memtable.Entry{
		{Key: []byte("a"), Value: bytes.Repeat([]byte{'a'}, 3000)},
		{Key: []byte("b"), Value: bytes.Repeat([]byte{'b'}, 3000)},
		{Key: []byte("c"), Value: bytes.Repeat([]byte{'c'}, 3000)},
	}
	table := memtable.New(16 * 1024 * 1024)
	for _, entry := range entries {
		if err := table.Put(entry.Key, entry.Value); err != nil {
			t.Fatalf("Put() error = %v", err)
		}
	}
	built, err := NewBuilder().Build(table)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	footerStart := len(built.Data) - 16
	indexOffset := binary.BigEndian.Uint64(built.Data[footerStart : footerStart+8])
	indexSize := binary.BigEndian.Uint64(built.Data[footerStart+8:])
	if indexSize/16 != uint64(len(entries)) {
		t.Fatalf("block count = %d, want %d", indexSize/16, len(entries))
	}

	for entryIndex, entry := range entries {
		indexEntryOffset := int(indexOffset) + entryIndex*16
		blockOffset := binary.BigEndian.Uint64(built.Data[indexEntryOffset : indexEntryOffset+8])
		blockSize := binary.BigEndian.Uint64(built.Data[indexEntryOffset+8 : indexEntryOffset+16])
		if !bytes.Equal(built.Data[blockOffset:blockOffset+blockSize], EncodeRecord(entry)) {
			t.Fatalf("entry %d was not kept as one block", entryIndex)
		}
	}

	largeEntry := memtable.Entry{Key: []byte("large"), Value: bytes.Repeat([]byte{'x'}, MaxBlockSize+1)}
	largeTable := memtable.New(16 * 1024 * 1024)
	if err := largeTable.Put(largeEntry.Key, largeEntry.Value); err != nil {
		t.Fatalf("large Put() error = %v", err)
	}
	if blocks := SplitBlocks(largeTable.Entries()); len(blocks) != 1 || len(blocks[0].Records) != 1 {
		t.Fatal("oversized entry was split")
	}
	largeBuilt, err := NewBuilder().Build(largeTable)
	if err != nil {
		t.Fatalf("large Build() error = %v", err)
	}
	largeFooterStart := len(largeBuilt.Data) - 16
	largeIndexSize := binary.BigEndian.Uint64(largeBuilt.Data[largeFooterStart+8:])
	if largeIndexSize != 16 {
		t.Fatalf("oversized entry block count = %d, want 1", largeIndexSize/16)
	}
}

func TestEncodeBlock(t *testing.T) {
	block := Block{Records: []memtable.Entry{
		{Key: []byte("a"), Value: []byte("one")},
		{Key: []byte("b"), Value: []byte("two")},
	}}
	want := append(EncodeRecord(block.Records[0]), EncodeRecord(block.Records[1])...)
	if got := EncodeBlock(block); !bytes.Equal(got, want) {
		t.Fatalf("encoded block = %x, want %x", got, want)
	}
}

func TestDecodeRecordRejectsMalformedInput(t *testing.T) {
	if _, _, err := DecodeRecord([]byte{1, 2, 3}); err != ErrMalformedRecord {
		t.Fatalf("DecodeRecord() error = %v, want %v", err, ErrMalformedRecord)
	}
	record := EncodeRecord(memtable.Entry{Key: []byte("key"), Value: []byte("value"), Seq: 3})
	if _, _, err := DecodeRecord(record[:len(record)-1]); err != ErrMalformedRecord {
		t.Fatalf("truncated DecodeRecord() error = %v, want %v", err, ErrMalformedRecord)
	}
}

func TestBuilderSSTableLayout(t *testing.T) {
	table := memtable.New(16 * 1024 * 1024)
	_ = table.Put([]byte("a"), bytes.Repeat([]byte{'a'}, MaxBlockSize))
	_ = table.Put([]byte("b"), bytes.Repeat([]byte{'b'}, MaxBlockSize))
	blocks := SplitBlocks(table.Entries())
	built, err := NewBuilder().Build(table)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	tableData := built.Data

	dataEnd := 0
	for _, block := range blocks {
		dataEnd += len(EncodeBlock(block))
	}
	indexSize := len(blocks) * 16
	footerStart := len(tableData) - 16
	if footerStart != dataEnd+indexSize {
		t.Fatalf("footer starts at %d, want %d", footerStart, dataEnd+indexSize)
	}

	footer := Footer{
		IndexOffset: binary.BigEndian.Uint64(tableData[footerStart : footerStart+8]),
		IndexSize:   binary.BigEndian.Uint64(tableData[footerStart+8:]),
	}
	if footer.IndexOffset != uint64(dataEnd) || footer.IndexSize != uint64(indexSize) {
		t.Fatalf("footer = %+v, want offset %d size %d", footer, dataEnd, indexSize)
	}

	for blockIndex, block := range blocks {
		entryOffset := int(footer.IndexOffset) + blockIndex*16
		entry := IndexEntry{
			Offset: binary.BigEndian.Uint64(tableData[entryOffset : entryOffset+8]),
			Size:   binary.BigEndian.Uint64(tableData[entryOffset+8 : entryOffset+16]),
		}
		encodedBlock := EncodeBlock(block)
		if entry.Offset+entry.Size > uint64(len(tableData)) {
			t.Fatalf("index entry %d points outside table: %+v", blockIndex, entry)
		}
		if !bytes.Equal(tableData[entry.Offset:entry.Offset+entry.Size], encodedBlock) {
			t.Fatalf("block %d does not match its index entry", blockIndex)
		}
	}
}
