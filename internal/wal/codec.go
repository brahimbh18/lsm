package wal

import (
	"encoding/binary"
	"hash/crc32"

	"lsm/internal/memtable"
)

const headerSize = 16
const checksumSize = 4
const tombstoneFlag = uint32(1 << 31)

func encode(entry memtable.Entry) []byte {
	size := headerSize + len(entry.Key) + len(entry.Value) + checksumSize
	buf := make([]byte, size)

	keyLength := uint32(len(entry.Key))
	if entry.Tombstone {
		keyLength |= tombstoneFlag
	}
	binary.BigEndian.PutUint32(buf[0:4], keyLength)
	binary.BigEndian.PutUint32(buf[4:8], uint32(len(entry.Value)))
	binary.BigEndian.PutUint64(buf[8:16], entry.Seq)

	copy(buf[headerSize:], entry.Key)
	copy(buf[headerSize+len(entry.Key):], entry.Value)

	checksum := crc32.ChecksumIEEE(buf[:size-checksumSize])

	binary.BigEndian.PutUint32(buf[size-checksumSize:], checksum)

	return buf
}
