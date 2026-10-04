package wal

import (
    "encoding/binary"
    "hash/crc32"
)

const headerSize = 8
const checksumSize = 4

func encode(key, value []byte) []byte {
    size := headerSize + len(key) + len(value) + checksumSize
    buf := make([]byte, size)

    binary.BigEndian.PutUint32(buf[0:4], uint32(len(key)))
    binary.BigEndian.PutUint32(buf[4:8], uint32(len(value)))

    copy(buf[8:], key)
    copy(buf[8+len(key):], value)

    checksum := crc32.ChecksumIEEE(buf[:size-checksumSize])

    binary.BigEndian.PutUint32(
        buf[size-checksumSize:],
        checksum,
    )

    return buf
}