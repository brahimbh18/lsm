package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sync"

	"lsm/internal/memtable"
)

var ErrCorruptRecord = errors.New("corrupt WAL record")

type WAL struct {
	mu   sync.Mutex
	file *os.File
	path string
}

func Open(path string) (*WAL, error) {
	file, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		0644,
	)

	if err != nil {
		return nil, err
	}

	return &WAL{file: file, path: path}, nil
}

func (w *WAL) Path() string {
	return w.path
}

func (w *WAL) Append(key, value []byte, sequences ...uint64) error {
	entry := memtable.Entry{Key: key, Value: value}
	if len(sequences) > 1 {
		return ErrInvalidSequence
	}
	if len(sequences) == 1 {
		entry.Seq = sequences[0]
	}
	return w.AppendEntry(entry)
}

func (w *WAL) AppendEntry(entry memtable.Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if entry.Tombstone && len(entry.Value) != 0 {
		return ErrCorruptRecord
	}
	record := encode(entry)

	n, err := w.file.Write(record)
	if err != nil {
		return err
	}

	if n != len(record) {
		return io.ErrShortWrite
	}

	return nil
}

func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.file.Sync()
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.file.Close()
}

func (w *WAL) Replay(fn func(memtable.Entry) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	for {
		header := make([]byte, headerSize)

		_, err := io.ReadFull(w.file, header)

		if err == io.EOF {
			break
		}

		if err == io.ErrUnexpectedEOF {
			break // incomplete final record
		}

		if err != nil {
			return err
		}

		encodedKeyLen := binary.BigEndian.Uint32(header[:4])
		tombstone := encodedKeyLen&tombstoneFlag != 0
		keyLen := encodedKeyLen &^ tombstoneFlag
		valueLen := binary.BigEndian.Uint32(header[4:8])
		seq := binary.BigEndian.Uint64(header[8:16])
		if tombstone && valueLen != 0 {
			return ErrCorruptRecord
		}

		// Prevent unreasonable allocations from corrupt lengths.
		if uint64(keyLen)+uint64(valueLen) > 64*1024*1024 {
			return ErrCorruptRecord
		}

		payload := make([]byte, int(keyLen)+int(valueLen)+4)

		if _, err := io.ReadFull(w.file, payload); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}

		record := append(header, payload...)

		expected := binary.BigEndian.Uint32(
			record[len(record)-4:],
		)

		actual := crc32.ChecksumIEEE(record[:len(record)-4])

		if expected != actual {
			return ErrCorruptRecord
		}

		key := payload[:keyLen]
		value := payload[keyLen : keyLen+valueLen]
		if tombstone {
			value = nil
		}

		if err := fn(memtable.Entry{Key: key, Value: value, Seq: seq, Tombstone: tombstone}); err != nil {
			return err
		}
	}

	_, err := w.file.Seek(0, io.SeekEnd)
	return err
}
