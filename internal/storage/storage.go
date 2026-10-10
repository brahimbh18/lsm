package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"lsm/internal/storage/sstable"
)

type Storage struct {
	directory string
	nextID    uint64
	mu        sync.Mutex
}

type WriteResult struct {
	Path string
	Size int64
}

func NewStorage(directory string) (*Storage, error) {
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, err
	}

	nextID, err := nextTableID(directory)
	if err != nil {
		return nil, err
	}

	return &Storage{directory: directory, nextID: nextID}, nil
}

func (s *Storage) Write(table *sstable.SSTable) error {
	_, err := s.WriteWithInfo(table)
	return err
}

func (s *Storage) WriteWithInfo(table *sstable.SSTable) (WriteResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	path := filepath.Join(s.directory, fmt.Sprintf("%06d.sst", s.nextID))
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return WriteResult{}, err
	}

	written, err := file.Write(table.Data)
	if err != nil {
		_ = file.Close()
		return WriteResult{}, err
	}
	if written != len(table.Data) {
		_ = file.Close()
		return WriteResult{}, io.ErrShortWrite
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return WriteResult{}, err
	}
	if err := file.Close(); err != nil {
		return WriteResult{}, err
	}

	s.nextID++
	return WriteResult{Path: path, Size: int64(len(table.Data))}, nil
}

func nextTableID(directory string) (uint64, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return 0, err
	}

	var nextID uint64 = 1
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sst" {
			continue
		}
		id, err := strconv.ParseUint(strings.TrimSuffix(entry.Name(), ".sst"), 10, 64)
		if err != nil {
			continue
		}
		if id >= nextID {
			nextID = id + 1
		}
	}
	return nextID, nil
}
