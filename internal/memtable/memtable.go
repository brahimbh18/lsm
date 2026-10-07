package memtable

import (
	"bytes"
	"errors"
	"sort"
	"sync"
)

var ErrImmutable = errors.New("cannot write to immutable memtable")

const MaxSize = 16 * 1024 * 1024 // 16 MiB
type Entry struct {
	Key   []byte
	Value []byte
}

type MemTable struct {
	mu      sync.RWMutex
	entries []Entry
	size    int
	frozen  bool
	maxSize int
}

func New(maxSize int) *MemTable {
	return &MemTable{
		entries: make([]Entry, 0),
		maxSize: maxSize,
	}
}

func (m *MemTable) Put(key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.frozen {
		return ErrImmutable
	}

	i := sort.Search(len(m.entries), func(i int) bool {
		return bytes.Compare(m.entries[i].Key, key) >= 0
	})

	entry := Entry{
		Key:   bytes.Clone(key),
		Value: bytes.Clone(value),
	}

	if i < len(m.entries) && bytes.Equal(m.entries[i].Key, key) {
		m.size -= len(m.entries[i].Key) + len(m.entries[i].Value)
		m.entries[i] = entry
	} else {
		m.entries = append(m.entries, Entry{})
		copy(m.entries[i+1:], m.entries[i:])
		m.entries[i] = entry
	}

	m.size += len(key) + len(value)
	return nil
}

func (m *MemTable) Get(key []byte) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	i := sort.Search(len(m.entries), func(i int) bool {
		return bytes.Compare(m.entries[i].Key, key) >= 0
	})

	if i == len(m.entries) || !bytes.Equal(m.entries[i].Key, key) {
		return nil, false
	}

	return bytes.Clone(m.entries[i].Value), true
}

func (m *MemTable) Entries() []Entry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entries := make([]Entry, len(m.entries))
	for index, entry := range m.entries {
		entries[index] = Entry{
			Key:   bytes.Clone(entry.Key),
			Value: bytes.Clone(entry.Value),
		}
	}
	return entries
}

func (m *MemTable) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.size
}

func (m *MemTable) IsFull() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.size >= m.maxSize
}

func (m *MemTable) Freeze() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.frozen = true
}
