package memtable

import (
	"bytes"
	"errors"
	"sort"
	"sync"
)

var ErrImmutable = errors.New("cannot write to immutable memtable")

type Entry struct {
	Key       []byte
	Value     []byte
	Seq       uint64
	Tombstone bool
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
	return m.PutEntry(Entry{Key: key, Value: value})
}

func (m *MemTable) PutEntry(input Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.frozen {
		return ErrImmutable
	}

	i := sort.Search(len(m.entries), func(i int) bool {
		return bytes.Compare(m.entries[i].Key, input.Key) >= 0
	})

	entry := Entry{
		Key:       bytes.Clone(input.Key),
		Value:     bytes.Clone(input.Value),
		Seq:       input.Seq,
		Tombstone: input.Tombstone,
	}

	if i < len(m.entries) && bytes.Equal(m.entries[i].Key, input.Key) {
		m.size -= len(m.entries[i].Key) + len(m.entries[i].Value)
		m.entries[i] = entry
	} else {
		m.entries = append(m.entries, Entry{})
		copy(m.entries[i+1:], m.entries[i:])
		m.entries[i] = entry
	}

	m.size += len(input.Key) + len(input.Value)
	return nil
}

func (m *MemTable) Get(key []byte) ([]byte, bool) {
	entry, ok := m.Lookup(key)
	if !ok || entry.Tombstone {
		return nil, false
	}
	return entry.Value, true
}

func (m *MemTable) Lookup(key []byte) (Entry, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	i := sort.Search(len(m.entries), func(i int) bool {
		return bytes.Compare(m.entries[i].Key, key) >= 0
	})

	if i == len(m.entries) || !bytes.Equal(m.entries[i].Key, key) {
		return Entry{}, false
	}

	entry := m.entries[i]
	entry.Key = bytes.Clone(entry.Key)
	entry.Value = bytes.Clone(entry.Value)
	return entry, true
}

func (m *MemTable) Entries() []Entry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entries := make([]Entry, len(m.entries))
	for index, entry := range m.entries {
		entries[index] = Entry{
			Key:       bytes.Clone(entry.Key),
			Value:     bytes.Clone(entry.Value),
			Seq:       entry.Seq,
			Tombstone: entry.Tombstone,
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
