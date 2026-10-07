package engine

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"

	"lsm/internal/storage"
)

const (
	workloadRecords = 600_000
	workloadValue   = 64
)

func TestLargeWorkload(t *testing.T) {
	directory := t.TempDir()
	debugLevel, _ := strconv.Atoi(os.Getenv("LSM_DEBUG"))
	options := Options{
		WALDir:          filepath.Join(directory, "wal"),
		DataDir:         filepath.Join(directory, "tables"),
		MemTableMaxSize: DefaultMemTableMaxSize,
		DebugLogger:     testingLogger{t: t},
		DebugLevel:      debugLevel,
	}

	db, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()

	start := time.Now()
	for index := 0; index < workloadRecords; index++ {
		key, value := workloadEntry(index)
		if err := db.Put(key, value); err != nil {
			t.Fatalf("Put(%q): %v", key, err)
		}
	}
	writeDuration := time.Since(start)

	if err := db.WaitForFlushes(); err != nil {
		t.Fatalf("WaitForFlushes(): %v", err)
	}

	stats := db.FlushStats()
	if stats.MemTablesFrozen < 2 {
		t.Fatalf("MemTables frozen = %d, want at least 2", stats.MemTablesFrozen)
	}
	if stats.MemTablesFlushed != stats.MemTablesFrozen {
		t.Fatalf("MemTables flushed = %d, frozen = %d", stats.MemTablesFlushed, stats.MemTablesFrozen)
	}
	if stats.L0SSTablesCreated != stats.MemTablesFlushed {
		t.Fatalf("L0 SSTables = %d, flushed MemTables = %d", stats.L0SSTablesCreated, stats.MemTablesFlushed)
	}

	files, err := filepath.Glob(filepath.Join(options.DataDir, "*.sst"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != stats.L0SSTablesCreated {
		t.Fatalf("SSTable files = %d, want %d", len(files), stats.L0SSTablesCreated)
	}
	walFiles, err := filepath.Glob(filepath.Join(options.WALDir, "wal-*.log"))
	if err != nil {
		t.Fatal(err)
	}
	if len(walFiles) != 1 {
		t.Fatalf("active WAL files = %d, want 1", len(walFiles))
	}
	sort.Strings(files)
	for _, file := range files {
		info, err := os.Stat(file)
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() == 0 {
			t.Fatalf("SSTable %s is empty", file)
		}
		verifySSTableLayout(t, file)
	}

	t.Logf("========== LSM WORKLOAD SUMMARY ==========")
	t.Logf("Records written:       %d", stats.RecordsWritten)
	t.Logf("Bytes written:         %d", stats.BytesWritten)
	t.Logf("MemTables created:     %d", stats.MemTablesCreated)
	t.Logf("MemTables frozen:      %d", stats.MemTablesFrozen)
	t.Logf("MemTables flushed:     %d", stats.MemTablesFlushed)
	t.Logf("L0 SSTables created:   %d", stats.L0SSTablesCreated)
	t.Logf("WAL bytes:             %d", directorySize(t, options.WALDir))
	t.Logf("SSTable bytes:         %d", stats.SSTableBytes)
	t.Logf("Total SSTable blocks:  %d", stats.TotalSSTableBlocks)
	t.Logf("Write duration:        %s", writeDuration)

	queries := []struct {
		name  string
		index int
		found bool
	}{
		{"active MemTable", workloadRecords - 1, true},
		{"flushed MemTable", 0, false},
		{"earlier L0 SSTable", workloadRecords / 4, false},
		{"later L0 SSTable", workloadRecords / 2, false},
		{"missing", workloadRecords, false},
	}
	var latencies []time.Duration
	found := 0
	for _, query := range queries {
		key, want := workloadEntry(query.index)
		queryStart := time.Now()
		value, ok := db.Get(key)
		latency := time.Since(queryStart)
		latencies = append(latencies, latency)
		if ok {
			found++
		}
		if ok != query.found || (ok && !bytes.Equal(value, want)) {
			t.Errorf("%s query: found=%v, want %v", query.name, ok, query.found)
		}
		t.Logf("[QUERY] type=%s key=%s result=%s latency=%s", query.name, key, resultName(ok), latency)
	}
	t.Logf("========== QUERY RESULTS ==========")
	t.Logf("Queries: %d, Found: %d, Not found: %d", len(queries), found, len(queries)-found)
	t.Logf("Query latency: %s", summarizeLatencies(latencies))
	t.Log("SSTable queries are not implemented; flushed-key misses are expected.")
}

func TestFlushFailureKeepsWAL(t *testing.T) {
	directory := t.TempDir()
	db, err := Open(Options{
		WALDir:          filepath.Join(directory, "wal"),
		DataDir:         filepath.Join(directory, "tables"),
		MemTableMaxSize: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	db.flushStorage = failingStorage{}
	if err := db.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := db.WaitForFlushes(); err == nil {
		t.Fatal("WaitForFlushes() error = nil, want flush failure")
	}
	walFiles, err := filepath.Glob(filepath.Join(directory, "wal", "wal-*.log"))
	if err != nil {
		t.Fatal(err)
	}
	if len(walFiles) != 2 {
		t.Fatalf("WAL files = %d, want frozen and active WALs", len(walFiles))
	}
	_ = db.Close()
}

type failingStorage struct{}

func (failingStorage) WriteWithInfo(*storage.SSTable) (storage.WriteResult, error) {
	return storage.WriteResult{}, errors.New("injected SSTable write failure")
}

type testingLogger struct{ t *testing.T }

func (l testingLogger) Printf(format string, args ...any) {
	l.t.Logf(format, args...)
}

func workloadEntry(index int) ([]byte, []byte) {
	key := []byte(fmt.Sprintf("key-%08d", index))
	value := []byte(fmt.Sprintf("value-for-key-%08d", index))
	value = append(value, bytes.Repeat([]byte{'x'}, workloadValue-len(value))...)
	return key, value
}

func verifySSTableLayout(t *testing.T, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) < 16 {
		t.Fatalf("SSTable %s is too small", path)
	}
	footerStart := len(data) - 16
	indexOffset := binary.BigEndian.Uint64(data[footerStart : footerStart+8])
	indexSize := binary.BigEndian.Uint64(data[footerStart+8:])
	if indexOffset > uint64(footerStart) || indexSize != uint64(footerStart)-indexOffset || indexSize%16 != 0 {
		t.Fatalf("invalid SSTable footer in %s", path)
	}
	for offset := indexOffset; offset < indexOffset+indexSize; offset += 16 {
		blockOffset := binary.BigEndian.Uint64(data[offset : offset+8])
		blockSize := binary.BigEndian.Uint64(data[offset+8 : offset+16])
		if blockSize == 0 || blockOffset+blockSize > indexOffset {
			t.Fatalf("invalid block index entry in %s", path)
		}
	}
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}

func directorySize(t *testing.T, directory string) int64 {
	t.Helper()
	var total int64
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		total += fileSize(t, filepath.Join(directory, entry.Name()))
	}
	return total
}

func resultName(found bool) string {
	if found {
		return "found"
	}
	return "not-found"
}

func summarizeLatencies(latencies []time.Duration) string {
	if len(latencies) == 0 {
		return "no active MemTable hits"
	}
	var total time.Duration
	minimum, maximum := latencies[0], latencies[0]
	for _, latency := range latencies {
		total += latency
		minimum = min(minimum, latency)
		maximum = max(maximum, latency)
	}
	sorted := append([]time.Duration(nil), latencies...)
	sort.Slice(sorted, func(left, right int) bool {
		return sorted[left] < sorted[right]
	})
	p95 := sorted[(len(sorted)*95+99)/100-1]
	return fmt.Sprintf("min=%s average=%s p95=%s max=%s", minimum, total/time.Duration(len(latencies)), p95, maximum)
}
