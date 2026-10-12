package engine

import "lsm/internal/storage/compaction"

const DefaultMemTableMaxSize = 16 * 1024 * 1024 // 16 MiB

type DebugLogger interface {
	Printf(format string, args ...any)
}

type SyncMode int

const (
	SyncModeAsync SyncMode = iota
	SyncModeSync
)

type Options struct {
	DataDir          string
	MemTableMaxSize  int
	SyncMode         SyncMode
	DebugLogger      DebugLogger
	DebugLevel       int
	CompactionPolicy compaction.CompactionPolicy
}
