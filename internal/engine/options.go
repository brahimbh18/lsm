package engine

import (
	"fmt"
	"time"

	"lsm/internal/storage/compaction"
)

const DefaultMemTableMaxSize = 16 * 1024 * 1024 // 16 MiB

type DebugLogger interface {
	Printf(format string, args ...any)
}

type SyncMode int

const (
	SyncModeSync SyncMode = iota
	SyncModeAsync
)

type SyncPolicy int

const (
	SyncPolicyAlways SyncPolicy = iota
	SyncPolicyEverysec
	SyncPolicyNever
)

func (p SyncPolicy) String() string {
	switch p {
	case SyncPolicyAlways:
		return "always"
	case SyncPolicyEverysec:
		return "everysec"
	case SyncPolicyNever:
		return "never"
	default:
		return "unknown"
	}
}

func ParseSyncPolicy(value string) (SyncPolicy, error) {
	switch value {
	case "always":
		return SyncPolicyAlways, nil
	case "everysec":
		return SyncPolicyEverysec, nil
	case "never":
		return SyncPolicyNever, nil
	default:
		return 0, fmt.Errorf("unknown WAL sync policy %q", value)
	}
}

type Options struct {
	DataDir          string
	MemTableMaxSize  int
	SyncPolicy       SyncPolicy
	SyncInterval     time.Duration
	SyncMode         SyncMode
	DebugLogger      DebugLogger
	DebugLevel       int
	CompactionPolicy compaction.CompactionPolicy
}
