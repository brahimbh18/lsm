package engine

type SyncMode int

const (
	SyncModeAsync SyncMode = iota
	SyncModeSync
)

type Options struct {
	WALPath         string
	DataDir         string
	MemTableMaxSize int
	SyncMode        SyncMode
}
