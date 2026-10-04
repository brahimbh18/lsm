package engine

type SyncMode int

const (
    SyncModeAsync SyncMode = iota
    SyncModeSync
)

type Options struct {
    WALPath  string
    SyncMode SyncMode
}