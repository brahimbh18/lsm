# LSM Key-Value Store

An educational Log-Structured Merge-tree (LSM) key-value store in Go with a
RESP2-compatible TCP server.

## Architecture

The project maintains a clean separation between the storage engine internals and the HTTP adapter layer:

```text
                 External programs / redis-cli
                              │ RESP2
                              ▼
              ┌───────────────────────────┐
              │        RESP2 Server       │
              │   (internal/server)       │
              │                           │
              │ SET / GET / DEL / MGET    │
              │ MSET / EXISTS / PING      │
              └─────────────┬─────────────┘
                            │
                            ▼
                      ┌───────────┐
                      │ engine.DB │
                      └─────┬─────┘
                            │
             ┌──────────────┼──────────────┐
             ▼              ▼              ▼
         MemTables         WAL        L0 SSTables
             │
             ▼
        Flush Worker
```

- **RESP2 Server**: A thin adapter translating RESP2 commands into calls to its
  `server.Store` interface. The production implementation is `engine.DB`; the
  parser lives in `internal/protocol` and has no storage or socket dependency.
- **engine.DB**: Coordinates active and frozen MemTables, WAL logging, and background SSTable flushes.

The engine accepts one database data directory and keeps its storage
mechanisms separated beneath it:

```text
data/
├── wal/
└── tables/
```

## Record formats

Writes receive monotonically increasing sequence numbers from `engine.DB`. The WAL record format is a 16-byte big-endian header (`keyLen` uint32,
`valueLen` uint32, `seq` uint64), followed by key bytes, value bytes, and the
existing 4-byte CRC-32 checksum. The high bit of `keyLen` marks a tombstone;
tombstones must have a zero `valueLen`. SSTable records use the same header and
tombstone flag, followed by key and value bytes. The index and footer formats
are unchanged.

This reserves key lengths with the high bit set and is an intentional on-disk
format change for tombstone records. Existing records with ordinary key
lengths remain readable; no migration is provided for old files containing
reserved-length keys.

---

## Compaction policy

Memtables default to 16 MiB. A flush creates one L0 SSTable whose exact size
depends on key/value lengths and SSTable metadata. Compaction uses on-disk
bytes rather than file count as the level-pressure metric:

- L0 compaction starts at 64 MiB or four files, whichever comes first.
- L1 targets 640 MiB.
- Each following level targets 10 times the previous level.
- L0 uses a file-count trigger because L0 files can overlap and increase read
  amplification; higher levels use byte pressure only.

The thresholds are represented by `storage/compaction.CompactionPolicy`. The
engine triggers automatic L0 compaction from accumulated bytes or four files,
whichever comes first. The
`storage/sstable` package provides SSTable encoding and decoding, while
`storage/compaction` provides overlap selection and `CompactSSTables`, which
merges tables by sequence number while retaining tombstones. Level membership
and ranges are persisted in the table manifest. Compaction runs as independent
bounded passes: an L0 pass can promote to L1, and later passes can promote L1
to L2 and so on. A dedicated serialized compaction worker continuously rescans
level pressure after each pass, without recursively loading all levels into one
operation. Input files are removed only after the manifest update is durable.

---

## Starting the Server

Build and start the RESP2 server using `cmd/lsm-server`:

```bash
go run ./cmd/lsm-server --addr :6380 --data-dir ./data --wal-sync everysec --wal-sync-interval 1s
```

Command-line flags:
- `--addr`: RESP2 TCP listen address (default: `:6380`).
- `--data-dir`: Database root directory; WALs and SSTables are stored in its `wal/` and `tables/` subdirectories (default: `./data`).
- `--wal-sync`: WAL durability policy: `always` (default), `everysec`, or `never`.
- `--wal-sync-interval`: Background sync interval used by `everysec` (default: `1s`).

`everysec` acknowledges writes after they reach the WAL file and syncs the
active WAL periodically; it also performs a final sync during graceful
shutdown. `never` does not call `fsync`, including during graceful shutdown.

At startup, the server prints:
```text
LSM RESP2 server listening on :6380
data directory: ./data
wal sync policy=always interval=1s
```

The server cleanly shuts down on `SIGINT` (Ctrl+C) and `SIGTERM`:
1. Ceases accepting new RESP2 connections.
2. Gracefully finishes in-flight requests.
3. Closes `engine.DB` and drains background flush and compaction workers before
   process exit.

---

## Interacting with the Server (redis-cli)

### Set and get a key

```bash
redis-cli -p 6380 SET foo hello
redis-cli -p 6380 GET foo
```

### Delete and multi-key commands

```bash
redis-cli -p 6380 DEL foo
redis-cli -p 6380 MSET a one b two
redis-cli -p 6380 MGET a b missing
redis-cli -p 6380 EXISTS a b missing
```

Missing keys return `(nil)` and deleting a missing key does not increase the
delete count.

---

## Running Tests

Run all unit tests:

```bash
go test ./...
```

Run tests with data race detection:

```bash
go test -race ./...
```
