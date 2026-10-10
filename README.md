# LSM Key-Value Store

An educational Log-Structured Merge-tree (LSM) key-value store in Go.

## Architecture

The project maintains a clean separation between the storage engine internals and the HTTP adapter layer:

```text
                 External programs / curl
                  │         │          │
                  │   HTTP  │          │
                  ▼         ▼          ▼
              ┌───────────────────────────┐
              │        HTTP Server        │
              │   (internal/server)       │
              │                           │
              │   PUT /kv/:key            │
              │   GET /kv/:key            │
              │   GET /health             │
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

- **HTTP Server**: A thin adapter translating incoming JSON HTTP requests into calls to its `server.Store` interface. The production implementation is `engine.DB`, but the adapter is not coupled to that concrete engine and does not contain storage logic or server-level mutexes.
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

## Starting the Server

Build and start the server using `cmd/lsm-server`:

```bash
go run ./cmd/lsm-server --addr :8080 --data-dir ./data
```

Command-line flags:
- `--addr`: HTTP listen address (default: `:8080`).
- `--data-dir`: Database root directory; WALs and SSTables are stored in its `wal/` and `tables/` subdirectories (default: `./data`).

At startup, the server prints:
```text
LSM server listening on :8080
data directory: ./data
```

The server cleanly shuts down on `SIGINT` (Ctrl+C) and `SIGTERM`:
1. Ceases accepting new HTTP requests.
2. Gracefully finishes in-flight requests.
3. Closes `engine.DB` and drains background flush workers before process exit.

---

## Interacting with the Server (curl)

### Health Check

```bash
curl http://localhost:8080/health
```

Response:
```json
{"status":"ok"}
```

### Put a Key-Value Pair

```bash
curl -X PUT \
  -H 'Content-Type: application/json' \
  -d '{"value":"hello"}' \
  http://localhost:8080/kv/foo
```

Response:
```json
{"key":"foo","status":"ok"}
```

### Get a Key

```bash
curl http://localhost:8080/kv/foo
```

Response:
```json
{"key":"foo","value":"hello"}
```

### Get a Missing Key

```bash
curl -i http://localhost:8080/kv/does-not-exist
```

Response:
```http
HTTP/1.1 404 Not Found
Content-Type: application/json

{"error":"key not found"}
```

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
