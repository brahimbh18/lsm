# Architecture and Current Behavior

This document records behavior found in the implementation. Paths are relative to the repository root. If this document conflicts with the source, the source is authoritative. Items marked **Unverified** need targeted testing; items marked **Not implemented** are not available behavior.

## Components

- `cmd/lsm/main.go`: line-oriented CLI invoking `Put`, `Get`, and `Delete`.
- `cmd/lsm-server/main.go`: opens the engine and serves TCP, with WAL sync and address options.
- `internal/server/server.go`: accepts connections, parses commands through `internal/protocol`, and invokes a `Store` interface. It handles a bounded number of connections with a semaphore. `RANGE` currently returns an empty array; it is not an implemented scan.
- `internal/protocol/{parse,encode,types}.go`: RESP request parsing, response encoding, and limits.
- `internal/engine/db.go`: sequences writes; manages active/frozen memtables, WALs, reads, flush and compaction workers, manifest state, and close.
- `internal/memtable/memtable.go`: sorted in-memory entries; one latest entry per key in each table.
- `internal/wal/{wal,codec,record}.go`: append, sync, replay, and record encoding.
- `internal/storage/sstable/{builder,sstable}.go`: SSTable builder, blocks, index/footer encoding, validation, decoding, and maximum-sequence scan.
- `internal/storage/{storage,manifest}.go`: SSTable files and JSON manifest persistence.
- `internal/storage/compaction/{compaction,merge}.go`: thresholds, input selection, and sequence-based merge.

## Data structures and formats

`memtable.Entry` carries byte-slice key/value, `Seq uint64`, and `Tombstone bool`. A `MemTable` keeps entries sorted by key and replaces the existing entry for that key. `DB.lastSequence` assigns increasing sequence numbers during writes and is restored from SSTables and replayed WALs.

WAL records have a 16-byte big-endian header: key length (uint32), value length (uint32), and sequence (uint64), followed by key, value, and a 4-byte CRC-32. The high key-length bit marks a tombstone; tombstones have zero value length. WAL replay ignores an incomplete trailing record, rejects a complete record with a bad checksum, and rejects tombstones with a value. See `wal.encode`, `WAL.AppendEntry`, and `WAL.Replay`.

SSTable records use the same 16-byte header and tombstone flag, followed by key and value; they have no per-record CRC in the current encoding. Records are grouped into blocks up to `sstable.MaxBlockSize` (4 KiB, except a single oversized record), followed by 16-byte index entries containing block offset and size, then a 16-byte footer containing index offset and size. `DecodeSSTable` validates boundaries and decodes every record. `DB.Get` does not use the index to seek to a key.

The `tables/MANIFEST` file is JSON containing a `Tables` list and `min_live_wal_id`. Each `TableMeta` stores ID, level, size, smallest key, and largest key. The WAL boundary is the smallest ID that recovery must replay; lower IDs have been installed in SSTables. `WriteManifest` writes `MANIFEST.tmp`, syncs that file, closes it, and renames it to `MANIFEST`; it does not sync the containing directory. SSTable file names are six-digit numeric IDs with `.sst`; WAL names are `wal-` plus a six-digit ID and `.log`. These ID namespaces are independent.

## Write flow

`DB.Put` and `DB.Delete` call `DB.write` (`internal/engine/db.go`). It serializes writers with `queueMu`, checks closed/error/sequence-overflow state, increments `lastSequence`, appends to the active WAL, applies the selected WAL sync policy, then inserts into the active memtable. `MemTable.PutEntry` clones key/value slices. `always` syncs before the write returns; `everysec` marks the active WAL dirty for background sync and syncs it during rotation or close; `never` does not request sync. Consequently, an acknowledged write under `everysec` or `never` can be lost after abrupt failure before its WAL is durable.

When the active memtable reaches its configured byte count, the writer creates a new WAL/memtable, freezes the old memtable, appends the frozen table to `frozenMemtables`, and queues a flush. The size counts key and value bytes for the current unique entry per key; it does not include SSTable/WAL overhead.

## Read flow

`DB.Get` briefly takes `db.mu` to capture the active memtable, frozen memtable pointers, and manifest table metadata as one snapshot. It pins each table ID in that snapshot, releases the lock, then scans all captured layers and selects the matching entry with the greatest sequence. A newest tombstone returns not-found. It releases table pins when finished. SSTable reads still decode every record in a candidate table.

**Current limitation:** SSTable read or decode errors are logged and skipped by `DB.Get`, which can report not-found or an older value instead of a storage error. The public `Get` signature cannot return an error.

## Flush flow

The single flush worker consumes `flushQueue`. `DB.flush` snapshots frozen entries through the memtable API, builds an L0 SSTable, writes and syncs the SSTable file, computes metadata, then serializes the manifest install. It writes a manifest containing the new table and advances `min_live_wal_id` through the WAL IDs represented by that frozen state. After the manifest write succeeds, one `db.mu` critical section publishes the new in-memory manifest and removes the frozen table from the read view. It then closes and removes the represented WAL files. A reader that captured the old frozen table keeps its Go pointer and can finish safely; readers starting after publication find the SSTable. Flush failures are stored as `flushErr`, causing later writes to fail. `WaitForFlushes` waits on `flushPending`, which includes queued/running compaction requests as well as flush work.

The ordering intends to make the SSTable/manifest visible before deleting the corresponding WAL. However, directory entries are not explicitly synced, and failure/crash paths have additional recovery hazards below.

## Startup and recovery

`engine.Open` creates `wal/` and `tables/`, enumerates IDs, opens storage, reads the manifest, and, only when the manifest has no tables, attempts to reconstruct it from all discovered SSTables. It scans every SSTable file for its maximum sequence. It replays every WAL whose ID is at least `min_live_wal_id` into one active memtable; the highest replayed WAL remains open for appends, while earlier replayed WALs are closed and retained until the active memtable flushes. If no WAL is eligible, it creates a new WAL ID above the manifest boundary and existing WAL IDs. SSTable IDs are not consulted when deciding which WALs to replay. A manifest without this field has the zero value, so all extant WALs are replayed conservatively.

Incomplete final WAL records are ignored but are not truncated by `WAL.Replay`; replay seeks back to end before future appends. Recovery does not rotate/flush a large replayed memtable during open.

The manifest boundary is advanced only by a successful ordered flush, in the same manifest update that installs the output SSTable. Thus compaction output IDs cannot affect recovery. A legacy manifest has no boundary; replaying leftover WALs can duplicate records that are already in SSTables, so `Get` compares sequence numbers across all snapshot layers rather than assuming memtable recency implies version recency.

## Compaction flow

After a successful flush, `requestCompactionLocked` queues work if the one-slot compaction queue is available. A dedicated worker repeatedly calls `compactOneTriggeredLevel`. L0 triggers on accumulated bytes or file count; higher levels trigger on byte budget. Defaults are 64 MiB L0, four L0 files, and a 10x level multiplier.

`SelectCompaction` takes every source table at L0, or the first source table at higher levels, plus next-level tables whose key ranges overlap. `CompactSSTables` decodes all inputs, keeps the greatest sequence per key, sorts by key, retains tombstones, and rebuilds an SSTable. The engine snapshots input metadata and performs reads, merge, and output write without `manifestMu`. It then takes `manifestMu` for installation, verifies that all input IDs remain in the current manifest, and builds the replacement from that current manifest so L0 files flushed meanwhile are preserved. After the manifest is written, it publishes the new view. `Get` pins SSTable IDs while reading; compaction waits for pins on its retired inputs to drain before deleting those files. Tombstones are never discarded by the current merge, including at bottommost levels.

## Concurrency model and invariants

- `DB.mu` protects engine fields and coordinates access to active/frozen state; memtables have independent `RWMutex` locks; each WAL serializes append/sync/replay/close with a mutex.
- `queueMu` serializes write/rotation queue ordering. `rotationMu` and `rotationCond` coordinate rotations and waiting writers.
- `manifestMu` serializes manifest persistence and installation. Readers do not hold it during I/O; they snapshot under `db.mu` and pin referenced SSTable IDs. Compaction waits for pinned readers before deleting replaced files.
- One flush worker and one compaction worker serialize their respective background work. The WAL syncer has its own mutex and periodic goroutine under `everysec`.

Correctness constraints to preserve when changing these paths:

1. WAL append precedes memtable visibility; sequence numbers order versions.
2. Frozen memtables are immutable and remain readable while their SSTable is installed.
3. Tombstones suppress all older values; empty byte values remain distinguishable from missing keys.
4. Flush must install its SSTable and advance the manifest WAL boundary before deleting represented WALs. Compaction must not delete inputs before the replacement manifest is durable and readers of older snapshots have finished.
5. Lock ordering must be consistent across write/rotation/close and manifest/read/flush/compaction paths.
6. WAL recovery follows the manifest boundary, never SSTable numeric IDs. The boundary may advance only over WAL IDs whose data is represented by installed tables.

**Confirmed deadlock risk:** the rotation path holds `rotationMu`, then later reacquires `queueMu`; `DB.Close` acquires `queueMu` and then waits for `rotationMu`. An interleaving can deadlock. Existing tests do not cover close during rotation.

## Known limitations and open questions

- **Confirmed:** SSTable reads load and decode full files; no Bloom filter or key-directed block lookup is used.
- **Confirmed:** `DB.Get` cannot distinguish missing keys from storage read/decode errors because errors are logged and skipped.
- **Confirmed:** startup replays eligible WALs into a single active memtable without enforcing its configured threshold.
- **Confirmed:** manifest file sync is followed by rename without a directory sync. Durability of the rename across power loss is therefore not established by the code.
- **Confirmed:** the RESP server does not track connection goroutines or wait for them when `Serve` returns. The command's listener shutdown therefore does not itself guarantee in-flight requests are drained before deferred DB close.
- **Confirmed:** protocol `RANGE` is a stub; there is no ordered scan API in the engine.
- **Not implemented:** replication, consensus, multi-process coordination, snapshots, and distributed durability.
- **Unverified:** complete crash consistency for each partial SSTable write, manifest update, rename, and deletion point. Tests currently cover orderly close/reopen and selected flush failures, not systematic power-loss fault injection.
- **Unverified:** race freedom under all supported operation interleavings. Existing concurrency tests cover selected put/get/freeze and put/delete workloads; they do not cover close/rotation deadlock or run every test under the race detector.
- **Open design question:** how recovery should validate and reconcile manifest references, orphan tables, and WAL files after interrupted flush/compaction. The WAL boundary removes filename-ID inference, but does not validate that every manifest-referenced SSTable exists and decodes correctly.
- **Open design question:** what public read behavior should expose storage corruption, given `Get` currently returns only `(value, found)`.
- **Open design question:** which directory syncing guarantees and platform assumptions the persistence contract should make.
