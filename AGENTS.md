# Repository Guide

## Project

This repository is an educational Go key-value store built around a log-structured merge tree. `cmd/lsm` is a command-line client and `cmd/lsm-server` serves a RESP2 interface. Storage coordination is in `internal/engine`; the supporting packages under `internal/` implement memtables, the WAL, SSTables, compaction, RESP2 parsing, and the TCP server.

The verified implementation and its current limitations are documented in [docs/architecture.md](docs/architecture.md). Consult it before changing persistence, recovery, locking, or compaction behavior; source code remains authoritative if documentation and code disagree.

## Build and test

- `go build ./...` builds packages and commands.
- `go test ./...` runs the repository test suite. Server tests open a local TCP listener; run them in an environment that permits loopback sockets.
- `go test ./internal/engine -run TestName` runs a focused engine test.
- `go vet ./...` runs Go static analysis.

Format changed Go files with `gofmt`. Tests use Go's standard `testing` package. Add or update tests for behavior changes, with particular care around persistence, recovery, compaction, protocol boundaries, and concurrency. Do not claim a check passed unless it completed successfully; record environmental restrictions separately from code failures.

## Coding expectations

Use standard Go formatting and idioms, concise package names, `PascalCase` for exported identifiers, and `camelCase` for unexported identifiers. Keep responsibilities aligned with existing `internal/` package boundaries. Keep generated database files and local runtime state out of source changes; check `.gitignore` before adding fixtures.

## Correctness constraints

- A successful write is appended to its WAL before it becomes visible in the memtable. Sequence numbers determine which version wins across storage layers.
- A tombstone suppresses older values and must remain represented until it is safe to discard. Empty values are valid values.
- A frozen memtable is immutable and must remain visible until its persisted replacement is visible to reads.
- Never remove a WAL or compaction input until its replacement data and metadata are safely installed. Verify every crash point, not only orderly restart.
- WAL and SSTable identifiers come from separate allocators. Recovery uses the manifest's minimum live WAL ID; never use an SSTable filename or numeric ID to decide whether a WAL is replayed.
- Review lock acquisition order whenever changing writes, rotation, reads, flush, compaction, or close. Existing behavior and known risks are described in the architecture document.

## Bug investigation and change validation

Trace the affected operation through `internal/engine` into the WAL, memtable, storage, SSTable, or server implementation. Treat comments and README claims as claims to verify against code. For persistence issues, inspect ordering of file writes, syncs, renames, metadata publication, deletion, and startup replay; consider process crashes and power loss separately. For concurrency issues, record all locks and the order in which each path acquires them.

Before proposing a fix, identify the violated invariant and add a focused regression test. Run the focused test and relevant package tests, then `go test ./...` when the environment allows it. Use race detection for concurrency changes when available. Report exact commands and outcomes, including tests blocked by environment restrictions. Do not change application behavior, dependencies, or generated data as part of documentation-only work.

## Change hygiene

Keep each change focused. Use short imperative commit subjects. Pull request descriptions should state behavior and impact and include test commands and results; include CLI or protocol examples when they clarify a user-visible change.
