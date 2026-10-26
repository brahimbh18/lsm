# Repository Guidelines

## Project Structure

This repository is a Go implementation of an LSM storage engine. Executable entry points are in `cmd/lsm` (CLI) and `cmd/lsm-server` (RESP2 server). Core packages live under `internal/`: engine coordination, memtable, WAL, protocol handling, storage and SSTables, compaction, and server logic. Tests are kept beside the package they exercise as `*_test.go`. `bench-sizes.sh` is a repository-level benchmark helper; `data/` is runtime data, not source code.

## Build, Test, and Development

- `go build ./...` builds all packages and commands.
- `go test ./...` runs the full unit and integration test suite.
- `go test ./internal/engine -run TestName` runs a focused engine test (replace `TestName` with the test to run).
- `go run ./cmd/lsm` starts the CLI; `go run ./cmd/lsm-server` starts the RESP2 server. Check `README.md` for supported options and usage.
- `gofmt -w <files>` formats changed Go files; run `go vet ./...` for static checks.

## Coding Style and Naming

Use standard Go formatting and idioms: tabs as produced by `gofmt`, concise package names, exported identifiers in `PascalCase`, and unexported identifiers in `camelCase`. Keep package responsibilities aligned with the existing `internal/` boundaries. Name tests `Test<Behavior>` and keep them in the package's existing test file or a clearly named `*_test.go` file.

## Testing Guidelines

Add or update tests with behavior changes, especially for persistence, recovery, compaction, and protocol edge cases. Run `go test ./...` before submitting changes. Tests use Go's standard `testing` package; there is no separate coverage threshold configured in the repository.

## Commits and Pull Requests

Recent history uses short, imperative subject lines (for example, “Read values from persisted SSTables” and “Add dedicated compaction worker”). Follow that style and keep each commit focused. Pull requests should explain the behavior change and its impact, link related issues when applicable, and include test commands and results. Include CLI or protocol examples when they clarify a user-visible change.

## Data and Configuration

Keep generated database files and local runtime state out of commits. Review `.gitignore` before adding fixtures or data files, and avoid committing secrets or machine-specific configuration.
