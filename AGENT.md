# Netsy - Agent Development Guide

## Commands
- **Protobuf**: `make proto` - generates Go files from proto files in `./proto`
- **Build**: `make build` - builds netsy binary with version info
- **Dev**: `INSTANCE_ID=test ./dev.sh` - live reload with Air (requires INSTANCE_ID env var)
- **Test**: `go test ./...` - run all tests
- **Test package**: `go test ./internal/peerapi/` - run specific package tests
- **Clean**: `make clean` - remove bin/ directory
- **Format**: `gofmt -w .` - format code
- **Localstack S3**: `docker compose` anything for working with the Localstack S3 container
- **Kubernetes API Server**: `./scripts/kube-apiserver.sh` but requires a running netsy instance, and Ctrl-C to exit.

## Architecture
One Component:
1. **Netsy** (cmd/netsy) - etcd alternative compatible with Kubernetes etcd clients

Key packages:
- `internal/clientapi/` - API surface for clients such as `kube-apiserver` and `etcdctl`
- `internal/commonapi/` - code shared by `clientapi` and `peerapi`
- `internal/config/` - Netsy server configuration
- `internal/datafile/` - Netsy file format writing/reading
- `internal/localdb/` - SQLite local DB operations
- `internal/peerapi/` - API surface for Peer Netsy servers
- `internal/proto` - built Go files from proto files in `./proto`
- `internal/s3client` - AWS S3 client helpers

## Code Style
- **File headers**: Copyright 2025 Nadrama Pty Ltd + Apache-2.0 license
- **Imports**: stdlib → third-party → local (github.com/nadrama-com/netsy/*)
- **Naming**: PascalCase types/methods, camelCase variables, lowercase packages
- **Errors**: Named returns `(result Type, err error)`, early returns, `fmt.Errorf()` wrapping
- **Comments**: Function name first, describe purpose, TODO for improvements
