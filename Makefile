# Copyright 2025 Nadrama Pty Ltd
# SPDX-License-Identifier: Apache-2.0

BINDIR=bin

BINARY_NAME=netsy
MAIN_PKG=./cmd/netsy

BUILDVARS_PKG=github.com/nadrama-com/netsy/internal/buildvars

CURRENT := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# version format: YYYYMMDDhhmmss
BUILD_VERSION=$(shell date -u '+%Y%m%d%H%M%S')
BUILD_DATE=$(shell date -u '+%Y-%m-%dT%H:%M:%S')
COMMIT_HASH=$(shell git rev-parse --short HEAD)
COMMIT_DATE=$(shell git log -1 --format=%cd --date=format:'%Y-%m-%dT%H:%M:%S')
COMMIT_BRANCH=$(shell git rev-parse --abbrev-ref HEAD)

# Cross-compilation settings, defaulting OS/ARCH to the current platform
GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)
CGO_ENABLED=1
EXTRA_LD_FLAGS=
ifeq ($(GOOS),linux)
	BUILD_TAGS=linux
	EXTRA_LD_FLAGS=-extldflags -static
	ifeq ($(GOARCH),amd64)
		CC=x86_64-linux-musl-gcc
		CXX=x86_64-linux-musl-g++
	else ifeq ($(GOARCH),arm64)
		CC=aarch64-linux-musl-gcc
		CXX=aarch64-linux-musl-g++
	endif
else ifeq ($(GOOS),darwin)
	ifeq ($(GOARCH),amd64)
		BUILD_TAGS=darwin amd64
		CC=clang
		CXX=clang++
	else ifeq ($(GOARCH),arm64)
		BUILD_TAGS=darwin arm64
		CC=clang
		CXX=clang++
	endif
endif

.PHONY: test build proto clean run

test:
	go test -v ./...

build:
	mkdir -p $(BINDIR)
	GOOS=$(GOOS) GOARCH=$(GOARCH) \
	CGO_ENABLED=$(CGO_ENABLED) CC=$(CC) CXX=$(CXX) \
	go build $(if $(BUILD_TAGS),-tags "$(BUILD_TAGS)") \
	    -o $(BINDIR)/$(BINARY_NAME) \
		-trimpath \
		-ldflags "$(EXTRA_LD_FLAGS) \
		-X $(BUILDVARS_PKG).buildVersion=$(BUILD_VERSION) \
		-X $(BUILDVARS_PKG).buildDate=$(BUILD_DATE) \
		-X $(BUILDVARS_PKG).commitHash=$(COMMIT_HASH) \
		-X $(BUILDVARS_PKG).commitDate=$(COMMIT_DATE) \
		-X $(BUILDVARS_PKG).commitBranch=$(COMMIT_BRANCH) \
		" $(MAIN_PKG)
	printf "%s" "$(BUILD_VERSION)-$(COMMIT_HASH)" > $(BINDIR)/version.txt

proto:
	protoc -I=$(CURRENT) \
	       --go_out=$(CURRENT)internal \
	       --go_opt=paths=source_relative $(CURRENT)proto/*.proto

clean:
	rm -rf $(BINDIR)
