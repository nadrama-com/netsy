#!/bin/bash
# Copyright 2025 Nadrama Pty Ltd
# SPDX-License-Identifier: Apache-2.0

# Use air for live reload in dev
# @see https://github.com/air-verse/air

# CURRENT=$(dirname "$(readlink -f "$0")")

DEFAULT_ARGS=(
)

source .env

# Check instance ID is set
if [ -z "$INSTANCE_ID" ]; then
    echo "INSTANCE_ID env var is required"
    exit 1
fi

# Check go is installed
if ! command -v "go" &>/dev/null; then
    echo "Error: go command not found. Exiting..."
    exit 1
fi

# Check air binary is installed
AIR_BIN=$(go env GOPATH)/bin/air
if [ ! -f "$AIR_BIN" ]; then
    echo "air not found at $AIR_BIN"
    echo "installing air..."
    CMD="go install github.com/air-verse/air@latest"
    echo "$CMD"
    eval "$CMD"
fi

# Run air
echo "Running via Air with args:"
if [ $# -eq 0 ]; then
    echo "${DEFAULT_ARGS[@]}"
    exec "$AIR_BIN" -- "${DEFAULT_ARGS[@]}"
else
    echo "$@"
    exec "$AIR_BIN" -- "$@"
fi
