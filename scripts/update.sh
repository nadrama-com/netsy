#!/usr/bin/env bash
# Copyright 2025 Nadrama Pty Ltd
# SPDX-License-Identifier: Apache-2.0
set -eo pipefail

CURRENT=$(dirname "$(readlink -f "$0")")

KEY="${1:-examplekey}"
REV="${2:-1}"
VALUE="${3:-exampleUPDATEDvalue}"

# note: with etcdctl, we must have two newlines between each of:
# - compare
# - success requests (get, put, delete)
# - failure requests (range, only on update/delete)
TXN="mod(\"${KEY}\") = \"${REV}\"

put \"${KEY}\" \"${VALUE}\"

get \"${KEY}\"
"

echo -n "echo '${TXN}' | "
echo "${TXN}" | ${CURRENT}/etcdctl.sh txn
