#!/usr/bin/env bash
# Copyright 2025 Nadrama Pty Ltd
# SPDX-License-Identifier: Apache-2.0
set -eo pipefail

CURRENT=$(dirname "$(readlink -f "$0")")
CERTS_DIR="${CURRENT}/../certs"

USE_NETSY=1
ENDPOINT=127.0.0.1:2378
if [[ "$NETSY" = 0 ]] || [[ "$NETSY" = "false" ]]; then
    USE_NETSY=0
    ENDPOINT=127.0.0.1:2379
fi

CMD=(
    etcdctl --cert=${CERTS_DIR}/netsy.client.crt
            --key=${CERTS_DIR}/netsy.client.key
            --cacert=${CERTS_DIR}/ca.crt
            --endpoints=${ENDPOINT}
            -w json
    "$@"
)
if [ "$USE_NETSY" -eq 0 ]; then
    CMD=(
        etcdctl -w json "$@"
    )
fi

echo "${CMD[@]//$CURRENT\/..\//}"
${CMD[@]} | jq .
