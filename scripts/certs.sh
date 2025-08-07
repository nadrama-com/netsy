#!/usr/bin/env bash
# Copyright 2025 Nadrama Pty Ltd
# SPDX-License-Identifier: Apache-2.0
set -eo pipefail

CURRENT=$(dirname "$(readlink -f "$0")")
CERTS_DIR="${CURRENT}/../certs"

# check if openssl command exists
command -v openssl >/dev/null 2>&1 || { echo >&2 "openssl is required but it's not installed.  Aborting."; exit 1; }

# check that no certs dir exists
if [ -d "${CERTS_DIR}" ]; then
    echo >&2 "certs directory already exists (${CERTS_DIR}). Aborting."
    exit 1
fi
mkdir -p "${CERTS_DIR}"

# generate certificates required to run netsy and kube-apiserver
# note that we are using Ed25519 keys
# generate Ed25519 keys for netsy and kube-apiserver
KEYS=(
    ca
    netsy.server
    netsy.client
    kube-apiserver.client
    kubectl.client
)
for item in "${KEYS[@]}"; do
    CMD="openssl genpkey -algorithm Ed25519 -out ${CERTS_DIR}/${item}.key"
    echo "${CMD}"
    ${CMD}
done

# generate a service account key - note that Ed25519 is not supported
openssl genrsa -out "${CERTS_DIR}/service-account.key" 2048

# Generate CA certificate
echo "Generating CA certificate..."
openssl req -new -x509 \
    -key "${CERTS_DIR}/ca.key" \
    -out "${CERTS_DIR}/ca.crt" \
    -days 3650 \
    -subj "/CN=Netsy CA"

# Generate netsy server certificate with SAN
echo "Generating netsy server certificate..."
openssl req -new \
    -key "${CERTS_DIR}/netsy.server.key" \
    -out "${CERTS_DIR}/netsy.server.csr" \
    -subj "/CN=netsy-server"

# Create config for SAN
cat > "${CERTS_DIR}/netsy.server.conf" << EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req

[req_distinguished_name]

[v3_req]
basicConstraints = CA:FALSE
keyUsage = nonRepudiation, digitalSignature, keyEncipherment
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = host.containers.internal
IP.1 = 127.0.0.1
EOF

openssl x509 -req \
    -in "${CERTS_DIR}/netsy.server.csr" \
    -CA "${CERTS_DIR}/ca.crt" \
    -CAkey "${CERTS_DIR}/ca.key" \
    -CAcreateserial \
    -out "${CERTS_DIR}/netsy.server.crt" \
    -days 365 \
    -not_before "$(date -u -v-1H '+%y%m%d%H%M%SZ')" \
    -extensions v3_req \
    -extfile "${CERTS_DIR}/netsy.server.conf"

# Generate netsy.client client certificate
echo "Generating netsy client certificate..."
openssl req -new \
    -key "${CERTS_DIR}/netsy.client.key" \
    -out "${CERTS_DIR}/netsy.client.csr" \
    -subj "/CN=netsy.client"

openssl x509 -req \
    -in "${CERTS_DIR}/netsy.client.csr" \
    -CA "${CERTS_DIR}/ca.crt" \
    -CAkey "${CERTS_DIR}/ca.key" \
    -CAcreateserial \
    -out "${CERTS_DIR}/netsy.client.crt" \
    -days 365 \
    -not_before "$(date -u -v-1H '+%y%m%d%H%M%SZ')"

# Generate kube-apiserver client certificate
echo "Generating kube-apiserver client certificate..."
openssl req -new \
    -key "${CERTS_DIR}/kube-apiserver.client.key" \
    -out "${CERTS_DIR}/kube-apiserver.client.csr" \
    -subj "/CN=kube-apiserver"

openssl x509 -req \
    -in "${CERTS_DIR}/kube-apiserver.client.csr" \
    -CA "${CERTS_DIR}/ca.crt" \
    -CAkey "${CERTS_DIR}/ca.key" \
    -CAcreateserial \
    -out "${CERTS_DIR}/kube-apiserver.client.crt" \
    -days 365 \
    -not_before "$(date -u -v-1H '+%y%m%d%H%M%SZ')"

# Generate kubectl client certificate with system:masters group
echo "Generating kubectl client certificate..."
openssl req -new \
    -key "${CERTS_DIR}/kubectl.client.key" \
    -out "${CERTS_DIR}/kubectl.client.csr" \
    -subj "/CN=kubectl-admin/O=system:masters"

openssl x509 -req \
    -in "${CERTS_DIR}/kubectl.client.csr" \
    -CA "${CERTS_DIR}/ca.crt" \
    -CAkey "${CERTS_DIR}/ca.key" \
    -CAcreateserial \
    -out "${CERTS_DIR}/kubectl.client.crt" \
    -days 365 \
    -not_before "$(date -u -v-1H '+%y%m%d%H%M%SZ')"

# Clean up CSR files and config
rm "${CERTS_DIR}/netsy.server.csr" "${CERTS_DIR}/kube-apiserver.client.csr" "${CERTS_DIR}/kubectl.client.csr" "${CERTS_DIR}/netsy.server.conf"

echo "Certificates generated successfully in ${CERTS_DIR}/"
