#!/usr/bin/env bash
# Copyright 2025 Nadrama Pty Ltd
# SPDX-License-Identifier: Apache-2.0
set -eo pipefail

CURRENT=$(dirname "$(readlink -f "$0")")

# check if docker command exists
command -v docker >/dev/null 2>&1 || { echo >&2 "docker is required but it's not installed.  Aborting."; exit 1; }

# Get latest kubernetes version
VERSION=$(curl -s https://api.github.com/repos/kubernetes/kubernetes/releases/latest | grep '"tag_name"' | cut -d'"' -f4)

# Run kube-apiserver container
CONTAINER_NAME=kube-apiserver
trap "docker stop $CONTAINER_NAME >/dev/null; docker rm $CONTAINER_NAME >/dev/null; exit" INT
docker run -d --name $CONTAINER_NAME \
  --entrypoint kube-apiserver \
  -v "${CURRENT}/../certs:/opt/netsy-certs:ro" \
  -p 8080:8080 \
  registry.k8s.io/kube-apiserver:$VERSION \
  --etcd-servers=https://host.containers.internal:2378 \
  --etcd-certfile=/opt/netsy-certs/kube-apiserver.client.crt \
  --etcd-keyfile=/opt/netsy-certs/kube-apiserver.client.key \
  --etcd-cafile=/opt/netsy-certs/ca.crt \
  --service-cluster-ip-range=10.0.0.0/24 \
  --service-account-issuer=https://kubernetes.default.svc \
  --service-account-signing-key-file=/opt/netsy-certs/service-account.key \
  --service-account-key-file=/opt/netsy-certs/service-account.key \
  --api-audiences=api \
  --authorization-mode=RBAC \
  --allow-privileged=true
echo "Running $CONTAINER_NAME (Press Ctrl+C to stop)"
docker logs -f $CONTAINER_NAME &
docker wait $CONTAINER_NAME >/dev/null
docker rm $CONTAINER_NAME >/dev/null
