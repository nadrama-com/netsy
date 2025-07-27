# Netsy

Netsy is an [etcd](https://etcd.io/) alternative for Kubernetes which stores data in S3.

Unlike etcd which uses the Raft consensus algorithm, the design of Netsy is inspired by PostgreSQL synchronous streaming replication, and by modern architectures of systems like Loki/Mimir and OpenObserve which use S3 (or S3-compatible) object storage for data persistence.

Netsy was created by [Nadrama](https://nadrama.com). Nadrama helps you deploy containers, in your cloud account, in minutes. Nadrama uses Netsy in production for its Kubernetes clusters!

## Project Status

We have released this repository as a developer preview and welcome your feedback!

__Netsy is currently single-node only and not all features outlined in this README are necessarily implemented. Multi-node support is in development - you can read/give feedback on the [spec here](https://docs.google.com/document/d/1wNdsiVe35uSLWfnvq6fZ5ftmUThVQMfj6zChKfjxN-g/edit?usp=sharing).__

Nadrama is committed to Open Source - read more [here](https://nadrama.com/opensource).

## Why create an etcd alternative?

We want to make infrastructure easy.

Running servers that store persistent state can be challenging to get right.

S3 (and S3-compatible) object storage is the de facto solution for simple, reliable, durable storage.

There has been a recent trend in new system designs which leverage S3 as the primary data store.
For example, instead of ELK for logs and Prometheus for metrics, you can use [Loki](https://github.com/grafana/loki)+[Mimir](https://github.com/grafana/mimir) or [OpenObserve](https://github.com/openobserve/openobserve).

By relying on S3 instead of local filesystems, operators are able to treat VMs "like cattle, not pets".
That is to say, VMs become easily replaceable - no longer something to individually manage and maintain.

VM deployments can be as simple as an Auto-Scaling Group (ASG) with some userdata.
Fixing issues is as simple as deleting one VM, and waiting for a new one to come online.

And with ASGs, scaling up and down is greatly simplified - enabling operators to reduce costs.
In fact, most of these systems can often scale down to just a single VM - also great for non-production environments.

When we looked at the options for how to approach managing Kubernetes and etcd in production, the challenges were:

1. etcd requires 3 nodes (VMs) for fault tolerance.

2. etcd stores data to disk, requiring careful management of persistent volumes.

3. snapshots of etcd are asynchronous, so if a single node cluster VM shutdown even milliseconds after,
   you may lose data unless you correctly manage the persistent volumes.

[Kine](https://github.com/k3s-io/kine/) is an Open Source Kubernetes-compatible etcd alternative.
It's a great project which enables you to use SQLite or an external SQL databases such as MySQL or PostgreSQL.
However, kine is often not recommended for production environment for reasons such as:

1. All reads and writes go to a single database endpoint.

2. Watches are implemented by polling the database.

3. It does not implement etcd leases.

We considered using it with tools like [Litestream](https://github.com/benbjohnson/litestream) to stream
the WAL to S3. However, this would limit reads and writes to a single kine instance.
Additionally, stream WAL files are asynchronous and difficult to guarentee completion prior to system shutdown.

What we wanted is something we knew would ensure data was safely stored in S3 even if a VM is shutdown and deleted.

And so, we created Netsy.

## Goals

Netsy was created to reduce the operational complexity and compute requirements
traditionally associated with running etcd for Kubernetes clusters.

It MUST maintain compatibility with the subset of the etcd API used by Kubernetes.

It is a non-goal to fully support the entire etcd API.

## Usage

Build:

```
make build
```

You can look at the [.env](./.env) file for configuration examples.

### AWS IAM Policy

Example policy:

```
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "NetsyS3ObjectOperations",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListObjectsV2",
        "s3:HeadObject",
        "s3:GetObjectAttributes",
        "s3:CreateMultipartUpload",
        "s3:UploadPart",
        "s3:CompleteMultipartUpload",
        "s3:AbortMultipartUpload",
        "s3:ListMultipartUploads",
        "s3:ListParts"
      ],
      "Resource": ["arn:aws:s3:::your-netsy-bucket/*"]
    },
    {
      "Sid": "NetsyS3BucketOperations",
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": ["arn:aws:s3:::your-netsy-bucket"]
    },
    {
      "Sid": "NetsyKMSAccess",
      "Effect": "Allow",
      "Action": [
        "kms:Encrypt",
        "kms:Decrypt",
        "kms:ReEncrypt*",
        "kms:GenerateDataKey*",
        "kms:DescribeKey"
      ],
      "Resource": "arn:aws:kms:your-region:your-account:key/your-kms-key-id",
      "Condition": {
        "StringEquals": {
          "kms:ViaService": "s3.your-region.amazonaws.com"
        }
      }
    },
    {
      "Sid": "NetsyAssumeRoleForSTS",
      "Effect": "Allow",
      "Action": ["sts:AssumeRole"],
      "Resource": "arn:aws:iam::your-account:role/your-netsy-role"
    }
  ]
}
```

## Design

Netsy is designed to run alongside every `kube-apiserver` instance in a cluster.

Currently, Netsy synchronously replicates data to S3. Asynchronous replication is planned.

### Netsy Data Files

A `.netsy` data file is a varint size-delimited Protocol Buffer messages file with optional body+footer compression, consisting of:

* Header: 1x Header message (always uncompressed)
* Body: 1+ Record message(s) (compressed or uncompressed based on header)
* Footer: 1x Footer message (compressed or uncompressed based on header)

There are two kinds of Netsy data files:
* Snapshot files - containing a complete snapshot of KV records (always compressed when created by Netsy).
* Chunk files - containing a set of new records not yet captured in a Snapshot (compressed when created by Netsy only if >4KB of key+value data for all records combined).

The compression type is specified in the header's `compression` field (`COMPRESSION_NONE` or `COMPRESSION_ZSTD`). External systems can create uncompressed snapshot files for easier implementation.

The file is written using the [google.golang.org/protobuf/encoding/protodelim](https://google.golang.org/protobuf/encoding/protodelim) package.

#### CRCs

We use CRC64 to protect against accidental corruption like bit rot, network errors, S3 silent failures.
It does not defend against malicious tampering of S3 objects

CRC64 advantages:

   * Detects all single-bit errors
   * Detects burst errors up to 64 bits
   * Collision probability: 1 in 2^64 for random data
   * Much faster chunk upload/validation
   * 8 bytes vs 32 bytes (smaller files)

We use CRC's in 4 places:
  * Header struct contents CRC
  * Record struct contents CRC
  * Footer struct contents CRC
  * All-Records field in the Footer struct (a CRC of all Record structs in the File)

Per-Record CRC protects against:
   * Corrupted Record content
   * Individual Record bit rot

All-Records CRC protects against:
   * Missing records: All individual CRCs valid, but records 500-600 disappeared
   * Duplicated records: Record 123 appears twice (both have valid CRCs)
   * Reordered records: All CRCs valid but revision sequence scrambled
   * Truncated files: Partial S3 download, missing last 1000 records
   * Header/footer corruption: Per-record CRCs don't protect metadata

## Development

Start a localstack s3 server:

```
docker compose up -d
```

Generate some certificates:

```
./scripts/certs.sh
```

Start a netsy dev server:

```
./dev.sh
```

(or with database reset first):

```
rm -f temp/data/db.sqlite3*; ./dev.sh
```

If you need to reset S3 bucket contents:

```
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test aws --endpoint-url="http://localhost:4566" s3 rm s3://netsy-dev --recursive
```

And if you want to test with a kube-apiserver container:

```
./scripts/kube-apiserver.sh
```

You can also run `etcdctl` with a helper script (which wires up the correct certs and endpoint):

```
./scripts/etcdctl.sh
```

## License

Netsy is licensed under the Apache License, Version 2.0.
Copyright 2025 Nadrama Pty Ltd.
See the [LICENSE](./LICENSE) file for details.
