// Copyright 2025 Nadrama Pty Ltd
// SPDX-License-Identifier: Apache-2.0

package localdb

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/nadrama-com/netsy/internal/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ReplicateRecord is used when receing a copy of the latest Record record from a leader,
// or when backfilling records. It differs significantly from the InsertRecord function,
// in that no validation is performed on the fields and there is no handling of revision
// incrementation - meaning you must be extremely careful when using this function.
func (db *database) ReplicateRecord(record *proto.Record) (*proto.Record, error) {
	// do not allow zero values for revision
	if record.Revision == 0 {
		return nil, fmt.Errorf("cannot insert record with revision=0")
	}

	// set replicated at
	record.ReplicatedAt = timestamppb.Now()

	// prepare data
	query := `INSERT INTO records (` +
		`revision, ` +
		`key, ` +
		`created, ` +
		`deleted, ` +
		`create_revision, ` +
		`prev_revision, ` +
		`version, ` +
		`lease, ` +
		`dek, ` +
		`value, ` +
		`created_at, ` +
		`compacted_at, ` +
		`leader_id, ` +
		`replicated_at ` +
		`) VALUES (` +
		`?1, ` + // revision
		`?2, ` + // key
		`?3, ` + // created
		`?4, ` + // deleted
		`?5, ` + // create_revision
		`?6, ` + // prev_revision
		`?7, ` + // version
		`?8, ` + // lease
		`?9, ` + // dek
		`?10, ` + // value
		`?11, ` + // created_at
		`NULL, ` + // compacted_at
		`?12, ` + // leader_id
		`?13 ` + // replicated_at
		`) RETURNING *`

	// insert record
	var createdAtStr string
	var replicatedAtStr interface{}
	if record.CreatedAt != nil {
		createdAtStr = record.CreatedAt.AsTime().Format(time.RFC3339Nano)
	}
	if record.ReplicatedAt != nil {
		replicatedAtStr = record.ReplicatedAt.AsTime().Format(time.RFC3339Nano)
	} else {
		replicatedAtStr = nil
	}

	// insert record and get returned values
	var returnedRecord proto.Record
	var returnedCreatedAtStr string
	var compactedAtStr, returnedReplicatedAtStr sql.NullString
	err := db.conn.QueryRow(
		query,
		record.Revision,       // 1
		record.Key,            // 2
		record.Created,        // 3
		record.Deleted,        // 4
		record.CreateRevision, // 5
		record.PrevRevision,   // 6
		record.Version,        // 7
		record.Lease,          // 8
		record.Dek,            // 9
		record.Value,          // 10
		createdAtStr,          // 11
		record.LeaderId,       // 12
		replicatedAtStr,       // 13
	).Scan(
		&returnedRecord.Revision,
		&returnedRecord.Key,
		&returnedRecord.Created,
		&returnedRecord.Deleted,
		&returnedRecord.CreateRevision,
		&returnedRecord.PrevRevision,
		&returnedRecord.Version,
		&returnedRecord.Lease,
		&returnedRecord.Dek,
		&returnedRecord.Value,
		&returnedCreatedAtStr,
		&compactedAtStr,
		&returnedRecord.LeaderId,
		&returnedReplicatedAtStr,
	)
	if err != nil {
		return nil, err
	}

	// check insert ID matches revision
	if returnedRecord.Revision != record.Revision {
		return nil, fmt.Errorf("Unexpected error: insert ID (%d) does not match revision (%d)", returnedRecord.Revision, record.Revision)
	}

	// Convert string timestamps back to protobuf timestamps
	if returnedCreatedAtStr != "" {
		if t, err := time.Parse(time.RFC3339Nano, returnedCreatedAtStr); err == nil {
			returnedRecord.CreatedAt = timestamppb.New(t)
		}
	}
	if compactedAtStr.Valid && compactedAtStr.String != "" {
		if t, err := time.Parse(time.RFC3339Nano, compactedAtStr.String); err == nil {
			returnedRecord.CompactedAt = timestamppb.New(t)
		}
	}
	if returnedReplicatedAtStr.Valid && returnedReplicatedAtStr.String != "" {
		if t, err := time.Parse(time.RFC3339Nano, returnedReplicatedAtStr.String); err == nil {
			returnedRecord.ReplicatedAt = timestamppb.New(t)
		}
	}

	return &returnedRecord, nil
}
