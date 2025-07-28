// Copyright 2025 Nadrama Pty Ltd
// SPDX-License-Identifier: Apache-2.0

package localdb

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/nadrama-com/netsy/internal/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (db *database) selectRecord(queryEnd string, latestPerKey bool, excludeDeleted bool, args ...any) (records []*proto.Record, err error) {
	query := "SELECT " +
		"revision, " +
		"key, " +
		"created, " +
		"deleted, " +
		"create_revision, " +
		"prev_revision, " +
		"version, " +
		"lease, " +
		"dek, " +
		"value, " +
		"created_at, " +
		"compacted_at, " +
		"leader_id, " +
		"replicated_at " +
		" FROM (SELECT " +
		"records.*," +
		"ROW_NUMBER() OVER (" +
		"PARTITION BY key ORDER BY revision DESC" +
		") as rn " +
		"FROM records " + queryEnd + ")"
	if latestPerKey || excludeDeleted {
		query += " WHERE"
	}
	if latestPerKey {
		query += " rn = 1"
	}
	if excludeDeleted {
		if latestPerKey {
			query += " AND"
		}
		query += " deleted = 0"
	}
	rows, err := db.conn.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var row proto.Record
		var createdAtStr string
		var compactedAtStr, replicatedAtStr sql.NullString
		err := rows.Scan(
			&row.Revision,
			&row.Key,
			&row.Created,
			&row.Deleted,
			&row.CreateRevision,
			&row.PrevRevision,
			&row.Version,
			&row.Lease,
			&row.Dek,
			&row.Value,
			&createdAtStr,
			&compactedAtStr,
			&row.LeaderId,
			&replicatedAtStr,
		)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, err
		}

		// Convert string timestamps to protobuf timestamps
		if createdAtStr != "" {
			if t, err := time.Parse(time.RFC3339Nano, createdAtStr); err == nil {
				row.CreatedAt = timestamppb.New(t)
			}
		}
		if compactedAtStr.Valid && compactedAtStr.String != "" {
			if t, err := time.Parse(time.RFC3339Nano, compactedAtStr.String); err == nil {
				row.CompactedAt = timestamppb.New(t)
			}
		}
		if replicatedAtStr.Valid && replicatedAtStr.String != "" {
			if t, err := time.Parse(time.RFC3339Nano, replicatedAtStr.String); err == nil {
				row.ReplicatedAt = timestamppb.New(t)
			}
		}

		records = append(records, &row)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func (db *database) FindRecordsBy(whereQuery string, whereArgs []any, revision int64, limit int64, order string) ([]*proto.Record, int64, int64, error) {
	if order != "ASC" && order != "DESC" {
		return nil, 0, 0, fmt.Errorf("invalid order: %s", order)
	}

	// Build WHERE clause
	whereClause := fmt.Sprintf("WHERE (%s)", whereQuery)
	if revision > 0 {
		whereClause += " AND revision <= ?"
		whereArgs = append(whereArgs, revision)
	}

	// Build ORDER BY clause
	orderClause := fmt.Sprintf("ORDER BY key %s, revision DESC", order)

	// Build LIMIT clause
	limitClause := ""
	if limit > 0 {
		limitClause = fmt.Sprintf("LIMIT %d", limit)
	}

	// Single query with CTE to get both count and records
	query := fmt.Sprintf(`
		WITH filtered AS (
			SELECT
				revision, key, created, deleted, create_revision, prev_revision, version, lease, dek, value, created_at, compacted_at, leader_id, replicated_at,
				ROW_NUMBER() OVER (PARTITION BY key ORDER BY revision DESC) as rn
			FROM records
			%s
		)
		SELECT
			(SELECT MAX(revision) FROM records) as max_revision,
			(SELECT COUNT(*) FROM filtered WHERE rn = 1 AND deleted = 0) as records_count,
			revision, key, created, deleted, create_revision, prev_revision, version, lease, dek, value, created_at, compacted_at, leader_id, replicated_at
		FROM filtered
		WHERE rn = 1 AND deleted = 0
		%s
		%s`, whereClause, orderClause, limitClause)
	rows, err := db.conn.Query(query, whereArgs...)
	if err != nil {
		return nil, 0, 0, err
	}
	defer rows.Close()

	// Parse query results
	var records []*proto.Record
	var maxRevision int64
	var totalCount int64
	for rows.Next() {
		var record proto.Record
		var createdAtStr string
		var compactedAtStr, replicatedAtStr sql.NullString

		err := rows.Scan(
			&maxRevision, // max_revision from CTE
			&totalCount,  // records_count from CTE
			&record.Revision,
			&record.Key,
			&record.Created,
			&record.Deleted,
			&record.CreateRevision,
			&record.PrevRevision,
			&record.Version,
			&record.Lease,
			&record.Dek,
			&record.Value,
			&createdAtStr,
			&compactedAtStr,
			&record.LeaderId,
			&replicatedAtStr,
		)
		if err != nil {
			return nil, 0, 0, err
		}

		// Convert string timestamps to protobuf timestamps
		if createdAtStr != "" {
			if t, err := time.Parse(time.RFC3339Nano, createdAtStr); err == nil {
				record.CreatedAt = timestamppb.New(t)
			}
		}
		if compactedAtStr.Valid && compactedAtStr.String != "" {
			if t, err := time.Parse(time.RFC3339Nano, compactedAtStr.String); err == nil {
				record.CompactedAt = timestamppb.New(t)
			}
		}
		if replicatedAtStr.Valid && replicatedAtStr.String != "" {
			if t, err := time.Parse(time.RFC3339Nano, replicatedAtStr.String); err == nil {
				record.ReplicatedAt = timestamppb.New(t)
			}
		}

		records = append(records, &record)
	}
	if err = rows.Err(); err != nil {
		return nil, 0, 0, err
	}

	return records, totalCount, maxRevision, nil
}

// FindAllRecordsForSnapshot returns all non-compacted records up to the specified revision,
// including deleted records (needed for proper snapshot creation)
func (db *database) FindAllRecordsForSnapshot(upToRevision int64) ([]*proto.Record, error) {
	queryEnd := "WHERE revision <= ? AND compacted_at IS NULL ORDER BY revision ASC"
	var records []*proto.Record
	var err error
	// latestPerKey=false, excludeDeleted=false - we want all non-compacted records including deleted ones
	records, err = db.selectRecord(queryEnd, false, false, upToRevision)
	if err != nil {
		return nil, err
	}
	return records, nil
}

func (db *database) FindRecordByRev(rev int64) (record *proto.Record, err error) {
	query := "SELECT " +
		"revision, " +
		"key, " +
		"created, " +
		"deleted, " +
		"create_revision, " +
		"prev_revision, " +
		"version, " +
		"lease, " +
		"dek, " +
		"value, " +
		"created_at, " +
		"compacted_at, " +
		"leader_id, " +
		"replicated_at " +
		"FROM records WHERE revision = ?"
	rows, err := db.conn.Query(query, rev)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var row proto.Record
	var createdAtStr string
	var compactedAtStr, replicatedAtStr sql.NullString
	err = rows.Scan(
		&row.Revision,
		&row.Key,
		&row.Created,
		&row.Deleted,
		&row.CreateRevision,
		&row.PrevRevision,
		&row.Version,
		&row.Lease,
		&row.Dek,
		&row.Value,
		&createdAtStr,
		&compactedAtStr,
		&row.LeaderId,
		&replicatedAtStr,
	)
	if err != nil {
		return nil, err
	}
	// Convert string timestamps to protobuf timestamps
	if createdAtStr != "" {
		if t, err := time.Parse(time.RFC3339Nano, createdAtStr); err == nil {
			row.CreatedAt = timestamppb.New(t)
		}
	}
	if compactedAtStr.Valid && compactedAtStr.String != "" {
		if t, err := time.Parse(time.RFC3339Nano, compactedAtStr.String); err == nil {
			row.CompactedAt = timestamppb.New(t)
		}
	}
	if replicatedAtStr.Valid && replicatedAtStr.String != "" {
		if t, err := time.Parse(time.RFC3339Nano, replicatedAtStr.String); err == nil {
			row.ReplicatedAt = timestamppb.New(t)
		}
	}
	return record, nil
}
