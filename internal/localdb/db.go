// Copyright 2025 Nadrama Pty Ltd
// SPDX-License-Identifier: Apache-2.0

package localdb

import (
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
	"github.com/nadrama-com/netsy/internal/proto"
)

type database struct {
	file string
	conn *sql.DB
}

type Database interface {
	Connect() error
	LatestRevision() (int64, error)
	GetRevision(findRevision int64) (revision int64, compacted bool, compactedAt sql.NullString, err error)
	VerifyIntegrity() error
	FindRecordsBy(whereQuery string, whereArgs []any, revision int64, limit int64, order string) ([]*proto.Record, int64, int64, error)
	FindRecordByRev(revision int64) (*proto.Record, error)
	FindAllRecordsForSnapshot(upToRevision int64) ([]*proto.Record, error)
	InsertRecord(record *proto.Record, tx *Tx) (*proto.Record, error)
	BeginTx() (*Tx, error)
	ReplicateRecord(record *proto.Record) (*proto.Record, error)
	Size() (int64, error)
	Close() error
}

func New(file string) *database {
	return &database{
		file: file,
	}
}

func (db *database) LatestRevision() (int64, error) {
	query := "SELECT revision FROM records ORDER BY revision DESC LIMIT 1"
	var revision int64
	row := db.conn.QueryRow(query)
	if err := row.Scan(&revision); err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}
	return revision, nil
}

func (db *database) GetRevision(findRevision int64) (revision int64, compacted bool, compactedAt sql.NullString, err error) {
	query := "SELECT revision,compacted_at FROM records WHERE revision = ? ORDER BY revision DESC LIMIT 1"
	row := db.conn.QueryRow(query, findRevision)
	if err = row.Scan(&revision, &compactedAt); err != nil {
		return
	}
	if compactedAt.Valid {
		compacted = true
	}
	return
}

// VerifyIntegrity checks that the latest revision is the same as the total
// number of records in the records table. Essentially - ensuring that no records
// are missing. We can do this because our form of compaction is not to delete
// records, but rather to empty their values.
func (db *database) VerifyIntegrity() error {
	query := "SELECT " +
		"COUNT(*) as total," +
		"COALESCE(MAX(revision), 0) as latest " +
		"FROM records"
	row := db.conn.QueryRow(query)
	var total, latest int64
	if err := row.Scan(&total, &latest); err != nil {
		return err
	}
	if total != latest {
		return fmt.Errorf("integrity error: total records (%d) does not match latest revision (%d)", total, latest)
	}
	return nil
}

func (db *database) Size() (int64, error) {
	query := "SELECT (page_count * page_size) AS db_size FROM pragma_page_count(), pragma_page_size();"
	var dbSize int64
	row := db.conn.QueryRow(query)
	if err := row.Scan(&dbSize); err != nil {
		return 0, err
	}
	return dbSize, nil
}

func (db *database) Close() error {
	if db.conn == nil {
		return nil
	}
	return db.conn.Close()
}
