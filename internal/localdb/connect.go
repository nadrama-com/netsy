// Copyright 2025 Nadrama Pty Ltd
// SPDX-License-Identifier: Apache-2.0

package localdb

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

func (db *database) Connect() error {
	if db.file == "" {
		return errors.New("db file path not configured")
	}

	// check directory exists
	dbDir := filepath.Dir(db.file)
	if _, err := os.Stat(dbDir); os.IsNotExist(err) {
		err := os.Mkdir(dbDir, 0750)
		if err != nil {
			return fmt.Errorf("error creating database directory %s: %s", dbDir, err)
		} else {
			return fmt.Errorf("created database directory %s", dbDir)
		}
	}

	// connect
	conn, err := sql.Open("sqlite3", db.file)
	if err != nil {
		return err
	}
	db.conn = conn

	// Enable WAL mode for better concurrency (allows reads during writes)
	_, err = conn.Exec("PRAGMA journal_mode=WAL")
	if err != nil {
		return fmt.Errorf("failed to enable WAL mode: %w", err)
	}

	// define schema
	migrations := []string{
		`CREATE TABLE IF NOT EXISTS records (
			revision integer PRIMARY KEY NOT NULL,
			key blob NOT NULL,
			created integer NOT NULL,
			deleted integer NOT NULL,
			create_revision integer NOT NULL,
			prev_revision integer NOT NULL,
			version integer NOT NULL,
			lease integer NOT NULL,
			dek integer NOT NULL,
			value blob,
			created_at text NOT NULL,
			compacted_at text,
			leader_id text NOT NULL,
			replicated_at text
		);`,
		`CREATE UNIQUE INDEX IF NOT EXISTS records_key_create_rev_prev_rev_uindex ON records (key, create_revision, prev_revision)`,
		`CREATE INDEX IF NOT EXISTS records_index_key ON records (key);`,
	}
	for _, sqlStmt := range migrations {
		_, err = db.conn.Exec(sqlStmt)
		if err != nil {
			log.Printf(
				"error running migration.\nmigration: %s\nerror: %s\n",
				sqlStmt,
				err,
			)
			return err
		}
	}

	return nil
}
