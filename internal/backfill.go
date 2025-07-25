// Copyright 2025 Nadrama Pty Ltd
// SPDX-License-Identifier: Apache-2.0

package internal

import (
	"bufio"
	"context"
	"fmt"
	"os"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/nadrama-com/netsy/internal/config"
	"github.com/nadrama-com/netsy/internal/datafile"
	"github.com/nadrama-com/netsy/internal/localdb"
	pb "github.com/nadrama-com/netsy/internal/proto"
	"github.com/nadrama-com/netsy/internal/s3client"
)

// Backfill fetches the latest netsy data files from S3 and ensures they are
// inserted into the local database.
// If the latest revision = 0, it will first check for a snapshot and download that
// if it exists.
// After that, it will iterate on finding any chunks, and insert each of those.
func Backfill(logger log.Logger, db localdb.Database, cfg *config.Config, latestRevision int64, latestSnapshotInfo *s3client.LatestSnapshotInfo, s3Client *s3client.S3Client) error {
	// If S3 is not enabled, skip backfill
	if !cfg.S3Enabled() {
		level.Info(logger).Log("msg", "S3 not enabled, skipping backfill")
		return nil
	}

	ctx := context.Background()
	var err error

	// Track temporary files for cleanup
	var tempFiles []string
	defer func() {
		// Clean up temporary files
		for _, file := range tempFiles {
			if err := os.Remove(file); err != nil && !os.IsNotExist(err) {
				level.Warn(logger).Log("msg", "failed to clean up temporary file", "file", file, "error", err)
			} else {
				level.Debug(logger).Log("msg", "cleaned up temporary file", "file", file)
			}
		}
	}()

	// Step 1: If database is empty (latestRevision == 0), try to download latest snapshot
	if latestRevision == 0 && latestSnapshotInfo != nil && latestSnapshotInfo.Found {
		level.Info(logger).Log("msg", "database is empty, downloading latest snapshot", "key", latestSnapshotInfo.Key, "revision", latestSnapshotInfo.Revision)
		err = downloadAndImportSnapshotFile(ctx, logger, db, s3Client, cfg, latestSnapshotInfo, &tempFiles)
		if err != nil {
			return fmt.Errorf("failed to download snapshot: %w", err)
		}

		// Get updated latest revision after snapshot import
		latestRevision, err = db.LatestRevision()
		if err != nil {
			return fmt.Errorf("failed to get latest revision after snapshot: %w", err)
		}
		level.Info(logger).Log("msg", "updated latest revision after snapshot", "revision", latestRevision)
	}

	// Step 2: Find and download chunk files for revisions greater than latestRevision
	err = downloadAndImportChunks(ctx, logger, db, s3Client, cfg, latestRevision, &tempFiles)
	if err != nil {
		return fmt.Errorf("failed to download chunks: %w", err)
	}

	level.Info(logger).Log("msg", "backfill complete")
	return nil
}

func downloadAndImportSnapshotFile(ctx context.Context, logger log.Logger, db localdb.Database, s3Client *s3client.S3Client, cfg *config.Config, snapshotInfo *s3client.LatestSnapshotInfo, tempFiles *[]string) error {
	// Download and import the snapshot
	return downloadAndImportFile(ctx, logger, db, s3Client, cfg, snapshotInfo.Key, snapshotInfo.Size, pb.FileKind_KIND_SNAPSHOT, tempFiles)
}

func downloadAndImportSnapshot(ctx context.Context, logger log.Logger, db localdb.Database, s3Client *s3client.S3Client, cfg *config.Config, tempFiles *[]string) error {
	// List available snapshots
	snapshots, err := s3Client.ListSnapshots(ctx)
	if err != nil {
		return fmt.Errorf("failed to list snapshots: %w", err)
	}

	if len(snapshots) == 0 {
		level.Info(logger).Log("msg", "no snapshot found")
		return nil
	}

	// Get the latest snapshot (ListSnapshots returns them sorted newest first)
	latest := snapshots[0]
	level.Info(logger).Log("msg", "found latest snapshot", "key", latest.Key, "revision", latest.Revision, "size", latest.Size)

	// Download and import the snapshot
	return downloadAndImportFile(ctx, logger, db, s3Client, cfg, latest.Key, latest.Size, pb.FileKind_KIND_SNAPSHOT, tempFiles)
}

func downloadAndImportChunks(ctx context.Context, logger log.Logger, db localdb.Database, s3Client *s3client.S3Client, cfg *config.Config, fromRevision int64, tempFiles *[]string) error {
	// List available chunks greater than fromRevision
	chunks, err := s3Client.ListChunks(ctx, fromRevision)
	if err != nil {
		return fmt.Errorf("failed to list chunks: %w", err)
	}

	if len(chunks) == 0 {
		level.Info(logger).Log("msg", "no chunks found to backfill")
		return nil
	}

	level.Info(logger).Log("msg", "found chunks to backfill", "count", len(chunks))

	// Download and import each chunk file (ListChunks returns them sorted oldest first)
	for _, chunk := range chunks {
		err := downloadAndImportFile(ctx, logger, db, s3Client, cfg, chunk.Key, chunk.Size, pb.FileKind_KIND_CHUNK, tempFiles)
		if err != nil {
			return fmt.Errorf("failed to import chunk %s: %w", chunk.Key, err)
		}
	}

	return nil
}

// downloadAndImportFile downloads and imports a file, automatically choosing the best strategy
func downloadAndImportFile(ctx context.Context, logger log.Logger, db localdb.Database, s3Client *s3client.S3Client, cfg *config.Config, key string, size int64, expectedKind pb.FileKind, tempFiles *[]string) error {
	level.Debug(logger).Log("msg", "downloading and importing file", "key", key, "size", size)

	// Download the file using the appropriate strategy
	reader, err := s3Client.DownloadFile(ctx, key, size, cfg.DataDir(), tempFiles)
	if err != nil {
		return fmt.Errorf("failed to download file: %w", err)
	}
	defer reader.Close()

	// Create buffered reader for the datafile reader
	buffer := bufio.NewReader(reader)

	return importFromReader(logger, db, buffer, expectedKind, key)
}

// importFromReader handles the common logic for importing records from a reader
func importFromReader(logger log.Logger, db localdb.Database, buffer *bufio.Reader, expectedKind pb.FileKind, key string) error {
	// Create datafile reader
	reader, err := datafile.NewReader(buffer, &expectedKind)
	if err != nil {
		return fmt.Errorf("failed to create datafile reader: %w", err)
	}

	// Read and import all records
	recordCount := int64(0)
	for i := int64(0); i < reader.Count(); i++ {
		record, err := reader.Read()
		if err != nil {
			return fmt.Errorf("failed to read record %d: %w", i, err)
		}

		// Import record using replicate function (no validation)
		_, err = db.ReplicateRecord(record)
		if err != nil {
			return fmt.Errorf("failed to replicate record %d: %w", i, err)
		}

		recordCount++
	}

	// Close reader and verify
	results, err := reader.Close()
	if err != nil {
		return fmt.Errorf("failed to close reader: %w", err)
	}

	level.Info(logger).Log("msg", "successfully imported file", "key", key, "kind", results.Kind, "records", recordCount, "first_revision", results.FirstRevision, "last_revision", results.LastRevision)
	return nil
}
