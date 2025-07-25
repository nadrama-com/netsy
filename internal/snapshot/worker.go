// Copyright 2025 Nadrama Pty Ltd
// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/nadrama-com/netsy/internal/config"
	"github.com/nadrama-com/netsy/internal/datafile"
	"github.com/nadrama-com/netsy/internal/localdb"
	"github.com/nadrama-com/netsy/internal/proto"
	"github.com/nadrama-com/netsy/internal/s3client"
)

// SnapshotRequest represents a request to potentially create a snapshot
type SnapshotRequest struct {
	Revision   int64
	Timestamp  time.Time
	RecordSize int64
}

// Worker handles snapshot creation in a separate goroutine
type Worker struct {
	logger    log.Logger
	config    *config.Config
	db        localdb.Database
	s3Client  *s3client.S3Client
	
	// Channel for receiving snapshot requests
	requestCh chan SnapshotRequest
	
	// Snapshot state tracking
	lastSnapshotRevision int64
	lastSnapshotTime     time.Time
	cumulativeSize       int64  // Cumulative size since last snapshot
	stateMutex          sync.Mutex
	
	// Prevents concurrent snapshot creation
	snapshotMutex sync.Mutex
	
	// Context for shutdown
	ctx    context.Context
	cancel context.CancelFunc
}

// NewWorker creates a new snapshot worker
func NewWorker(logger log.Logger, config *config.Config, db localdb.Database, s3Client *s3client.S3Client) *Worker {
	ctx, cancel := context.WithCancel(context.Background())
	
	return &Worker{
		logger:    logger,
		config:    config,
		db:        db,
		s3Client:  s3Client,
		requestCh: make(chan SnapshotRequest, 100), // Buffered channel to avoid blocking
		ctx:       ctx,
		cancel:    cancel,
	}
}

// Start begins the snapshot worker goroutine
func (w *Worker) Start() {
	go w.run()
}

// Stop gracefully shuts down the snapshot worker
func (w *Worker) Stop() {
	w.cancel()
}

// RequestSnapshot sends a snapshot request to the worker
func (w *Worker) RequestSnapshot(revision int64, timestamp time.Time, recordSize int64) {
	req := SnapshotRequest{
		Revision:   revision,
		Timestamp:  timestamp,
		RecordSize: recordSize,
	}
	
	select {
	case w.requestCh <- req:
		// Request sent successfully
	default:
		// Channel is full, log warning but don't block
		level.Warn(w.logger).Log("msg", "snapshot request channel full, dropping request", "revision", revision)
	}
}

// run is the main worker loop
func (w *Worker) run() {
	level.Info(w.logger).Log("msg", "snapshot worker started")
	
	for {
		select {
		case <-w.ctx.Done():
			level.Info(w.logger).Log("msg", "snapshot worker stopping")
			return
		case req := <-w.requestCh:
			w.processRequest(req)
		}
	}
}

// processRequest handles a single snapshot request
func (w *Worker) processRequest(req SnapshotRequest) {
	// Skip if S3 is not enabled
	if w.s3Client == nil {
		return
	}
	
	w.stateMutex.Lock()
	// Add this record's size to cumulative size
	w.cumulativeSize += req.RecordSize
	
	shouldCreate, reason := w.shouldCreateSnapshot(
		req.Revision,
		req.Timestamp,
		w.cumulativeSize,
		w.lastSnapshotRevision,
		w.lastSnapshotTime,
	)
	
	if shouldCreate {
		// Update state and reset cumulative size
		w.lastSnapshotRevision = req.Revision
		w.lastSnapshotTime = req.Timestamp
		w.cumulativeSize = 0  // Reset after snapshot
	}
	w.stateMutex.Unlock()
	
	if !shouldCreate {
		return
	}
	
	level.Info(w.logger).Log("msg", "snapshot thresholds met, creating snapshot",
		"current_revision", req.Revision, "reason", reason)
	
	w.createSnapshot(req.Revision)
}

// shouldCreateSnapshot determines if a snapshot should be created based on thresholds
// Returns (shouldCreate bool, reason string)
func (w *Worker) shouldCreateSnapshot(currentRevision int64, currentTime time.Time, cumulativeSize int64, lastRevision int64, lastTime time.Time) (bool, string) {
	// Prevent duplicate snapshots - only create if we have new records
	if currentRevision <= lastRevision {
		return false, ""
	}

	// Check record count threshold
	recordsThreshold := w.config.SnapshotThresholdRecords()
	if recordsThreshold > 0 && (currentRevision-lastRevision) >= recordsThreshold {
		level.Debug(w.logger).Log("msg", "snapshot record threshold reached",
			"current_revision", currentRevision, "last_snapshot_revision", lastRevision,
			"records_since_last", currentRevision-lastRevision, "threshold", recordsThreshold)
		return true, "record_count"
	}

	// Check age threshold
	ageThreshold := w.config.SnapshotThresholdAgeMinutes()
	if ageThreshold > 0 {
		if lastTime.IsZero() {
			// First snapshot - create immediately if age threshold is enabled
			level.Debug(w.logger).Log("msg", "first snapshot - age threshold enabled", "threshold_minutes", ageThreshold)
			return true, "first_snapshot"
		} else {
			timeSinceLastSnapshot := currentTime.Sub(lastTime)
			if timeSinceLastSnapshot >= time.Duration(ageThreshold)*time.Minute {
				level.Debug(w.logger).Log("msg", "snapshot age threshold reached",
					"time_since_last", timeSinceLastSnapshot, "threshold_minutes", ageThreshold)
				return true, "age"
			}
		}
	}

	// Check size threshold using cumulative size since last snapshot
	sizeThresholdMB := w.config.SnapshotThresholdSizeMB()
	if sizeThresholdMB > 0 {
		cumulativeSizeMB := cumulativeSize / (1024 * 1024)

		if cumulativeSizeMB >= sizeThresholdMB {
			level.Debug(w.logger).Log("msg", "snapshot size threshold reached",
				"cumulative_size_mb", cumulativeSizeMB, "threshold_mb", sizeThresholdMB)
			return true, "size"
		}
	}

	return false, ""
}

// createSnapshot creates and uploads a snapshot file containing all records up to the specified revision
func (w *Worker) createSnapshot(upToRevision int64) {
	// Acquire snapshot mutex to prevent concurrent snapshot creation
	w.snapshotMutex.Lock()
	defer w.snapshotMutex.Unlock()

	level.Info(w.logger).Log("msg", "starting snapshot creation", "up_to_revision", upToRevision)

	// Get all non-compacted records up to the specified revision
	records, err := w.db.FindAllRecordsForSnapshot(upToRevision)
	if err != nil {
		level.Error(w.logger).Log("msg", "failed to get records for snapshot", "error", err)
		return
	}

	if len(records) == 0 {
		level.Warn(w.logger).Log("msg", "no records found for snapshot", "up_to_revision", upToRevision)
		return
	}

	// Create temporary file for snapshot
	tempFile, err := os.CreateTemp(w.config.DataDir(), fmt.Sprintf("snapshot_%d_*.netsy", upToRevision))
	if err != nil {
		level.Error(w.logger).Log("msg", "failed to create temporary snapshot file", "error", err)
		return
	}
	tempFilePath := tempFile.Name()
	defer func() {
		tempFile.Close()
		if err := os.Remove(tempFilePath); err != nil && !os.IsNotExist(err) {
			level.Warn(w.logger).Log("msg", "failed to cleanup temporary snapshot file", "file", tempFilePath, "error", err)
		}
	}()

	// Write snapshot using datafile writer
	level.Debug(w.logger).Log("msg", "writing snapshot file", "temp_file", tempFilePath, "records_count", len(records))
	err = w.writeSnapshotFile(tempFile, records, upToRevision)
	if err != nil {
		level.Error(w.logger).Log("msg", "failed to write snapshot file", "temp_file", tempFilePath, "error", err)
		return
	}
	level.Debug(w.logger).Log("msg", "snapshot file written successfully", "temp_file", tempFilePath)

	// Close temp file before upload
	tempFile.Close()

	// Upload snapshot to S3 (UploadFile will add the prefix)
	snapshotKey := fmt.Sprintf("snapshots/%019d.netsy", upToRevision)

	level.Info(w.logger).Log("msg", "uploading snapshot to S3", "key", snapshotKey, "file_path", tempFilePath)

	err = w.s3Client.UploadFile(w.ctx, snapshotKey, tempFilePath)
	if err != nil {
		level.Error(w.logger).Log("msg", "failed to upload snapshot to S3", "key", snapshotKey, "file_path", tempFilePath, "error", err)
		return
	}

	level.Info(w.logger).Log("msg", "snapshot uploaded to S3 successfully", "revision", upToRevision, "records", len(records), "key", snapshotKey)

	// Start cleanup of old chunk files
	level.Info(w.logger).Log("msg", "starting chunk file cleanup", "up_to_revision", upToRevision)

	// List all chunk files that are covered by the snapshot (revision <= upToRevision)
	chunks, err := w.s3Client.ListChunksForCleanup(w.ctx, upToRevision)
	if err != nil {
		level.Error(w.logger).Log("msg", "failed to list chunks for cleanup", "error", err)
		return
	}
	deletedCount := 0
	for _, chunk := range chunks {
		err := w.s3Client.DeleteFile(w.ctx, chunk.Key)
		if err != nil {
			level.Warn(w.logger).Log("msg", "failed to delete chunk file", "key", chunk.Key, "error", err)
			continue
		}
		deletedCount++
		level.Debug(w.logger).Log("msg", "deleted chunk file", "key", chunk.Key, "revision", chunk.Revision)
	}

	level.Info(w.logger).Log("msg", "chunk file cleanup completed",
		"up_to_revision", upToRevision, "deleted_chunks", deletedCount)
}

// writeSnapshotFile writes records to a snapshot file using the datafile writer
func (w *Worker) writeSnapshotFile(file *os.File, records []*proto.Record, upToRevision int64) error {
	// Create buffered writer
	buffer := bufio.NewWriter(file)
	defer buffer.Flush()

	// Create datafile writer for snapshot
	writer, err := datafile.NewWriter(buffer, proto.FileKind_KIND_SNAPSHOT, int64(len(records)), w.config.InstanceID())
	if err != nil {
		return fmt.Errorf("failed to create datafile writer: %w", err)
	}

	// Write all records
	for _, record := range records {
		err = writer.Write(record)
		if err != nil {
			return fmt.Errorf("failed to write record %d to snapshot: %w", record.Revision, err)
		}
	}

	// Close writer
	err = writer.Close()
	if err != nil {
		return fmt.Errorf("failed to close datafile writer: %w", err)
	}

	return nil
}

// InitializeWithSnapshot initializes the snapshot worker state from existing snapshot info
func (w *Worker) InitializeWithSnapshot(snapshotInfo *s3client.LatestSnapshotInfo) {
	w.stateMutex.Lock()
	defer w.stateMutex.Unlock()
	
	if snapshotInfo == nil || !snapshotInfo.Found {
		// No existing snapshots, initialize with default state
		w.lastSnapshotRevision = 0
		w.lastSnapshotTime = time.Time{}
		w.cumulativeSize = 0
		
		level.Info(w.logger).Log("msg", "no existing snapshots found, initialized with default state")
		return
	}

	// Initialize from existing snapshot
	w.lastSnapshotRevision = snapshotInfo.Revision
	w.lastSnapshotTime = time.Now() // Use current time since we don't know exact creation time
	w.cumulativeSize = 0 // Start tracking from zero

	level.Info(w.logger).Log("msg", "initialized snapshot tracking from existing snapshot",
		"latest_snapshot_revision", snapshotInfo.Revision)
}
