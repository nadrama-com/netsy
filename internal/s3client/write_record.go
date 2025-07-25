// Copyright 2025 Nadrama Pty Ltd
// SPDX-License-Identifier: Apache-2.0

package s3client

import (
	"bufio"
	"bytes"
	"context"
	"fmt"

	"github.com/go-kit/log/level"
	"github.com/nadrama-com/netsy/internal/datafile"
	pb "github.com/nadrama-com/netsy/internal/proto"
)

// WriteRecord writes a single record to S3 as a chunk file
func (s *S3Client) WriteRecord(ctx context.Context, record *pb.Record) error {
	// Create a buffer to write the chunk file data
	buffer := &bytes.Buffer{}
	bufWriter := bufio.NewWriter(buffer)

	// Create datafile writer for a single record chunk
	// Use the instance ID from config as the leader ID
	leaderID := s.config.InstanceID()
	writer, err := datafile.NewWriter(bufWriter, pb.FileKind_KIND_CHUNK, 1, leaderID)
	if err != nil {
		return fmt.Errorf("failed to create datafile writer: %w", err)
	}

	// Write the record
	err = writer.Write(record)
	if err != nil {
		return fmt.Errorf("failed to write record: %w", err)
	}

	// Close/flush writer
	err = writer.Close()
	if err != nil {
		return fmt.Errorf("failed to close datafile writer: %w", err)
	}

	// Generate S3 key for the chunk file
	// Format: chunks/{partition}/{zero-padded-revision}.netsy
	// Partition is modulo 10000 to avoid hot paths
	// Revision is zero-padded to 19 characters (max int64)
	partition := record.Revision % 10000
	key := fmt.Sprintf("chunks/%04d/%019d.netsy", partition, record.Revision)

	// Upload to S3 with retry-once logic
	err = s.WriteChunkFile(ctx, key, bytes.NewReader(buffer.Bytes()))
	if err != nil {
		level.Debug(s.logger).Log("msg", "first S3 upload attempt failed, retrying once", "error", err, "key", key)
		// Retry once on failure
		err = s.WriteChunkFile(ctx, key, bytes.NewReader(buffer.Bytes()))
		if err != nil {
			return fmt.Errorf("S3 upload failed after retry: %w", err)
		}
		level.Info(s.logger).Log("msg", "S3 upload succeeded on retry", "key", key)
	}

	level.Debug(s.logger).Log("msg", "record written to S3", "revision", record.Revision, "key", key)
	return nil
}
