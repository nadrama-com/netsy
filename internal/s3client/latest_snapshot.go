// Copyright 2025 Nadrama Pty Ltd
// SPDX-License-Identifier: Apache-2.0

package s3client

import (
	"context"

	"github.com/go-kit/log/level"
)

// LatestSnapshotInfo contains information about the latest snapshot
type LatestSnapshotInfo struct {
	Revision int64
	Key      string
	Size     int64
	Found    bool
}

// GetLatestSnapshot returns information about the latest snapshot file, or Found=false if none exists
func (s *S3Client) GetLatestSnapshot(ctx context.Context) (*LatestSnapshotInfo, error) {
	snapshots, err := s.ListSnapshots(ctx)
	if err != nil {
		return nil, err
	}

	if len(snapshots) == 0 {
		level.Debug(s.logger).Log("msg", "no snapshots found")
		return &LatestSnapshotInfo{Found: false}, nil
	}

	// Get the latest snapshot (ListSnapshots returns them sorted newest first)
	latest := snapshots[0]
	level.Debug(s.logger).Log("msg", "found latest snapshot", "key", latest.Key, "revision", latest.Revision, "size", latest.Size)

	return &LatestSnapshotInfo{
		Revision: latest.Revision,
		Key:      latest.Key,
		Size:     latest.Size,
		Found:    true,
	}, nil
}
