// Copyright 2025 Nadrama Pty Ltd
// SPDX-License-Identifier: Apache-2.0

package peerapi

import (
	"time"
)

// checkAndCreateSnapshot checks if a snapshot should be created based on configured thresholds
// and creates one asynchronously if needed. This should ideally only be called by the leader, since
// we ideally want to create snapshots from the latest data.
func (ps *PeerAPIServer) checkAndCreateSnapshot(currentRevision int64, recordSize int64) {
	// Skip if snapshot worker is not available
	if ps.snapshotWorker == nil {
		return
	}

	currentTime := time.Now()

	// Send snapshot request to worker (non-blocking)
	ps.snapshotWorker.RequestSnapshot(currentRevision, currentTime, recordSize)
}
