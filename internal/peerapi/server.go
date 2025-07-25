// Copyright 2025 Nadrama Pty Ltd
// SPDX-License-Identifier: Apache-2.0

package peerapi

import (
	"sync"
	"sync/atomic"

	"github.com/go-kit/log"

	"github.com/nadrama-com/netsy/internal/config"
	"github.com/nadrama-com/netsy/internal/localdb"
	"github.com/nadrama-com/netsy/internal/s3client"
	"github.com/nadrama-com/netsy/internal/snapshot"
)

type PeerAPIServer struct {
	logger         log.Logger
	config         *config.Config
	db             localdb.Database
	s3Client       *s3client.S3Client
	snapshotWorker *snapshot.Worker

	// leaderTxnMutex serializes all transaction processing on the leader node
	// This mutex should ONLY be used by the leader, not by follower nodes
	leaderTxnMutex sync.Mutex

	// nextRevisionID holds the next revision ID to assign
	// Managed atomically to ensure thread-safe access
	nextRevisionID atomic.Int64
}

func NewServer(logger log.Logger, conf *config.Config, db localdb.Database, snapshotWorker *snapshot.Worker, s3Client *s3client.S3Client) (*PeerAPIServer, error) {
	ps := &PeerAPIServer{
		logger:         logger,
		config:         conf,
		db:             db,
		s3Client:       s3Client,
		snapshotWorker: snapshotWorker,
	}

	// Initialize the next revision ID from database
	err := ps.initializeRevisionCounter()
	if err != nil {
		return nil, err
	}

	return ps, nil
}

// initializeRevisionCounter sets the next revision ID based on the highest
// revision currently in the database. This should only be called on leader startup.
func (ps *PeerAPIServer) initializeRevisionCounter() error {
	latestRevision, err := ps.db.LatestRevision()
	if err != nil {
		return err
	}
	ps.nextRevisionID.Store(latestRevision + 1)
	return nil
}


