// Copyright 2025 Nadrama Pty Ltd
// SPDX-License-Identifier: Apache-2.0

package clientapi

import (
	"context"
	"errors"

	"github.com/go-kit/log/level"
	"github.com/nadrama-com/netsy/internal/localdb"
	"github.com/nadrama-com/netsy/internal/proto"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

func (cs *ClientAPIServer) Txn(ctx context.Context, r *pb.TxnRequest) (resp *pb.TxnResponse, err error) {
	// Process transaction on leader
	inserted, resp, err := cs.peerServer.LeaderTxn(ctx, r)
	// If any type of error occurs, logs and then always return well-formed error response
	if err != nil {
		if errors.Is(err, localdb.ErrCompareRevisionFailed) ||
			errors.Is(err, localdb.ErrCreateKeyExists) ||
			errors.Is(err, localdb.ErrDeleteKeyNotFound) {
			if len(r.Failure) > 0 {
				level.Debug(cs.logger).Log("txnerror", err.Error())
			} else {
				level.Info(cs.logger).Log("txnerror", err.Error())
			}
		} else {
			cs.logger.Log("txnerror", err.Error())
		}
		// Best-effort latest revision retrieval
		// If this fails we still want to return a well formed error
		latestRevision, _ := cs.db.LatestRevision()
		resp = &pb.TxnResponse{
			Header: &pb.ResponseHeader{
				Revision: latestRevision,
			},
		}
	} else if inserted != nil && inserted.Created {
		level.Debug(cs.logger).Log("txncreated", string(inserted.Key))
	} else if inserted != nil && inserted.Deleted {
		level.Debug(cs.logger).Log("txndeleted", string(inserted.Key))
	} else if inserted != nil {
		level.Debug(cs.logger).Log("txnupdated", string(inserted.Key))
	}
	// Replicate to watchers
	var prevRecord *proto.Record
	if inserted != nil && !inserted.Created && inserted.PrevRevision > 0 {
		prevRecord, _ = cs.db.FindRecordByRev(inserted.PrevRevision)
	}
	if inserted != nil {
		cs.Distribute(inserted, prevRecord)
	}
	return resp, nil
}
