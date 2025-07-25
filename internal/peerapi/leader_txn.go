// Copyright 2025 Nadrama Pty Ltd
// SPDX-License-Identifier: Apache-2.0

package peerapi

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/go-kit/log/level"
	"github.com/nadrama-com/netsy/internal/commonapi"
	"github.com/nadrama-com/netsy/internal/localdb"
	"github.com/nadrama-com/netsy/internal/proto"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	googlepb "google.golang.org/protobuf/proto"
)

var ErrUnsupported = errors.New("Unsupported request - netsy only implementes the Kubernetes etcd API subet")

// LeaderTxn is our backend for the etcd transaction API, responsible for committing changes.
//
// It receives a pb.TxnRequest:
// https://pkg.go.dev/go.etcd.io/etcd/api/v3/etcdserverpb#TxnRequest
//
// It returns a pb.TxnResponse:
// https://pkg.go.dev/go.etcd.io/etcd/api/v3/etcdserverpb#TxnResponse
//
// The Kubernetes etcd client only uses a subset of the etcd transaction API.
//
// In the Kubernetes codebase, a storage interface implementation translates calls to a Kubernetes etcd client (in the etcd repository):
// * Create (->OptimisticPut): https://github.com/kubernetes/kubernetes/blob/master/staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go#L259
// * GuaranteedUpdate (->OptimisticPut): https://github.com/kubernetes/kubernetes/blob/master/staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go#L448C17-L448C33
// * Delete ->conditionalDelete(->OptimisticDelete): https://github.com/kubernetes/kubernetes/blob/master/staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go#L327
// The etcd repository contains the Kubernetes etcd client:
// * OptimisticPut: https://github.com/kubernetes/kubernetes/blob/master/vendor/go.etcd.io/etcd/client/v3/kubernetes/client.go#L83
// * OptimisticDelete: https://github.com/etcd-io/etcd/blob/main/client/v3/kubernetes/client.go#L109
//
// To summarise all Kubernetes etcd transaction request combinations:
//  1. compare, which checks if the mod_revision of the field:
//     -> for create requests: =0. meaning, there's no record or the key was deleted.
//     -> for update requests: =prev revision. meaning, it must match kubernetes known version.
//     -> for delete requests: =prev revision. meaning, it must match kubernetes known version.
//  2. 1x success, executed if compare succeeds.
//     -> create
//     -> update
//     -> delete
//  3. 0 or 1 failure, executed if compare fails:
//     -> create: can have no failure conditions, or range for existing key, returning single/first result.
//     -> update: range for existing key, returning single/first result.
//     -> delete: range for existing key, returning single/first result.
//
// Essentially the compare and failure condition for update and delete are the same, just success differs.
// Note that create and update can have a lease ID specified, which gets recorded in the success operation.
func (ps *PeerAPIServer) LeaderTxn(ctx context.Context, r *pb.TxnRequest) (record *proto.Record, parsed *pb.TxnResponse, err error) {
	var rangeResp *pb.RangeResponse
	var inserted *proto.Record
	// Serialize all leader transaction processing
	ps.leaderTxnMutex.Lock()
	defer ps.leaderTxnMutex.Unlock()
	// Validate and parse request
	record, err = ParseTxnRequest(r)
	if errors.Is(err, ErrUnsupported) {
		return nil, nil, fmt.Errorf("%w - request: %+v", err, r)
	} else if err != nil {
		return nil, nil, fmt.Errorf("error parsing request: %w", err)
	}
	// Use the instance ID from config as the leader ID
	record.LeaderId = ps.config.InstanceID()
	// Assign the next revision ID
	record.Revision = ps.nextRevisionID.Load()
	// Start transaction for S3 synchronous mode or use auto-commit
	if ps.s3Client != nil && ps.config.ReplicationMode() == "synchronous" {
		// Use transaction for synchronous S3 replication
		tx, err := ps.db.BeginTx()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to begin transaction: %w", err)
		}
		// Insert record within transaction
		inserted, err = ps.db.InsertRecord(record, tx)
		if err != nil &&
			errors.Is(err, localdb.ErrCompareRevisionFailed) &&
			len(r.Failure) == 1 {
			tx.Rollback()
			// Range on compare failure
			level.Debug(ps.logger).Log("msg", "record insert error - executing failure op (range)", "error", err)
			err = nil
			rangeResp, err = commonapi.Range(ps.db, ctx, &pb.RangeRequest{
				Key: []byte(record.Key),
			})
			if rangeResp == nil {
				return nil, nil, fmt.Errorf("error getting range response: %w", err)
			}
			// Don't upload to S3 on compare failure, just handle the range response
		} else if err != nil {
			tx.Rollback()
			return nil, nil, fmt.Errorf("error for %s: %w", record.Key, err)
		} else {
			// Upload to S3 within transaction boundary only on successful insert
			err = ps.s3Client.WriteRecord(ctx, inserted)
			if err != nil {
				tx.Rollback()
				return nil, nil, fmt.Errorf("S3 upload failed: %w", err)
			}
			// Commit transaction
			err = tx.Commit()
			if err != nil {
				return nil, nil, fmt.Errorf("failed to commit transaction: %w", err)
			}
			// Increment revision counter only after successful commit
			ps.nextRevisionID.Add(1)
			// Calculate record size for snapshot tracking
			recordSize := int64(googlepb.Size(inserted))
			// Check if snapshot should be created
			ps.checkAndCreateSnapshot(inserted.Revision, recordSize)
		}
	} else {
		// Just insert directly if S3 is not enabled
		inserted, err = ps.db.InsertRecord(record, nil)
		if err != nil &&
			errors.Is(err, localdb.ErrCompareRevisionFailed) &&
			len(r.Failure) == 1 {
			// Range on compare failure
			level.Debug(ps.logger).Log("msg", "record insert error - executing failure op (range)", "error", err)
			err = nil
			rangeResp, err = commonapi.Range(ps.db, ctx, &pb.RangeRequest{
				Key: []byte(record.Key),
			})
			if rangeResp == nil {
				return nil, nil, fmt.Errorf("error getting range response: %w", err)
			}
		} else if inserted != nil {
			// Increment revision counter only after successful insert
			ps.nextRevisionID.Add(1)
			// Calculate record size for snapshot tracking
			recordSize := int64(googlepb.Size(inserted))
			// Check if snapshot should be created
			ps.checkAndCreateSnapshot(inserted.Revision, recordSize)
		}
		if err != nil {
			return nil, nil, fmt.Errorf("error for %s: %w", record.Key, err)
		}
	}
	resp, err := BuildTxnResponse(inserted, rangeResp)
	if err != nil {
		return nil, nil, fmt.Errorf("error building response: %w", err)
	}
	return inserted, resp, nil
}

// ParseTxnRequest validates a pb.TxnRequest and creates a proto.Record
func ParseTxnRequest(r *pb.TxnRequest) (*proto.Record, error) {
	// Validate request
	if len(r.Compare) != 1 ||
		len(r.Success) != 1 ||
		(len(r.Failure) != 0 && len(r.Failure) != 1) ||
		r.Compare[0].Target != pb.Compare_MOD ||
		r.Compare[0].Result != pb.Compare_EQUAL {
		return nil, fmt.Errorf("invalid request - missing required fields")
	}
	compareKey := r.Compare[0].GetKey()
	compareModRevision := r.Compare[0].GetModRevision()
	successPut := r.Success[0].GetRequestPut()
	if successPut != nil && successPut.PrevKv {
		return nil, fmt.Errorf("invalid request - prevKv not supported for success put operations")
	}
	successDelete := r.Success[0].GetRequestDeleteRange()
	if successDelete != nil && successDelete.PrevKv {
		return nil, fmt.Errorf("invalid request - prevKv not supported for success delete operations")
	}
	if (successPut != nil && !bytes.Equal(compareKey, successPut.Key)) ||
		(successDelete != nil && !bytes.Equal(compareKey, successDelete.Key)) {
		return nil, fmt.Errorf("invalid request - key mismatch between compare and success operations")
	}
	var failureRange *pb.RangeRequest = nil
	if len(r.Failure) == 1 {
		failureRange = r.Failure[0].GetRequestRange()
		if failureRange == nil {
			return nil, fmt.Errorf("invalid request - failure operation must contain a range request")
		}
		if failureRange.RangeEnd != nil {
			return nil, fmt.Errorf("invalid request - rangeEnd not supported for failure range operations")
		}
		if !bytes.Equal(compareKey, failureRange.Key) {
			return nil, fmt.Errorf("invalid request - key mismatch between compare and failure operations")
		}
	}
	// check if create, update, or delete
	var record *proto.Record
	if compareModRevision == 0 && successPut != nil && successDelete == nil {
		// create
		record = &proto.Record{
			Key:     successPut.Key,
			Value:   successPut.Value,
			Lease:   successPut.Lease,
			Created: true, // true=created
			Deleted: false,
		}
	} else if compareModRevision > 0 && successPut != nil && successDelete == nil && failureRange != nil {
		// update
		record = &proto.Record{
			Key:          successPut.Key,
			Value:        successPut.Value,
			Lease:        successPut.Lease,
			Created:      false, // false=updated
			Deleted:      false,
			PrevRevision: compareModRevision,
		}
	} else if compareModRevision > 0 && successPut == nil && successDelete != nil && failureRange != nil {
		// delete
		record = &proto.Record{
			Key:          successDelete.Key,
			Value:        nil,
			Created:      false,
			Deleted:      true, // true=deleted
			PrevRevision: compareModRevision,
		}
	} else {
		// unknown
		return nil, ErrUnsupported
	}
	return record, nil
}

// BuildTxnResponse converts a proto.Record or pb.RangeResponse to a pb.TxnResponse
func BuildTxnResponse(record *proto.Record, rangeResp *pb.RangeResponse) (*pb.TxnResponse, error) {
	response := &pb.TxnResponse{
		Header: &pb.ResponseHeader{},
	}

	if rangeResp != nil {
		// Failed Comparison - return Failure operation ResponseRange
		response.Header.Revision = rangeResp.Header.Revision
		response.Succeeded = false
		response.Responses = []*pb.ResponseOp{
			{
				Response: &pb.ResponseOp_ResponseRange{
					ResponseRange: rangeResp,
				},
			},
		}
	} else if record != nil && record.Deleted {
		// Delete operation - return DeleteRangeResponse
		response.Header.Revision = record.Revision
		response.Succeeded = true
		response.Responses = []*pb.ResponseOp{
			{
				Response: &pb.ResponseOp_ResponseDeleteRange{
					ResponseDeleteRange: &pb.DeleteRangeResponse{
						Header:  &pb.ResponseHeader{Revision: record.Revision},
						Deleted: 1,
					},
				},
			},
		}
	} else if record != nil {
		// Create or Update operation - return PutResponse
		response.Header.Revision = record.Revision
		response.Succeeded = true
		response.Responses = []*pb.ResponseOp{
			{
				Response: &pb.ResponseOp_ResponsePut{
					ResponsePut: &pb.PutResponse{
						Header: &pb.ResponseHeader{Revision: record.Revision},
					},
				},
			},
		}
	}
	return response, nil
}
