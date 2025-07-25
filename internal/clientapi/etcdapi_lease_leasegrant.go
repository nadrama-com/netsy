// Copyright 2025 Nadrama Pty Ltd
// SPDX-License-Identifier: Apache-2.0

package clientapi

import (
	"context"
	"fmt"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (cs *ClientAPIServer) LeaseGrant(ctx context.Context, r *pb.LeaseGrantRequest) (resp *pb.LeaseGrantResponse, err error) {
	// TODO

	cs.logger.Log("msg", "lease grant", "TODO", "implement LeaseGrant", "req", fmt.Sprintf("%+v", r))
	return &pb.LeaseGrantResponse{
		Header: &pb.ResponseHeader{},
		ID:     r.TTL,
		TTL:    r.TTL,
	}, nil
}

func (cs *ClientAPIServer) LeaseRevoke(ctx context.Context, r *pb.LeaseRevokeRequest) (resp *pb.LeaseRevokeResponse, err error) {
	cs.logger.Log("TODO", "implement LeaseRevoke")
	return nil, status.Errorf(codes.Unimplemented, "method LeaseRevoke not implemented")
}

func (cs *ClientAPIServer) LeaseKeepAlive(ka pb.Lease_LeaseKeepAliveServer) error {
	cs.logger.Log("TODO", "implement LeaseKeepAlive")
	return fmt.Errorf("method LeaseKeepAlive not implemented")
}

func (cs *ClientAPIServer) LeaseTimeToLive(ctx context.Context, r *pb.LeaseTimeToLiveRequest) (resp *pb.LeaseTimeToLiveResponse, err error) {
	cs.logger.Log("TODO", "implement LeaseTimeToLive")
	return nil, status.Errorf(codes.Unimplemented, "method LeaseTimeToLive not implemented")
}

func (cs *ClientAPIServer) LeaseLeases(ctx context.Context, r *pb.LeaseLeasesRequest) (resp *pb.LeaseLeasesResponse, err error) {
	cs.logger.Log("TODO", "implement LeaseLeases")
	return nil, status.Errorf(codes.Unimplemented, "method LeaseLeases not implemented")
}
