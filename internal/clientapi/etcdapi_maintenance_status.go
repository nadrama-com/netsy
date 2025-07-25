// Copyright 2025 Nadrama Pty Ltd
// SPDX-License-Identifier: Apache-2.0

package clientapi

import (
	"context"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (cs *ClientAPIServer) Status(ctx context.Context, r *pb.StatusRequest) (resp *pb.StatusResponse, err error) {
	dbSize, err := cs.db.Size()
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "error getting db size: %s", err)
	}
	return &pb.StatusResponse{
		Header:  &pb.ResponseHeader{},
		DbSize:  dbSize,
		Version: "3.5.16",
	}, nil
}
