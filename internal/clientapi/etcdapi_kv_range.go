// Copyright 2025 Nadrama Pty Ltd
// SPDX-License-Identifier: Apache-2.0

package clientapi

import (
	"context"

	"github.com/nadrama-com/netsy/internal/commonapi"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

func (cs *ClientAPIServer) Range(ctx context.Context, r *pb.RangeRequest) (*pb.RangeResponse, error) {
	return commonapi.Range(cs.db, ctx, r)
}
