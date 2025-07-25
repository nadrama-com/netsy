// Copyright 2025 Nadrama Pty Ltd
// SPDX-License-Identifier: Apache-2.0

package clientapi

import (
	"context"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

func (cs *ClientAPIServer) MemberList(ctx context.Context, r *pb.MemberListRequest) (resp *pb.MemberListResponse, err error) {
	return &pb.MemberListResponse{
		Header: &pb.ResponseHeader{},
		Members: []*pb.Member{
			{
				Name:       "netsy",
				ClientURLs: []string{cs.config.ListenClientsAddr()},
				PeerURLs:   []string{cs.config.ListenClientsAddr()},
			},
		},
	}, nil
}
