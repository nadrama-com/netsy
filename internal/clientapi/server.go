// Copyright 2025 Nadrama Pty Ltd
// SPDX-License-Identifier: Apache-2.0

package clientapi

import (
	"fmt"

	"github.com/go-kit/log"
	"github.com/nadrama-com/netsy/internal/config"
	"github.com/nadrama-com/netsy/internal/localdb"
	"github.com/nadrama-com/netsy/internal/peerapi"
	"github.com/nadrama-com/netsy/internal/s3client"
	"github.com/nadrama-com/netsy/internal/snapshot"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

// ClientAPIServer implements a gRPC server compatible with the Kubernetes etcd API subset
// @see https://github.com/etcd-io/etcd/blob/main/api/etcdserverpb/rpc.proto#L37
// @see https://github.com/etcd-io/etcd/blob/main/api/etcdserverpb/rpc.pb.go
// etcd has the following gRPC "services":
// * KV
// * Watch
// * Lease
// * Cluster
// * Maintenance
// * Auth
// we include the 'Unimplemented' services by default and override them where required
type ClientAPIServer struct {
	logger     log.Logger
	config     *config.Config
	db         localdb.Database
	grpcServer *grpc.Server
	// note: in future we will replace this with a peer server gRPC client
	peerServer *peerapi.PeerAPIServer
	// note: sending messages not currently required
	//wsSendCh     chan []byte
	pb.UnimplementedKVServer
	pb.UnimplementedWatchServer
	pb.UnimplementedLeaseServer
	pb.UnimplementedClusterServer
	pb.UnimplementedMaintenanceServer
	pb.UnimplementedAuthServer
}

func NewServer(logger log.Logger, conf *config.Config, db localdb.Database, grpcServer *grpc.Server, snapshotWorker *snapshot.Worker, s3Client *s3client.S3Client) (*ClientAPIServer, error) {
	var err error

	// TODO: in future we will replace this with a peer server gRPC client
	// when the Netsy server is not the leader
	peerServer, err := peerapi.NewServer(logger, conf, db, snapshotWorker, s3Client)
	if err != nil {
		return nil, fmt.Errorf("peerapi.NewServer error: %s", err)
	}

	clientServer := &ClientAPIServer{
		logger:     logger,
		config:     conf,
		grpcServer: grpcServer,
		db:         db,
		// TODO: in future we will replace this with a peer server gRPC client
		// when the Netsy server is not the leader
		peerServer: peerServer,
	}

	pb.RegisterKVServer(grpcServer, clientServer)
	pb.RegisterWatchServer(grpcServer, clientServer)
	pb.RegisterLeaseServer(grpcServer, clientServer)
	pb.RegisterClusterServer(grpcServer, clientServer)
	pb.RegisterMaintenanceServer(grpcServer, clientServer)
	pb.RegisterAuthServer(grpcServer, clientServer)
	hsrv := health.NewServer()
	hsrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, hsrv)
	reflection.Register(grpcServer)

	return clientServer, nil
}

func (clientServer *ClientAPIServer) Close() {
	clientServer.grpcServer.GracefulStop()
	clientServer.db.Close()
}
