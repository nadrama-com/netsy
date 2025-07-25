// Copyright 2025 Nadrama Pty Ltd
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/nadrama-com/netsy/internal"
	"github.com/nadrama-com/netsy/internal/buildvars"
	"github.com/nadrama-com/netsy/internal/clientapi"
	"github.com/nadrama-com/netsy/internal/config"
	"github.com/nadrama-com/netsy/internal/localdb"
	"github.com/nadrama-com/netsy/internal/s3client"
	"github.com/nadrama-com/netsy/internal/snapshot"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.etcd.io/etcd/server/v3/embed"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

var rootCmd = &cobra.Command{
	Use:   "netsy",
	Short: "Netsy",
	Long:  `netsy is an etcd alternative which implements a subset of the etcd API for use with for Kubernetes.`,
}

func init() {
	pflags := rootCmd.PersistentFlags()
	pflags.BoolP("verbose", "v", false, "Enable verbose output")
	pflags.Bool("version", false, "Show version information")
	pflags.Lookup("verbose").NoOptDefVal = "true"
	pflags.VisitAll(func(flag *pflag.Flag) {
		viper.BindPFlag(flag.Name, flag)
	})
}

func NewRootCmd() *cobra.Command {
	// Create logger
	var logger log.Logger
	{
		logger = log.NewLogfmtLogger(os.Stderr)
		logger = log.With(logger, "ts", log.DefaultTimestampUTC)
		logger = log.With(logger, "caller", log.DefaultCaller)
	}

	// Initialize config
	c, err := config.Init(logger)
	if err != nil {
		fmt.Println("Error initializing config:", err)
		os.Exit(1)
	}

	// Apply log level filtering based on verbose setting
	if !c.Verbose() {
		logger = level.NewFilter(logger, level.AllowInfo())
	}

	// Define root command
	rootCmd.Run = func(cmd *cobra.Command, args []string) {
		var err error

		// check for version flag
		if viper.GetBool("version") {
			fmt.Printf("nstance %s\n", buildvars.BuildVersion())
			if c.Verbose() {
				fmt.Printf("build version: %s\n", buildvars.BuildVersion())
				fmt.Printf("build date: %s\n", buildvars.BuildDate())
				fmt.Printf("commit hash: %s\n", buildvars.CommitHash())
				fmt.Printf("commit date: %s\n", buildvars.CommitDate())
				fmt.Printf("commit branch: %s\n", buildvars.CommitBranch())
			}
			return
		}

		// validate config
		err = c.Validate()
		if err != nil {
			fmt.Printf("Invalid config/environment variables: %v\n", err)
			os.Exit(1)
		}

		// log modes
		if c.Verbose() {
			fmt.Println("Verbose ouput ENABLED")
		}

		// load certs and keys
		tlsFiles, err := config.LoadTLSFiles(c)
		if err != nil {
			logger.Log("msg", "Failed to load TLS files", "err", err)
			jitterWaitThenExit(logger)
		}

		// define TLS configuration for gRPC server
		tlsConfig := tls.Config{
			MinVersion:   tls.VersionTLS13,
			MaxVersion:   tls.VersionTLS13,
			CipherSuites: []uint16{tls.TLS_AES_256_GCM_SHA384},
			RootCAs:      tlsFiles.ServerCA,
			Certificates: []tls.Certificate{
				*tlsFiles.ServerCert,
			},
			ClientAuth: tls.RequireAndVerifyClientCert,
			ClientCAs:  tlsFiles.ClientCA,
		}

		// configure signal handling for shutdown
		shutdownErrsCh := make(chan error)
		go func() {
			// if a signal is received, push it on to the c channel
			c := make(chan os.Signal, 1)
			signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
			defer signal.Stop(c)
			// block until a signal is received, then push it on to the shutdownErrsCh channel
			shutdownErrsCh <- fmt.Errorf("%s", <-c)
		}()

		// instantiate database
		db := localdb.New(fmt.Sprintf("%s/db.sqlite3", c.DataDir()))
		err = db.Connect()
		if err != nil {
			logger.Log("msg", "db.Connect error: %s", "error", err)
			jitterWaitThenExit(logger)
		}

		// backfill and verify database
		latestRevision, err := db.LatestRevision()
		if err != nil {
			logger.Log("msg", "db.LatestRevision error", "error", err)
			jitterWaitThenExit(logger)
		}

		// Create S3 client and get latest snapshot info
		var snapshotWorker *snapshot.Worker
		var latestSnapshotInfo *s3client.LatestSnapshotInfo
		var s3Client *s3client.S3Client
		if c.S3Enabled() {
			s3Client, err = s3client.New(c, logger)
			if err != nil {
				logger.Log("msg", "Failed to create S3 client", "error", err)
				os.Exit(1)
			}

			// Get latest snapshot info once
			latestSnapshotInfo, err = s3Client.GetLatestSnapshot(context.Background())
			if err != nil {
				logger.Log("msg", "Failed to get latest snapshot info", "error", err)
				os.Exit(1)
			}

			snapshotWorker = snapshot.NewWorker(logger, c, db, s3Client)
			snapshotWorker.InitializeWithSnapshot(latestSnapshotInfo)

			// Ensure snapshot worker is stopped on shutdown
			defer func() {
				level.Info(logger).Log("msg", "shutting down snapshot worker")
				snapshotWorker.Stop()
			}()
		}

		err = internal.Backfill(logger, db, c, latestRevision, latestSnapshotInfo, s3Client)
		if err != nil {
			logger.Log("msg", "clientServer.Backfill error", "error", err)
			jitterWaitThenExit(logger)
		}
		err = db.VerifyIntegrity()
		if err != nil {
			logger.Log("msg", "clientServer.db.VerifyIntegrity error", "error", err)
			jitterWaitThenExit(logger)
		}

		// Start snapshot worker after backfill is complete
		if snapshotWorker != nil {
			snapshotWorker.Start()
		}

		// setup and run gRPC server with (etcd-compatible) client API
		gopts := []grpc.ServerOption{
			grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
				MinTime:             embed.DefaultGRPCKeepAliveMinTime,
				PermitWithoutStream: false,
			}),
			grpc.KeepaliveParams(keepalive.ServerParameters{
				Time:    embed.DefaultGRPCKeepAliveInterval,
				Timeout: embed.DefaultGRPCKeepAliveTimeout,
			}),
		}
		gopts = append(gopts, grpc.Creds(credentials.NewTLS(&tlsConfig)))
		grpcServer := grpc.NewServer(gopts...)
		clienApiServer, err := clientapi.NewServer(logger, c, db, grpcServer, snapshotWorker, s3Client)
		if err != nil {
			logger.Log("msg", "Unable to create server client", "err", err)
			os.Exit(1)
		}
		grpcListener, err := net.Listen("tcp", c.ListenClientsAddr())
		if err != nil {
			logger.Log("msg", "Unable to create gRPC server listener", "err", err)
			os.Exit(1)
		}
		logger.Log("msg", "starting client (grpc) server...", "addr", c.ListenClientsAddr())
		go func() {
			shutdownErrsCh <- grpcServer.Serve(grpcListener)
		}()

		// block until a shutdown error is received (err or signal)
		err = <-shutdownErrsCh
		logger.Log("msg", "shutting down...")

		// cleanup and exit
		clienApiServer.Close()
		logger.Log("msg", "exiting")
	}

	return rootCmd
}

func jitterWaitThenExit(logger log.Logger) {
	// generate a random amount of time to wait before exiting
	// to introduce jitter / so we don't constantly retry
	waitFor := time.Duration(rand.Intn(10)) * time.Second
	logger.Log("msg", "waiting before exiting", "wait", waitFor)
	time.Sleep(waitFor)
	logger.Log("msg", "exiting...")
	os.Exit(1)
	return
}
