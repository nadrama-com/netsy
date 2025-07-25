// Copyright 2025 Nadrama Pty Ltd
// SPDX-License-Identifier: Apache-2.0

package s3client

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/nadrama-com/netsy/internal/config"
)

// S3Client wraps AWS S3 operations for Netsy
type S3Client struct {
	client *s3.Client
	config *config.Config
	logger log.Logger
}

// FileInfo represents metadata about a file in S3 - used for list operations
type FileInfo struct {
	Key      string
	Size     int64
	Revision int64
}

// New creates a new S3Client with the provided configuration
func New(cfg *config.Config, logger log.Logger) (*S3Client, error) {
	if !cfg.S3Enabled() {
		return nil, fmt.Errorf("S3 is not enabled")
	}

	// Configure AWS SDK
	var awsCfg aws.Config
	var err error

	// Create config options
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.S3Region()),
	}

	// Add endpoint if specified (for MinIO, LocalStack, etc.)
	if cfg.S3Endpoint() != "" {
		opts = append(opts, awsconfig.WithEndpointResolverWithOptions(
			aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:               cfg.S3Endpoint(),
					HostnameImmutable: true,
				}, nil
			}),
		))
	}

	// Load base config first
	awsCfg, err = awsconfig.LoadDefaultConfig(context.TODO(), opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Configure credentials with STS AssumeRole preference
	if cfg.S3RoleArn() != "" {
		// Prefer STS AssumeRole over static credentials
		stsClient := sts.NewFromConfig(awsCfg)
		provider := stscreds.NewAssumeRoleProvider(stsClient, cfg.S3RoleArn(), func(o *stscreds.AssumeRoleOptions) {
			o.RoleSessionName = cfg.S3RoleSessionName()
		})
		awsCfg.Credentials = aws.NewCredentialsCache(provider)
		level.Info(logger).Log("msg", "Using STS AssumeRole for S3 access", "role", cfg.S3RoleArn())
	} else if cfg.S3AccessKeyID() != "" && cfg.S3SecretAccessKey() != "" {
		// Fall back to static credentials if no role ARN
		awsCfg.Credentials = credentials.NewStaticCredentialsProvider(
			cfg.S3AccessKeyID(),
			cfg.S3SecretAccessKey(),
			cfg.S3SessionToken(),
		)
		level.Info(logger).Log("msg", "Using static credentials for S3 access")
	} else {
		level.Info(logger).Log("msg", "Using default AWS credential chain for S3 access")
	}

	// Create S3 client with path-style addressing if needed
	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = cfg.S3ForcePathStyle()
	})

	level.Info(logger).Log("msg", "S3Client initialized", "bucket", cfg.S3BucketName(), "region", cfg.S3Region())

	return &S3Client{
		client: s3Client,
		config: cfg,
		logger: logger,
	}, nil
}

// Client returns the underlying S3 client for direct API access
func (s *S3Client) Client() *s3.Client {
	return s.client
}
