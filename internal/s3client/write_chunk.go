// Copyright 2025 Nadrama Pty Ltd
// SPDX-License-Identifier: Apache-2.0

package s3client

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/go-kit/log/level"
)

// WriteChunkFile writes a chunk file to S3
func (s *S3Client) WriteChunkFile(ctx context.Context, key string, data io.Reader) error {
	// Read data into memory buffer to get content length
	buf := &bytes.Buffer{}
	_, err := io.Copy(buf, data)
	if err != nil {
		return fmt.Errorf("failed to read data: %w", err)
	}

	// Prepare S3 key with prefix
	s3Key := key
	if s.config.S3KeyPrefix() != "" {
		s3Key = s.config.S3KeyPrefix() + "/" + key
	}

	// Prepare put object input with conditional write to prevent overwrite
	bucketName := s.config.S3BucketName()
	storageClass := s.config.S3StorageClass()
	input := &s3.PutObjectInput{
		Bucket:           &bucketName,
		Key:              &s3Key,
		Body:             bytes.NewReader(buf.Bytes()),
		IfNoneMatch:      aws.String("*"), // Fail if object already exists
		StorageClass:     types.StorageClass(storageClass),
	}

	// Set server-side encryption
	if s.config.S3Encryption() == "aws:kms" {
		input.ServerSideEncryption = types.ServerSideEncryptionAwsKms
		if s.config.S3KMSKeyID() != "" {
			kmsKeyID := s.config.S3KMSKeyID()
			input.SSEKMSKeyId = &kmsKeyID
		}
	} else if s.config.S3Encryption() == "AES256" {
		input.ServerSideEncryption = types.ServerSideEncryptionAes256
	}

	// Upload to S3
	_, err = s.client.PutObject(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to upload to S3: %w", err)
	}

	level.Debug(s.logger).Log("msg", "chunk file uploaded to S3", "key", s3Key, "bucket", s.config.S3BucketName())
	return nil
}
