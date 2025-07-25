// Copyright 2025 Nadrama Pty Ltd
// SPDX-License-Identifier: Apache-2.0

package s3client

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-kit/log/level"
)

// DeleteFile deletes a file from S3
func (s *S3Client) DeleteFile(ctx context.Context, key string) error {
	// Prepare S3 key with prefix
	s3Key := key
	if s.config.S3KeyPrefix() != "" {
		s3Key = s.config.S3KeyPrefix() + "/" + key
	}

	// Prepare delete object input
	bucketName := s.config.S3BucketName()
	input := &s3.DeleteObjectInput{
		Bucket: &bucketName,
		Key:    &s3Key,
	}

	// Delete from S3
	_, err := s.client.DeleteObject(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to delete file from S3: %w", err)
	}

	level.Debug(s.logger).Log("msg", "file deleted from S3", "key", s3Key, "bucket", s.config.S3BucketName())
	return nil
}
