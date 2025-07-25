// Copyright 2025 Nadrama Pty Ltd
// SPDX-License-Identifier: Apache-2.0

package s3client

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/go-kit/log/level"
)

// UploadFile uploads a local file to S3
func (s *S3Client) UploadFile(ctx context.Context, key, filePath string) error {
	// Open local file
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	// Get file info for content length
	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	// Prepare S3 key with prefix
	s3Key := key
	if s.config.S3KeyPrefix() != "" {
		s3Key = s.config.S3KeyPrefix() + "/" + key
	}

	// Prepare put object input
	bucketName := s.config.S3BucketName()
	storageClass := s.config.S3StorageClass()
	input := &s3.PutObjectInput{
		Bucket:       &bucketName,
		Key:          &s3Key,
		Body:         file,
		ContentLength: aws.Int64(fileInfo.Size()),
		StorageClass: types.StorageClass(storageClass),
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
	level.Debug(s.logger).Log("msg", "uploading to S3", "bucket", s.config.S3BucketName(), "key", s3Key, "size", fileInfo.Size())
	_, err = s.client.PutObject(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to upload file to S3: %w", err)
	}

	level.Info(s.logger).Log("msg", "file uploaded to S3", "key", s3Key, "bucket", s.config.S3BucketName(), "size", fileInfo.Size())
	return nil
}
