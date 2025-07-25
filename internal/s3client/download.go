// Copyright 2025 Nadrama Pty Ltd
// SPDX-License-Identifier: Apache-2.0

package s3client

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-kit/log/level"
)

// DownloadFile downloads a file from S3, automatically choosing the best strategy based on size
// Returns a reader that should be closed by the caller
func (s *S3Client) DownloadFile(ctx context.Context, key string, size int64, dataDir string, tempFiles *[]string) (io.ReadCloser, error) {
	const maxMemorySize = 2 * 1024 * 1024 // 2MB

	if size > maxMemorySize {
		return s.downloadLargeFile(ctx, key, size, dataDir, tempFiles)
	} else {
		return s.downloadSmallFile(ctx, key)
	}
}

// downloadSmallFile downloads small files to memory with retry logic
func (s *S3Client) downloadSmallFile(ctx context.Context, key string) (io.ReadCloser, error) {
	level.Debug(s.logger).Log("msg", "downloading small file to memory", "key", key)

	bucketName := s.config.S3BucketName()
	input := &s3.GetObjectInput{
		Bucket: &bucketName,
		Key:    &key,
	}

	var lastErr error
	maxRetries := 3
	baseDelay := 100 * time.Millisecond

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff with jitter
			delay := time.Duration(attempt) * baseDelay
			level.Debug(s.logger).Log("msg", "retrying small file download", "key", key, "attempt", attempt+1, "delay", delay)
			time.Sleep(delay)
		}

		output, err := s.client.GetObject(ctx, input)
		if err != nil {
			lastErr = err
			level.Debug(s.logger).Log("msg", "small file download attempt failed", "key", key, "attempt", attempt+1, "error", err)
			continue
		}

		level.Debug(s.logger).Log("msg", "small file download succeeded", "key", key, "attempt", attempt+1)
		return output.Body, nil
	}

	return nil, fmt.Errorf("failed to download small file after %d attempts: %w", maxRetries, lastErr)
}

// downloadLargeFile downloads large files to disk with multipart support
func (s *S3Client) downloadLargeFile(ctx context.Context, key string, size int64, dataDir string, tempFiles *[]string) (io.ReadCloser, error) {
	level.Debug(s.logger).Log("msg", "downloading large file to disk", "key", key, "size", size)

	// Determine file prefix based on key
	var prefix string
	if strings.Contains(key, "snapshots/") {
		prefix = "snapshot_"
	} else {
		prefix = "chunk_"
	}

	// Create temporary file
	tempFile, err := os.CreateTemp(dataDir, prefix+"*.netsy")
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary file: %w", err)
	}
	tempPath := tempFile.Name()
	*tempFiles = append(*tempFiles, tempPath)

	// Use AWS S3 downloader with multipart support
	downloader := manager.NewDownloader(s.client, func(d *manager.Downloader) {
		// Configure multipart download - use parts for files >10MB
		if size > 10*1024*1024 {
			d.PartSize = 5 * 1024 * 1024 // 5MB parts
		}
		d.Concurrency = 3 // Download up to 3 parts concurrently
	})

	bucketName := s.config.S3BucketName()
	_, err = downloader.Download(ctx, tempFile, &s3.GetObjectInput{
		Bucket: &bucketName,
		Key:    &key,
	})
	if err != nil {
		tempFile.Close()
		return nil, fmt.Errorf("failed to download large file from S3: %w", err)
	}

	// Close and reopen file for reading
	tempFile.Close()

	readFile, err := os.Open(tempPath)
	if err != nil {
		return nil, fmt.Errorf("failed to reopen downloaded file: %w", err)
	}

	level.Debug(s.logger).Log("msg", "large file download succeeded", "key", key, "path", tempPath)
	return readFile, nil
}
