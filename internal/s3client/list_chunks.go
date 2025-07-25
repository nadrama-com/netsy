// Copyright 2025 Nadrama Pty Ltd
// SPDX-License-Identifier: Apache-2.0

package s3client

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-kit/log/level"
)

// ListChunks returns all chunk files with revision > fromRevision, sorted by revision (oldest first)
func (s *S3Client) ListChunks(ctx context.Context, fromRevision int64) ([]FileInfo, error) {
	prefix := "chunks/"
	if s.config.S3KeyPrefix() != "" {
		prefix = s.config.S3KeyPrefix() + "/" + prefix
	}

	bucketName := s.config.S3BucketName()
	input := &s3.ListObjectsV2Input{
		Bucket: &bucketName,
		Prefix: &prefix,
	}

	var chunks []FileInfo
	paginator := s3.NewListObjectsV2Paginator(s.client, input)

	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list chunk objects: %w", err)
		}

		for _, obj := range output.Contents {
			// Extract revision from filename: chunks/{partition}/{revision}.netsy
			keyParts := strings.Split(*obj.Key, "/")
			if len(keyParts) < 3 {
				continue
			}
			filename := keyParts[len(keyParts)-1]
			if !strings.HasSuffix(filename, ".netsy") {
				continue
			}
			revisionStr := strings.TrimSuffix(filename, ".netsy")
			revision, err := strconv.ParseInt(revisionStr, 10, 64)
			if err != nil {
				level.Debug(s.logger).Log("msg", "skipping invalid chunk filename", "filename", filename)
				continue
			}

			// Only include chunks with revision > fromRevision
			if revision > fromRevision {
				chunks = append(chunks, FileInfo{
					Key:      *obj.Key,
					Size:     *obj.Size,
					Revision: revision,
				})
			}
		}
	}

	// Sort by revision (oldest first)
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].Revision < chunks[j].Revision
	})

	return chunks, nil
}
