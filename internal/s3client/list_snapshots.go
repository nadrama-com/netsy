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

// ListSnapshots returns all snapshot files sorted by revision (newest first)
func (s *S3Client) ListSnapshots(ctx context.Context) ([]FileInfo, error) {
	prefix := "snapshots/"
	if s.config.S3KeyPrefix() != "" {
		prefix = s.config.S3KeyPrefix() + "/" + prefix
	}

	bucketName := s.config.S3BucketName()
	input := &s3.ListObjectsV2Input{
		Bucket: &bucketName,
		Prefix: &prefix,
	}

	var snapshots []FileInfo
	paginator := s3.NewListObjectsV2Paginator(s.client, input)

	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list snapshot objects: %w", err)
		}

		for _, obj := range output.Contents {
			// Extract revision from filename: snapshots/{revision}.netsy
			keyParts := strings.Split(*obj.Key, "/")
			if len(keyParts) < 2 {
				continue
			}
			filename := keyParts[len(keyParts)-1]
			if !strings.HasSuffix(filename, ".netsy") {
				continue
			}
			revisionStr := strings.TrimSuffix(filename, ".netsy")
			revision, err := strconv.ParseInt(revisionStr, 10, 64)
			if err != nil {
				level.Debug(s.logger).Log("msg", "skipping invalid snapshot filename", "filename", filename)
				continue
			}

			snapshots = append(snapshots, FileInfo{
				Key:      *obj.Key,
				Size:     *obj.Size,
				Revision: revision,
			})
		}
	}

	// Sort by revision (newest first)
	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].Revision > snapshots[j].Revision
	})

	return snapshots, nil
}
