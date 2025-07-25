// Copyright 2025 Nadrama Pty Ltd
// SPDX-License-Identifier: Apache-2.0

package peerapi

import (
	"strings"
	"testing"

	"github.com/nadrama-com/netsy/internal/proto"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

func TestParseTxnRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *pb.TxnRequest
		expected    *proto.Record
		expectError bool
		errorMsg    string
	}{
		// Valid create operation (compare mod_revision = 0)
		{
			name: "valid_create_operation",
			request: &pb.TxnRequest{
				Compare: []*pb.Compare{{
					Key:    []byte("test-key"),
					Target: pb.Compare_MOD,
					Result: pb.Compare_EQUAL,
					TargetUnion: &pb.Compare_ModRevision{
						ModRevision: 0,
					},
				}},
				Success: []*pb.RequestOp{{
					Request: &pb.RequestOp_RequestPut{
						RequestPut: &pb.PutRequest{
							Key:   []byte("test-key"),
							Value: []byte("test-value"),
							Lease: 123,
						},
					},
				}},
				Failure: []*pb.RequestOp{},
			},
			expected: &proto.Record{
				Key:     []byte("test-key"),
				Value:   []byte("test-value"),
				Lease:   123,
				Created: true,
				Deleted: false,
			},
			expectError: false,
		},
		// Valid update operation (compare mod_revision > 0)
		{
			name: "valid_update_operation",
			request: &pb.TxnRequest{
				Compare: []*pb.Compare{{
					Key:    []byte("test-key"),
					Target: pb.Compare_MOD,
					Result: pb.Compare_EQUAL,
					TargetUnion: &pb.Compare_ModRevision{
						ModRevision: 5,
					},
				}},
				Success: []*pb.RequestOp{{
					Request: &pb.RequestOp_RequestPut{
						RequestPut: &pb.PutRequest{
							Key:   []byte("test-key"),
							Value: []byte("updated-value"),
							Lease: 456,
						},
					},
				}},
				Failure: []*pb.RequestOp{{
					Request: &pb.RequestOp_RequestRange{
						RequestRange: &pb.RangeRequest{
							Key: []byte("test-key"),
						},
					},
				}},
			},
			expected: &proto.Record{
				Key:     []byte("test-key"),
				Value:   []byte("updated-value"),
				Lease:   456,
				Created: false,
				Deleted: false,
			},
			expectError: false,
		},
		// Valid delete operation
		{
			name: "valid_delete_operation",
			request: &pb.TxnRequest{
				Compare: []*pb.Compare{{
					Key:    []byte("test-key"),
					Target: pb.Compare_MOD,
					Result: pb.Compare_EQUAL,
					TargetUnion: &pb.Compare_ModRevision{
						ModRevision: 3,
					},
				}},
				Success: []*pb.RequestOp{{
					Request: &pb.RequestOp_RequestDeleteRange{
						RequestDeleteRange: &pb.DeleteRangeRequest{
							Key: []byte("test-key"),
						},
					},
				}},
				Failure: []*pb.RequestOp{{
					Request: &pb.RequestOp_RequestRange{
						RequestRange: &pb.RangeRequest{
							Key: []byte("test-key"),
						},
					},
				}},
			},
			expected: &proto.Record{
				Key:     []byte("test-key"),
				Value:   nil,
				Created: false,
				Deleted: true,
			},
			expectError: false,
		},
		// Valid: Create with a failure operation
		{
			name: "create_with_failure",
			request: &pb.TxnRequest{
				Compare: []*pb.Compare{{
					Key:    []byte("new-key"),
					Target: pb.Compare_MOD,
					Result: pb.Compare_EQUAL,
					TargetUnion: &pb.Compare_ModRevision{
						ModRevision: 0,
					},
				}},
				Success: []*pb.RequestOp{{
					Request: &pb.RequestOp_RequestPut{
						RequestPut: &pb.PutRequest{
							Key:   []byte("new-key"),
							Value: []byte("new-value"),
						},
					},
				}},
				Failure: []*pb.RequestOp{{
					Request: &pb.RequestOp_RequestRange{
						RequestRange: &pb.RangeRequest{
							Key: []byte("new-key"),
						},
					},
				}},
			},
			expected: &proto.Record{
				Key:     []byte("new-key"),
				Value:   []byte("new-value"),
				Lease:   0,
				Created: true,
				Deleted: false,
			},
			expectError: false,
		},
		// Invalid: Missing compare
		{
			name: "missing_compare",
			request: &pb.TxnRequest{
				Compare: []*pb.Compare{},
				Success: []*pb.RequestOp{{
					Request: &pb.RequestOp_RequestPut{
						RequestPut: &pb.PutRequest{
							Key:   []byte("test-key"),
							Value: []byte("test-value"),
						},
					},
				}},
			},
			expectError: true,
			errorMsg:    "invalid request - missing required fields",
		},
		// Invalid: Multiple compare operations
		{
			name: "multiple_compare",
			request: &pb.TxnRequest{
				Compare: []*pb.Compare{
					{
						Key:    []byte("key1"),
						Target: pb.Compare_MOD,
						Result: pb.Compare_EQUAL,
						TargetUnion: &pb.Compare_ModRevision{
							ModRevision: 0,
						},
					},
					{
						Key:    []byte("key2"),
						Target: pb.Compare_MOD,
						Result: pb.Compare_EQUAL,
						TargetUnion: &pb.Compare_ModRevision{
							ModRevision: 0,
						},
					},
				},
				Success: []*pb.RequestOp{{
					Request: &pb.RequestOp_RequestPut{
						RequestPut: &pb.PutRequest{
							Key:   []byte("key1"),
							Value: []byte("value1"),
						},
					},
				}},
			},
			expectError: true,
			errorMsg:    "invalid request - missing required fields",
		},
		// Invalid: Missing success operation
		{
			name: "missing_success",
			request: &pb.TxnRequest{
				Compare: []*pb.Compare{{
					Key:    []byte("test-key"),
					Target: pb.Compare_MOD,
					Result: pb.Compare_EQUAL,
					TargetUnion: &pb.Compare_ModRevision{
						ModRevision: 0,
					},
				}},
				Success: []*pb.RequestOp{},
			},
			expectError: true,
			errorMsg:    "invalid request - missing required fields",
		},
		// Invalid: Multiple success operations
		{
			name: "multiple_success",
			request: &pb.TxnRequest{
				Compare: []*pb.Compare{{
					Key:    []byte("test-key"),
					Target: pb.Compare_MOD,
					Result: pb.Compare_EQUAL,
					TargetUnion: &pb.Compare_ModRevision{
						ModRevision: 0,
					},
				}},
				Success: []*pb.RequestOp{
					{
						Request: &pb.RequestOp_RequestPut{
							RequestPut: &pb.PutRequest{
								Key:   []byte("test-key"),
								Value: []byte("value1"),
							},
						},
					},
					{
						Request: &pb.RequestOp_RequestPut{
							RequestPut: &pb.PutRequest{
								Key:   []byte("test-key"),
								Value: []byte("value2"),
							},
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "invalid request - missing required fields",
		},
		// Invalid: Multiple failure operations
		{
			name: "multiple_failure",
			request: &pb.TxnRequest{
				Compare: []*pb.Compare{{
					Key:    []byte("test-key"),
					Target: pb.Compare_MOD,
					Result: pb.Compare_EQUAL,
					TargetUnion: &pb.Compare_ModRevision{
						ModRevision: 5,
					},
				}},
				Success: []*pb.RequestOp{{
					Request: &pb.RequestOp_RequestPut{
						RequestPut: &pb.PutRequest{
							Key:   []byte("test-key"),
							Value: []byte("test-value"),
						},
					},
				}},
				Failure: []*pb.RequestOp{
					{
						Request: &pb.RequestOp_RequestRange{
							RequestRange: &pb.RangeRequest{
								Key: []byte("test-key"),
							},
						},
					},
					{
						Request: &pb.RequestOp_RequestRange{
							RequestRange: &pb.RangeRequest{
								Key: []byte("test-key"),
							},
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "invalid request - missing required fields",
		},
		// Invalid: Wrong compare target
		{
			name: "wrong_compare_target",
			request: &pb.TxnRequest{
				Compare: []*pb.Compare{{
					Key:    []byte("test-key"),
					Target: pb.Compare_VERSION,
					Result: pb.Compare_EQUAL,
					TargetUnion: &pb.Compare_ModRevision{
						ModRevision: 0,
					},
				}},
				Success: []*pb.RequestOp{{
					Request: &pb.RequestOp_RequestPut{
						RequestPut: &pb.PutRequest{
							Key:   []byte("test-key"),
							Value: []byte("test-value"),
						},
					},
				}},
			},
			expectError: true,
			errorMsg:    "invalid request - missing required fields",
		},
		// Invalid: Wrong compare result
		{
			name: "wrong_compare_result",
			request: &pb.TxnRequest{
				Compare: []*pb.Compare{{
					Key:    []byte("test-key"),
					Target: pb.Compare_MOD,
					Result: pb.Compare_GREATER,
					TargetUnion: &pb.Compare_ModRevision{
						ModRevision: 0,
					},
				}},
				Success: []*pb.RequestOp{{
					Request: &pb.RequestOp_RequestPut{
						RequestPut: &pb.PutRequest{
							Key:   []byte("test-key"),
							Value: []byte("test-value"),
						},
					},
				}},
			},
			expectError: true,
			errorMsg:    "invalid request - missing required fields",
		},
		// Invalid: Key mismatch between compare and success put
		{
			name: "key_mismatch_put",
			request: &pb.TxnRequest{
				Compare: []*pb.Compare{{
					Key:    []byte("compare-key"),
					Target: pb.Compare_MOD,
					Result: pb.Compare_EQUAL,
					TargetUnion: &pb.Compare_ModRevision{
						ModRevision: 0,
					},
				}},
				Success: []*pb.RequestOp{{
					Request: &pb.RequestOp_RequestPut{
						RequestPut: &pb.PutRequest{
							Key:   []byte("different-key"),
							Value: []byte("test-value"),
						},
					},
				}},
			},
			expectError: true,
			errorMsg:    "invalid request - key mismatch between compare and success operations",
		},
		// Invalid: Key mismatch between compare and success delete
		{
			name: "key_mismatch_delete",
			request: &pb.TxnRequest{
				Compare: []*pb.Compare{{
					Key:    []byte("compare-key"),
					Target: pb.Compare_MOD,
					Result: pb.Compare_EQUAL,
					TargetUnion: &pb.Compare_ModRevision{
						ModRevision: 5,
					},
				}},
				Success: []*pb.RequestOp{{
					Request: &pb.RequestOp_RequestDeleteRange{
						RequestDeleteRange: &pb.DeleteRangeRequest{
							Key: []byte("different-key"),
						},
					},
				}},
			},
			expectError: true,
			errorMsg:    "invalid request - key mismatch between compare and success operations",
		},
		// Invalid: Key mismatch between compare and failure operations
		{
			name: "key_mismatch_failure",
			request: &pb.TxnRequest{
				Compare: []*pb.Compare{{
					Key:    []byte("compare-key"),
					Target: pb.Compare_MOD,
					Result: pb.Compare_EQUAL,
					TargetUnion: &pb.Compare_ModRevision{
						ModRevision: 5,
					},
				}},
				Success: []*pb.RequestOp{{
					Request: &pb.RequestOp_RequestPut{
						RequestPut: &pb.PutRequest{
							Key:   []byte("compare-key"),
							Value: []byte("test-value"),
						},
					},
				}},
				Failure: []*pb.RequestOp{{
					Request: &pb.RequestOp_RequestRange{
						RequestRange: &pb.RangeRequest{
							Key: []byte("different-key"),
						},
					},
				}},
			},
			expectError: true,
			errorMsg:    "invalid request - key mismatch between compare and failure operations",
		},
		// Invalid: PrevKv in success put
		{
			name: "prevkv_in_put",
			request: &pb.TxnRequest{
				Compare: []*pb.Compare{{
					Key:    []byte("test-key"),
					Target: pb.Compare_MOD,
					Result: pb.Compare_EQUAL,
					TargetUnion: &pb.Compare_ModRevision{
						ModRevision: 0,
					},
				}},
				Success: []*pb.RequestOp{{
					Request: &pb.RequestOp_RequestPut{
						RequestPut: &pb.PutRequest{
							Key:    []byte("test-key"),
							Value:  []byte("test-value"),
							PrevKv: true,
						},
					},
				}},
			},
			expectError: true,
			errorMsg:    "invalid request - prevKv not supported for success put operations",
		},
		// Invalid: PrevKv in success delete
		{
			name: "prevkv_in_delete",
			request: &pb.TxnRequest{
				Compare: []*pb.Compare{{
					Key:    []byte("test-key"),
					Target: pb.Compare_MOD,
					Result: pb.Compare_EQUAL,
					TargetUnion: &pb.Compare_ModRevision{
						ModRevision: 5,
					},
				}},
				Success: []*pb.RequestOp{{
					Request: &pb.RequestOp_RequestDeleteRange{
						RequestDeleteRange: &pb.DeleteRangeRequest{
							Key:    []byte("test-key"),
							PrevKv: true,
						},
					},
				}},
			},
			expectError: true,
			errorMsg:    "invalid request - prevKv not supported for success delete operations",
		},
		// Invalid: RangeEnd in failure range
		{
			name: "rangeend_in_failure",
			request: &pb.TxnRequest{
				Compare: []*pb.Compare{{
					Key:    []byte("test-key"),
					Target: pb.Compare_MOD,
					Result: pb.Compare_EQUAL,
					TargetUnion: &pb.Compare_ModRevision{
						ModRevision: 5,
					},
				}},
				Success: []*pb.RequestOp{{
					Request: &pb.RequestOp_RequestPut{
						RequestPut: &pb.PutRequest{
							Key:   []byte("test-key"),
							Value: []byte("test-value"),
						},
					},
				}},
				Failure: []*pb.RequestOp{{
					Request: &pb.RequestOp_RequestRange{
						RequestRange: &pb.RangeRequest{
							Key:      []byte("test-key"),
							RangeEnd: []byte("test-key-end"),
						},
					},
				}},
			},
			expectError: true,
			errorMsg:    "invalid request - rangeEnd not supported for failure range operations",
		},
		// Valid delete operation (this was incorrectly marked as invalid in previous version)
		{
			name: "valid_delete_with_failure",
			request: &pb.TxnRequest{
				Compare: []*pb.Compare{{
					Key:    []byte("test-key"),
					Target: pb.Compare_MOD,
					Result: pb.Compare_EQUAL,
					TargetUnion: &pb.Compare_ModRevision{
						ModRevision: 5,
					},
				}},
				Success: []*pb.RequestOp{{
					Request: &pb.RequestOp_RequestDeleteRange{
						RequestDeleteRange: &pb.DeleteRangeRequest{
							Key: []byte("test-key"),
						},
					},
				}},
				Failure: []*pb.RequestOp{{
					Request: &pb.RequestOp_RequestRange{
						RequestRange: &pb.RangeRequest{
							Key: []byte("test-key"),
						},
					},
				}},
			},
			expected: &proto.Record{
				Key:     []byte("test-key"),
				Value:   nil,
				Created: false,
				Deleted: true,
			},
			expectError: false,
		},
		// Invalid: Indeterminate request - delete with mod_revision = 0
		{
			name: "indeterminate_delete_zero_revision",
			request: &pb.TxnRequest{
				Compare: []*pb.Compare{{
					Key:    []byte("test-key"),
					Target: pb.Compare_MOD,
					Result: pb.Compare_EQUAL,
					TargetUnion: &pb.Compare_ModRevision{
						ModRevision: 0,
					},
				}},
				Success: []*pb.RequestOp{{
					Request: &pb.RequestOp_RequestDeleteRange{
						RequestDeleteRange: &pb.DeleteRangeRequest{
							Key: []byte("test-key"),
						},
					},
				}},
			},
			expectError: true,
			errorMsg:    ErrUnsupported.Error(),
		},
		// Edge case: Create with empty value
		{
			name: "create_empty_value",
			request: &pb.TxnRequest{
				Compare: []*pb.Compare{{
					Key:    []byte("test-key"),
					Target: pb.Compare_MOD,
					Result: pb.Compare_EQUAL,
					TargetUnion: &pb.Compare_ModRevision{
						ModRevision: 0,
					},
				}},
				Success: []*pb.RequestOp{{
					Request: &pb.RequestOp_RequestPut{
						RequestPut: &pb.PutRequest{
							Key:   []byte("test-key"),
							Value: []byte(""),
						},
					},
				}},
			},
			expected: &proto.Record{
				Key:     []byte("test-key"),
				Value:   []byte(""),
				Lease:   0,
				Created: true,
				Deleted: false,
			},
			expectError: false,
		},
		// Edge case: Create with nil value
		{
			name: "create_nil_value",
			request: &pb.TxnRequest{
				Compare: []*pb.Compare{{
					Key:    []byte("test-key"),
					Target: pb.Compare_MOD,
					Result: pb.Compare_EQUAL,
					TargetUnion: &pb.Compare_ModRevision{
						ModRevision: 0,
					},
				}},
				Success: []*pb.RequestOp{{
					Request: &pb.RequestOp_RequestPut{
						RequestPut: &pb.PutRequest{
							Key:   []byte("test-key"),
							Value: nil,
						},
					},
				}},
			},
			expected: &proto.Record{
				Key:     []byte("test-key"),
				Value:   nil,
				Lease:   0,
				Created: true,
				Deleted: false,
			},
			expectError: false,
		},
		// Edge case: Empty key
		{
			name: "empty_key",
			request: &pb.TxnRequest{
				Compare: []*pb.Compare{{
					Key:    []byte(""),
					Target: pb.Compare_MOD,
					Result: pb.Compare_EQUAL,
					TargetUnion: &pb.Compare_ModRevision{
						ModRevision: 0,
					},
				}},
				Success: []*pb.RequestOp{{
					Request: &pb.RequestOp_RequestPut{
						RequestPut: &pb.PutRequest{
							Key:   []byte(""),
							Value: []byte("test-value"),
						},
					},
				}},
			},
			expected: &proto.Record{
				Key:     []byte(""),
				Value:   []byte("test-value"),
				Lease:   0,
				Created: true,
				Deleted: false,
			},
			expectError: false,
		},
		// Invalid: Failure operation with nil range request (regression test for nil pointer dereference)
		{
			name: "failure_with_nil_range_request",
			request: &pb.TxnRequest{
				Compare: []*pb.Compare{{
					Key:    []byte("test-key"),
					Target: pb.Compare_MOD,
					Result: pb.Compare_EQUAL,
					TargetUnion: &pb.Compare_ModRevision{
						ModRevision: 5,
					},
				}},
				Success: []*pb.RequestOp{{
					Request: &pb.RequestOp_RequestPut{
						RequestPut: &pb.PutRequest{
							Key:   []byte("test-key"),
							Value: []byte("test-value"),
						},
					},
				}},
				Failure: []*pb.RequestOp{{
					Request: &pb.RequestOp_RequestPut{
						RequestPut: &pb.PutRequest{
							Key:   []byte("test-key"),
							Value: []byte("failure-value"),
						},
					},
				}},
			},
			expectError: true,
			errorMsg:    "invalid request - failure operation must contain a range request",
		},
		// Invalid: Success operation with nil put request (regression test for potential nil pointer dereference)
		{
			name: "success_with_nil_put_request",
			request: &pb.TxnRequest{
				Compare: []*pb.Compare{{
					Key:    []byte("test-key"),
					Target: pb.Compare_MOD,
					Result: pb.Compare_EQUAL,
					TargetUnion: &pb.Compare_ModRevision{
						ModRevision: 0,
					},
				}},
				Success: []*pb.RequestOp{{
					Request: &pb.RequestOp_RequestRange{
						RequestRange: &pb.RangeRequest{
							Key: []byte("test-key"),
						},
					},
				}},
			},
			expectError: true,
			errorMsg:    ErrUnsupported.Error(),
		},
		// Invalid: Success operation with nil delete request (regression test for potential nil pointer dereference)
		{
			name: "success_with_nil_delete_request",
			request: &pb.TxnRequest{
				Compare: []*pb.Compare{{
					Key:    []byte("test-key"),
					Target: pb.Compare_MOD,
					Result: pb.Compare_EQUAL,
					TargetUnion: &pb.Compare_ModRevision{
						ModRevision: 5,
					},
				}},
				Success: []*pb.RequestOp{{
					Request: &pb.RequestOp_RequestRange{
						RequestRange: &pb.RangeRequest{
							Key: []byte("test-key"),
						},
					},
				}},
				Failure: []*pb.RequestOp{{
					Request: &pb.RequestOp_RequestRange{
						RequestRange: &pb.RangeRequest{
							Key: []byte("test-key"),
						},
					},
				}},
			},
			expectError: true,
			errorMsg:    ErrUnsupported.Error(),
		},
		// Invalid: Success operation is neither put nor delete
		{
			name: "invalid_success_operation",
			request: &pb.TxnRequest{
				Compare: []*pb.Compare{{
					Key:    []byte("test-key"),
					Target: pb.Compare_MOD,
					Result: pb.Compare_EQUAL,
					TargetUnion: &pb.Compare_ModRevision{
						ModRevision: 0,
					},
				}},
				Success: []*pb.RequestOp{{
					Request: &pb.RequestOp_RequestRange{
						RequestRange: &pb.RangeRequest{
							Key: []byte("test-key"),
						},
					},
				}},
			},
			expectError: true,
			errorMsg:    ErrUnsupported.Error(),
		},
		// Invalid: Update with zero mod revision (indeterminate)
		{
			name: "update_zero_revision",
			request: &pb.TxnRequest{
				Compare: []*pb.Compare{{
					Key:    []byte("test-key"),
					Target: pb.Compare_MOD,
					Result: pb.Compare_EQUAL,
					TargetUnion: &pb.Compare_ModRevision{
						ModRevision: 0,
					},
				}},
				Success: []*pb.RequestOp{{
					Request: &pb.RequestOp_RequestPut{
						RequestPut: &pb.PutRequest{
							Key:   []byte("test-key"),
							Value: []byte("test-value"),
						},
					},
				}},
			},
			expected: &proto.Record{
				Key:     []byte("test-key"),
				Value:   []byte("test-value"),
				Lease:   0,
				Created: true, // This is actually create, not update
				Deleted: false,
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseTxnRequest(tt.request)

			if tt.expectError {
				if err == nil {
					t.Errorf("ParseTxnRequest() expected error but got none")
					return
				}
				if tt.errorMsg != "" && !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("ParseTxnRequest() error = %v, want error containing %v", err.Error(), tt.errorMsg)
				}
				return
			}

			if err != nil {
				t.Errorf("ParseTxnRequest() unexpected error = %v", err)
				return
			}

			if result == nil {
				t.Errorf("ParseTxnRequest() returned nil result")
				return
			}

			// Compare the result with expected
			if string(result.Key) != string(tt.expected.Key) {
				t.Errorf("ParseTxnRequest() key = %v, want %v", string(result.Key), string(tt.expected.Key))
			}
			if string(result.Value) != string(tt.expected.Value) {
				t.Errorf("ParseTxnRequest() value = %v, want %v", string(result.Value), string(tt.expected.Value))
			}
			if result.Lease != tt.expected.Lease {
				t.Errorf("ParseTxnRequest() lease = %v, want %v", result.Lease, tt.expected.Lease)
			}
			if result.Created != tt.expected.Created {
				t.Errorf("ParseTxnRequest() created = %v, want %v", result.Created, tt.expected.Created)
			}
			if result.Deleted != tt.expected.Deleted {
				t.Errorf("ParseTxnRequest() deleted = %v, want %v", result.Deleted, tt.expected.Deleted)
			}
		})
	}
}

func TestBuildTxnResponse(t *testing.T) {
	tests := []struct {
		name        string
		record      *proto.Record
		rangeResp   *pb.RangeResponse
		expected    *pb.TxnResponse
		expectError bool
		errorMsg    string
	}{
		// Case 1: Range response provided (failure case)
		{
			name: "range_response_failure",
			record: &proto.Record{
				Revision: 100,
				Key:      []byte("test-key"),
				Value:    []byte("test-value"),
			},
			rangeResp: &pb.RangeResponse{
				Header: &pb.ResponseHeader{
					Revision: 99,
				},
				Kvs: []*mvccpb.KeyValue{
					{
						Key:            []byte("test-key"),
						Value:          []byte("existing-value"),
						ModRevision:    99,
						CreateRevision: 50,
						Version:        5,
					},
				},
				Count: 1,
			},
			expected: &pb.TxnResponse{
				Header: &pb.ResponseHeader{
					Revision: 99,
				},
				Succeeded: false,
				Responses: []*pb.ResponseOp{
					{
						Response: &pb.ResponseOp_ResponseRange{
							ResponseRange: &pb.RangeResponse{
								Header: &pb.ResponseHeader{
									Revision: 99,
								},
								Kvs: []*mvccpb.KeyValue{
									{
										Key:            []byte("test-key"),
										Value:          []byte("existing-value"),
										ModRevision:    99,
										CreateRevision: 50,
										Version:        5,
									},
								},
								Count: 1,
							},
						},
					},
				},
			},
			expectError: false,
		},
		// Case 2: Delete operation (record.Deleted = true)
		{
			name: "delete_operation_success",
			record: &proto.Record{
				Revision: 150,
				Key:      []byte("delete-key"),
				Value:    nil,
				Deleted:  true,
			},
			rangeResp: nil,
			expected: &pb.TxnResponse{
				Header: &pb.ResponseHeader{
					Revision: 150,
				},
				Succeeded: true,
				Responses: []*pb.ResponseOp{
					{
						Response: &pb.ResponseOp_ResponseDeleteRange{
							ResponseDeleteRange: &pb.DeleteRangeResponse{
								Header: &pb.ResponseHeader{
									Revision: 150,
								},
								Deleted: 1,
							},
						},
					},
				},
			},
			expectError: false,
		},
		// Case 3: Create operation
		{
			name: "put_operation_success",
			record: &proto.Record{
				Revision: 200,
				Key:      []byte("put-key"),
				Value:    []byte("put-value"),
				Created:  true,
				Deleted:  false,
			},
			rangeResp: nil,
			expected: &pb.TxnResponse{
				Header: &pb.ResponseHeader{
					Revision: 200,
				},
				Succeeded: true,
				Responses: []*pb.ResponseOp{
					{
						Response: &pb.ResponseOp_ResponsePut{
							ResponsePut: &pb.PutResponse{
								Header: &pb.ResponseHeader{
									Revision: 200,
								},
							},
						},
					},
				},
			},
			expectError: false,
		},
		// Case 4: Update operation (Created=false, Deleted=false)
		{
			name: "update_operation_success",
			record: &proto.Record{
				Revision: 250,
				Key:      []byte("update-key"),
				Value:    []byte("updated-value"),
				Created:  false,
				Deleted:  false,
				Lease:    456,
			},
			rangeResp: nil,
			expected: &pb.TxnResponse{
				Header: &pb.ResponseHeader{
					Revision: 250,
				},
				Succeeded: true,
				Responses: []*pb.ResponseOp{
					{
						Response: &pb.ResponseOp_ResponsePut{
							ResponsePut: &pb.PutResponse{
								Header: &pb.ResponseHeader{
									Revision: 250,
								},
							},
						},
					},
				},
			},
			expectError: false,
		},
		// Case 5: Empty range response (no kvs)
		{
			name: "empty_range_response",
			record: &proto.Record{
				Revision: 75,
				Key:      []byte("nonexistent-key"),
			},
			rangeResp: &pb.RangeResponse{
				Header: &pb.ResponseHeader{
					Revision: 74,
				},
				Kvs:   []*mvccpb.KeyValue{},
				Count: 0,
			},
			expected: &pb.TxnResponse{
				Header: &pb.ResponseHeader{
					Revision: 74,
				},
				Succeeded: false,
				Responses: []*pb.ResponseOp{
					{
						Response: &pb.ResponseOp_ResponseRange{
							ResponseRange: &pb.RangeResponse{
								Header: &pb.ResponseHeader{
									Revision: 74,
								},
								Kvs:   []*mvccpb.KeyValue{},
								Count: 0,
							},
						},
					},
				},
			},
			expectError: false,
		},
		// Case 6: Zero revision
		{
			name: "zero_revision",
			record: &proto.Record{
				Revision: 0,
				Key:      []byte("zero-key"),
				Value:    []byte("zero-value"),
				Created:  true,
			},
			rangeResp: nil,
			expected: &pb.TxnResponse{
				Header: &pb.ResponseHeader{
					Revision: 0,
				},
				Succeeded: true,
				Responses: []*pb.ResponseOp{
					{
						Response: &pb.ResponseOp_ResponsePut{
							ResponsePut: &pb.PutResponse{
								Header: &pb.ResponseHeader{
									Revision: 0,
								},
							},
						},
					},
				},
			},
			expectError: false,
		},
		// Case 7: High revision number
		{
			name: "high_revision",
			record: &proto.Record{
				Revision: 9223372036854775807, // max int64
				Key:      []byte("high-key"),
				Value:    []byte("high-value"),
			},
			rangeResp: nil,
			expected: &pb.TxnResponse{
				Header: &pb.ResponseHeader{
					Revision: 9223372036854775807,
				},
				Succeeded: true,
				Responses: []*pb.ResponseOp{
					{
						Response: &pb.ResponseOp_ResponsePut{
							ResponsePut: &pb.PutResponse{
								Header: &pb.ResponseHeader{
									Revision: 9223372036854775807,
								},
							},
						},
					},
				},
			},
			expectError: false,
		},
		// Case 8: Delete with empty key
		{
			name: "delete_empty_key",
			record: &proto.Record{
				Revision: 123,
				Key:      []byte(""),
				Value:    nil,
				Deleted:  true,
			},
			rangeResp: nil,
			expected: &pb.TxnResponse{
				Header: &pb.ResponseHeader{
					Revision: 123,
				},
				Succeeded: true,
				Responses: []*pb.ResponseOp{
					{
						Response: &pb.ResponseOp_ResponseDeleteRange{
							ResponseDeleteRange: &pb.DeleteRangeResponse{
								Header: &pb.ResponseHeader{
									Revision: 123,
								},
								Deleted: 1,
							},
						},
					},
				},
			},
			expectError: false,
		},
		// Case 9: Put with empty value
		{
			name: "put_empty_value",
			record: &proto.Record{
				Revision: 300,
				Key:      []byte("empty-value-key"),
				Value:    []byte(""),
				Created:  true,
			},
			rangeResp: nil,
			expected: &pb.TxnResponse{
				Header: &pb.ResponseHeader{
					Revision: 300,
				},
				Succeeded: true,
				Responses: []*pb.ResponseOp{
					{
						Response: &pb.ResponseOp_ResponsePut{
							ResponsePut: &pb.PutResponse{
								Header: &pb.ResponseHeader{
									Revision: 300,
								},
							},
						},
					},
				},
			},
			expectError: false,
		},
		// Case 10: Put with nil value
		{
			name: "put_nil_value",
			record: &proto.Record{
				Revision: 350,
				Key:      []byte("nil-value-key"),
				Value:    nil,
				Created:  true,
			},
			rangeResp: nil,
			expected: &pb.TxnResponse{
				Header: &pb.ResponseHeader{
					Revision: 350,
				},
				Succeeded: true,
				Responses: []*pb.ResponseOp{
					{
						Response: &pb.ResponseOp_ResponsePut{
							ResponsePut: &pb.PutResponse{
								Header: &pb.ResponseHeader{
									Revision: 350,
								},
							},
						},
					},
				},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := BuildTxnResponse(tt.record, tt.rangeResp)

			if tt.expectError {
				if err == nil {
					t.Errorf("BuildTxnResponse() expected error but got none")
					return
				}
				if tt.errorMsg != "" && !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("BuildTxnResponse() error = %v, want error containing %v", err.Error(), tt.errorMsg)
				}
				return
			}

			if err != nil {
				t.Errorf("BuildTxnResponse() unexpected error = %v", err)
				return
			}

			if result == nil {
				t.Errorf("BuildTxnResponse() returned nil result")
				return
			}

			// Compare header
			if result.Header == nil {
				t.Errorf("BuildTxnResponse() header is nil")
				return
			}
			if result.Header.Revision != tt.expected.Header.Revision {
				t.Errorf("BuildTxnResponse() header revision = %v, want %v", result.Header.Revision, tt.expected.Header.Revision)
			}

			// Compare succeeded flag
			if result.Succeeded != tt.expected.Succeeded {
				t.Errorf("BuildTxnResponse() succeeded = %v, want %v", result.Succeeded, tt.expected.Succeeded)
			}

			// Compare responses length
			if len(result.Responses) != len(tt.expected.Responses) {
				t.Errorf("BuildTxnResponse() responses length = %v, want %v", len(result.Responses), len(tt.expected.Responses))
				return
			}

			// Compare response content
			if len(result.Responses) > 0 {
				actualResp := result.Responses[0]
				expectedResp := tt.expected.Responses[0]

				// Check range response
				if expectedResp.GetResponseRange() != nil {
					actualRange := actualResp.GetResponseRange()
					expectedRange := expectedResp.GetResponseRange()
					if actualRange == nil {
						t.Errorf("BuildTxnResponse() expected range response but got nil")
						return
					}
					if actualRange.Header.Revision != expectedRange.Header.Revision {
						t.Errorf("BuildTxnResponse() range response revision = %v, want %v", actualRange.Header.Revision, expectedRange.Header.Revision)
					}
					if actualRange.Count != expectedRange.Count {
						t.Errorf("BuildTxnResponse() range response count = %v, want %v", actualRange.Count, expectedRange.Count)
					}
					if len(actualRange.Kvs) != len(expectedRange.Kvs) {
						t.Errorf("BuildTxnResponse() range response kvs length = %v, want %v", len(actualRange.Kvs), len(expectedRange.Kvs))
					}
				}

				// Check delete response
				if expectedResp.GetResponseDeleteRange() != nil {
					actualDelete := actualResp.GetResponseDeleteRange()
					expectedDelete := expectedResp.GetResponseDeleteRange()
					if actualDelete == nil {
						t.Errorf("BuildTxnResponse() expected delete response but got nil")
						return
					}
					if actualDelete.Header.Revision != expectedDelete.Header.Revision {
						t.Errorf("BuildTxnResponse() delete response revision = %v, want %v", actualDelete.Header.Revision, expectedDelete.Header.Revision)
					}
					if actualDelete.Deleted != expectedDelete.Deleted {
						t.Errorf("BuildTxnResponse() delete response deleted count = %v, want %v", actualDelete.Deleted, expectedDelete.Deleted)
					}
				}

				// Check put response
				if expectedResp.GetResponsePut() != nil {
					actualPut := actualResp.GetResponsePut()
					expectedPut := expectedResp.GetResponsePut()
					if actualPut == nil {
						t.Errorf("BuildTxnResponse() expected put response but got nil")
						return
					}
					if actualPut.Header.Revision != expectedPut.Header.Revision {
						t.Errorf("BuildTxnResponse() put response revision = %v, want %v", actualPut.Header.Revision, expectedPut.Header.Revision)
					}
				}
			}
		})
	}
}
