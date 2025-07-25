// Copyright 2025 Nadrama Pty Ltd
// SPDX-License-Identifier: Apache-2.0

package clientapi

import (
	"fmt"
	"testing"

	"github.com/nadrama-com/netsy/internal/proto"
)

func TestIsWatchMatch(t *testing.T) {
	tests := []struct {
		w      watch
		record *proto.Record
		expect bool
	}{
		// different key
		{watch{key: []byte("1")}, &proto.Record{Key: []byte("")}, false},
		{watch{key: []byte("1")}, &proto.Record{Key: []byte("")}, false},
		{watch{key: []byte("1")}, &proto.Record{Key: []byte("")}, false},
		// exact match
		{watch{key: []byte("1")}, &proto.Record{Key: []byte("1")}, true},
		{watch{key: []byte("1")}, &proto.Record{Key: []byte("1")}, true},
		{watch{key: []byte("1")}, &proto.Record{Key: []byte("1")}, true},
		// 1 inside range 1-3
		{watch{key: []byte("1"), rangeEnd: []byte("3")}, &proto.Record{Key: []byte("1")}, true},
		{watch{key: []byte("1"), rangeEnd: []byte("3")}, &proto.Record{Key: []byte("1")}, true},
		{watch{key: []byte("1"), rangeEnd: []byte("3")}, &proto.Record{Key: []byte("1")}, true},
		// 2 inside range 1-3
		{watch{key: []byte("1"), rangeEnd: []byte("3")}, &proto.Record{Key: []byte("2")}, true},
		{watch{key: []byte("1"), rangeEnd: []byte("3")}, &proto.Record{Key: []byte("2")}, true},
		{watch{key: []byte("1"), rangeEnd: []byte("3")}, &proto.Record{Key: []byte("2")}, true},
		// 1 prefix match (range 1-2 triggers prefix match)
		{watch{key: []byte("1"), rangeEnd: []byte("2")}, &proto.Record{Key: []byte("1")}, true},
		{watch{key: []byte("1"), rangeEnd: []byte("2")}, &proto.Record{Key: []byte("1")}, true},
		{watch{key: []byte("1"), rangeEnd: []byte("2")}, &proto.Record{Key: []byte("1")}, true},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			result := isWatchMatch(test.w, test.record)
			if result != test.expect {
				t.Errorf("isWatchMatch(%+v, %+v)\n= %t\nwant %t", test.w, test.record, result, test.expect)
			}
		})
	}
}
