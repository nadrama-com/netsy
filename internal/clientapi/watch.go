// Copyright 2025 Nadrama Pty Ltd
// SPDX-License-Identifier: Apache-2.0

package clientapi

// glossory
// watcher - watchers represents a single gRPC bidrectional stream client
//           e.g. kube-apiserver
// watch   - watches range on/track specific events. multiple per watcher.
//           e.g. multiple `kubectl watch` commands connected to a
//                single kube-apiserver watcher.

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/nadrama-com/netsy/internal/proto"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

//
// watcher
//

// watchers is a map of watcher IDs to watcher structs.
// it also has a RWMutex because we need to lock the map,
// as new watchers are created on separate goroutines,
// and then cleanup happens when each of those goroutines ends.
// watcher is used by the dispatcher as well as Watch handler.
// the dispatcher only requires a read lock,
// everything else requires a write lock.
type watchers struct {
	sync.RWMutex
	servers map[int64]*watcher
}

// we track all watchers in a global
var allWatchers = watchers{
	servers: map[int64]*watcher{},
}

// watcherIDCounter is a global counter for watcher IDs
// atomic.AddInt64 is used to increment it while a lock is held
var watcherIDCounter int64

// watcher is a watch server that handles requests from a single client,
// where each client may have one or more 'watch(es)' and each 'watch' may have
// progress notifications enabled.
// client is a gRPC bidirectional stream
// inboxCh is used to send WatchResponse messages to the watcher
// data flow (where brackets represent other components):
// (kubeapi-server) > client.Recv > Get[Create|Cancel|Progress]Request > (api)
// (netsy Leader) > inboxCh > client.Send > (kube-apiserver) [> watcher client]
type watcher struct {
	id int64
	sync.RWMutex
	client   pb.Watch_WatchServer // the gRPC stream
	inboxOk  bool
	inboxCh  chan pb.WatchResponse
	watches  map[int64]watch
	progress map[int64]bool
}

// Cleanup is used to cleanup a watcher
// It closes/cancels any watches and related progress channels,
// then removes itself from the watchers map
func (w *watcher) Cleanup(watcherID int64) {
	fmt.Printf("watcher.Cleanup(%d)\n", watcherID)

	// obtain watcher write lock and release at end of the function
	w.Lock()
	defer w.Unlock()

	// close the watcher inbox channel
	w.inboxOk = false
	close(w.inboxCh)

	// remove all watchIDs from watcher (in case Cancel was not processed)
	for watchID, watch := range w.watches {
		watch.cancel()
		delete(w.watches, watchID)
	}
	for watchID := range w.progress {
		delete(w.progress, watchID)
	}

	// remove watcherID from all watchers map
	// obtain write lock, remove, then release lock immediately
	allWatchers.Lock()
	delete(allWatchers.servers, watcherID)
	allWatchers.Unlock()
}

//
// watch
//

// watch holds information from a CreateWatchRequest, plus a
// context cancellation function
type watch struct {
	key             []byte
	rangeEnd        []byte
	startRevision   int64
	prevKv          bool
	progressNotify  bool
	filtersNoPut    bool
	filtersNoDelete bool
	cancel          func()
}

// watchIDCounter is a global counter for watch IDs
// we do this because we do not support client-supplied watch IDs
// atomic.AddInt64 is used to increment it while a lock is held
var watchIDCounter int64

// CreateWatch handles watch create requests
func (w *watcher) CreateWatch(r *pb.WatchCreateRequest, latestRevision int64, getRevision func(findRevision int64) (revision int64, compacted bool, compactedAt sql.NullString, err error)) {
	fmt.Printf("CreateWatch(%d)\n", w.id)

	respHeader := &pb.ResponseHeader{
		Revision: latestRevision,
	}

	// do not support user-provided watch IDs
	if r.WatchId != clientv3.AutoWatchID {
		fmt.Printf("user-provided watch IDs ('%d') are unsupported", r.WatchId)
		_ = w.client.Send(&pb.WatchResponse{
			Header:  respHeader,
			Created: true,
			WatchId: r.WatchId,
		})
		_ = w.client.Send(&pb.WatchResponse{
			Header:       respHeader,
			Canceled:     true,
			CancelReason: "user-provided watch IDs are unsupported",
			WatchId:      r.WatchId,
		})
		return
	}

	// create a globally-unique watch ID
	watchID := atomic.AddInt64(&watchIDCounter, 1)

	// get cancel function associated with watch server
	_, cancelFunc := context.WithCancel(w.client.Context())

	// check if start revision exists or has been compacted
	// if it is set to zero, use latest revision and do not return error
	var revision int64
	var compacted bool
	var err error
	if r.StartRevision == 0 {
		revision = latestRevision
	} else {
		revision, compacted, _, err = getRevision(r.StartRevision)
	}
	respHeader.Revision = revision
	if err != nil || compacted {
		var cancelReason string
		var compactRevision int64
		if compacted {
			compactRevision = r.StartRevision
			cancelReason = fmt.Sprintf("revision '%d' has been compacted", r.StartRevision)
		} else if r.StartRevision <= latestRevision {
			respHeader.Revision = r.StartRevision
			cancelReason = fmt.Sprintf("failed to get revision '%d' for CreateWatch: %v", r.StartRevision, err)
		} else {
			// if asking for future revision, use latest
			revision = latestRevision
		}
		if cancelReason != "" {
			fmt.Printf("CreateWatch() failed: %s\n", cancelReason)
			w.client.Send(&pb.WatchResponse{
				Header:  respHeader,
				Created: true,
				WatchId: watchID,
			})
			w.client.Send(&pb.WatchResponse{
				Header:          respHeader,
				Canceled:        true,
				CancelReason:    cancelReason,
				CompactRevision: compactRevision,
				WatchId:         watchID,
			})
			cancelFunc()
			return
		}
	}

	// prep watch
	watchData := watch{
		key:            r.Key,
		rangeEnd:       r.RangeEnd,
		startRevision:  r.StartRevision,
		prevKv:         r.PrevKv,
		progressNotify: r.ProgressNotify,
		cancel:         cancelFunc,
	}
	for _, filterType := range r.Filters {
		switch filterType {
		case pb.WatchCreateRequest_NOPUT:
			watchData.filtersNoPut = true
		case pb.WatchCreateRequest_NODELETE:
			watchData.filtersNoDelete = true
		}
	}

	// add watchID to to the watcher
	// obtain write lock, add, then release lock immediately
	w.Lock()
	w.watches[watchID] = watchData
	w.progress[watchID] = r.ProgressNotify
	w.Unlock()

	// acknowledge the watch create request to the client
	if err := w.client.Send(&pb.WatchResponse{
		Header:  respHeader,
		Created: true,
		WatchId: watchID,
	}); err != nil {
		// cancel watch if unable to send ack
		w.CancelWatch(watchID, revision, err)
		return
	}
}

// CancelWatch handles watch cancel requests for a watch server instance.
// note that it may be called from multiple different go routines.
// arguments:
// * revision - latest known revision to place in response header.
// * reason - if watch is being cancelled due to an an unexpected error.
func (w *watcher) CancelWatch(watchID int64, revision int64, reason error) {
	fmt.Printf("CancelWatch()\n")

	// remove watchID from watcher
	// obtain write lock, cancel, delete, then release lock immediately
	w.Lock()
	if watch, ok := w.watches[watchID]; ok {
		watch.cancel()
	}
	delete(w.watches, watchID)
	delete(w.progress, watchID)
	w.Unlock()

	// ack cancellation with client
	reasonMsg := ""
	if reason != nil {
		reasonMsg = reason.Error()
	}
	err := w.client.Send(&pb.WatchResponse{
		Header: &pb.ResponseHeader{
			Revision: revision,
		},
		Canceled:     reason != nil,
		CancelReason: reasonMsg,
		WatchId:      watchID,
	})
	if err != nil && reason != nil && !clientv3.IsConnCanceled(err) {
		// TODO better error logging
		fmt.Printf("error: failed to send cancel to watch ID %d: %v", watchID, err)
	}
}

// ReportProgressOnInterval sends a progress report (aka the latest revision)
// on an interval to all watchers which have progress notifications enabled.
// This function is triggered by PollUntilContextCancel, hence we always return
// false for the condition and nil for the error, to permit it to continue polling.
// It obtains a read lock in order to check which watch IDs have progress
// notifications enabled. It then writes one message for each watch to the
// dispatch channel for the main watcher goroutine to handle sending back
// to the watcher client. If all watches have progress notifications enabled,
// instead of sending multiple messages, it sends a broadcast message.
// Note that this function is also used for on-demand progress requests.
func (w *watcher) ReportProgressOnInterval(DbLatestRevision func() (int64, error)) func(ctx context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		// get latest revision from local db
		revision, err := DbLatestRevision()
		if err != nil {
			fmt.Printf("Failed to get latest revision for ReportProgressOnInterval: %v", err)
			// always return condition=false, err=nil
			return false, nil
		}

		// create array of watchIDs to send to
		progressWatchIDs := make([]int64, 0)
		broadcast := true

		// get a read lock on the watcher to ensure inbox channel is not closed
		// release at the end of the function
		w.RLock()
		defer w.RUnlock()

		// check that inbox channel is not closed
		if !w.inboxOk {
			return false, nil
		}

		// determine which watch IDs have progress notifications enabled.
		// set broadcast to false if any of the watches have progress notifications
		// disabled.
		// obtain read lock, iterate on progress map, then release lock immediately
		for watchID, progressNotify := range w.progress {
			if progressNotify {
				progressWatchIDs = append(progressWatchIDs, watchID)
			} else {
				broadcast = false
			}
		}

		if broadcast {
			// send a single watch response to the dispatch channel
			w.inboxCh <- pb.WatchResponse{
				Header: &pb.ResponseHeader{
					Revision: revision,
				},
				// using an invalid watch ID makes it a broadcast
				WatchId: clientv3.InvalidWatchID,
			}
		} else {
			// send a watch response for each watch ID to the dispatch channel
			for _, watchID := range progressWatchIDs {
				w.inboxCh <- pb.WatchResponse{
					Header: &pb.ResponseHeader{
						Revision: revision,
					},
					WatchId: watchID,
				}
			}
		}

		// always return condition=false, err=nil
		return false, nil
	}
}

// Distribute is a handler for distributing new Kv records to watchers
// It's invoked from a separate go routine (i.e. in wsclient),
// and lives here as it needs to obtain the read lock on allWatchers and
// each of their watches, to check for matches and send on match.
func (cs *ClientAPIServer) Distribute(record *proto.Record, prevRecord *proto.Record) {
	if record == nil {
		return
	}

	eventType := mvccpb.PUT
	if record.Deleted {
		eventType = mvccpb.DELETE
	}

	// note: WatchId is set in the watches loop (below), this is a msg template
	msg := pb.WatchResponse{
		Header: &pb.ResponseHeader{
			Revision: record.Revision,
		},
		Events: []*mvccpb.Event{
			{
				Type: eventType,
				Kv: &mvccpb.KeyValue{
					Key:            record.Key,
					CreateRevision: record.CreateRevision,
					ModRevision:    record.Revision,
					Version:        record.Version,
					Value:          record.Value,
					Lease:          record.Lease,
				},
			},
		},
	}

	// note: this value will not be set if prevRecord has already
	// been compacted. we also do not set on the msg struct
	// directly as not all watches will request prev_kv=true.
	var msgPrevKv *mvccpb.KeyValue
	if prevRecord != nil {
		msgPrevKv = &mvccpb.KeyValue{
			Key:            prevRecord.Key,
			CreateRevision: prevRecord.CreateRevision,
			ModRevision:    prevRecord.Revision,
			Version:        prevRecord.Version,
			Value:          prevRecord.Value,
			Lease:          prevRecord.Lease,
		}
	}

	// obtain read lock on allWatchers
	allWatchers.RLock()
	defer allWatchers.RUnlock()

	// loop over all watchers
	for _, w := range allWatchers.servers {
		// obtain lock for all watcher watches
		w.RLock()
		defer w.RUnlock()
		// send to all watches that should receive the record
		for watchID, watch := range w.watches {
			if isWatchMatch(watch, record) {
				msg.WatchId = watchID
				if watch.prevKv {
					if msgPrevKv != nil {
						msg.Events[0].PrevKv = msgPrevKv
					} else {
						// For newly created keys, provide empty previous KeyValue
						// to satisfy kube-apiserver's expectation of non-nil PrevKv
						msg.Events[0].PrevKv = &mvccpb.KeyValue{
							Key: record.Key,
						}
					}
				} else {
					msg.Events[0].PrevKv = nil
				}
				w.inboxCh <- msg
			}
		}
	}
}

// isWatchMatch checks if a watch should be sent a record based on its filters properties
func isWatchMatch(w watch, record *proto.Record) bool {
	// ignore put actions if 'noPut' filter is set
	if w.filtersNoPut && !record.Deleted {
		return false
	}

	// ignore delete actions if 'noDelete' filter is set
	if w.filtersNoDelete && record.Deleted {
		return false
	}

	// ignore if revision is greater than watch startRevision
	if w.startRevision > record.Revision {
		return false
	}

	// match if key is 'in range'
	if isInRange(record.Key, w.key, w.rangeEnd) {
		return true
	}

	// default to false
	return false
}

// isInRange checks if a key is in the range e.g. of a watch
func isInRange(key []byte, rangeKey []byte, rangeEnd []byte) bool {
	// determine case (similar to etcdapi_kv_range.go Range)
	zeroByte := []byte{0}
	rangeKeyAndZeroByte := append(rangeKey, byte(0))
	var rangeEndPrefixValue []byte
	if len(key) > 0 {
		rangeKeyCopy := make([]byte, len(rangeKey))
		copy(rangeKeyCopy, rangeKey)
		rangeEndPrefixValue = append(
			rangeKeyCopy[:len(rangeKeyCopy)-1],
			rangeKeyCopy[len(rangeKeyCopy)-1]+1,
		)
	}
	if len(rangeEnd) == 0 || bytes.Equal(rangeEnd, rangeKeyAndZeroByte) {
		// check for exact match
		if bytes.Equal(key, bytes.TrimRight(rangeKey, "\x00")) {
			return true
		}
	} else if bytes.Equal(rangeKey, zeroByte) && bytes.Equal(rangeEnd, zeroByte) {
		// both keys are zero bytes, true for all keys
		return true
	} else if bytes.Equal(rangeEnd, zeroByte) {
		// rangeEnd is zero bytes, true for all keys greater than or equal to
		// range key
		// key_blob >= r.Key
		if bytes.Compare(key, rangeKey) >= 0 {
			// key=abc, rangeKey=abd, compare=-1 ("key less than rangeKey")
			// key=abc, rangeKey=abc, compare=0 ("key equal to rangeKey")
			// key=abc, rangeKey=abb, compare=1 ("key greater than rangeKey")
			return true
		}
	} else if rangeEndPrefixValue != nil && bytes.Equal(rangeEnd, rangeEndPrefixValue) {
		// check if key matches prefix, where rangeKey is the prefix
		if bytes.HasPrefix(key, rangeKey) {
			return true
		}
	} else {
		// range; check if key is greater than or equal to rangeKey, and less than rangeEnd
		// key_blob >= r.Key
		// AND key_blob < r.RangeEnd
		if bytes.Compare(key, rangeKey) >= 0 && bytes.Compare(key, rangeEnd) < 0 {
			// key=abc, rangeKey=abd, compare=-1 ("key less than rangeKey")
			// key=abc, rangeKey=abc, compare=0 ("key equal to rangeKey")
			// key=abc, rangeKey=abb, compare=1 ("key greater than rangeKey")
			return true
		}
	}
	return false
}
