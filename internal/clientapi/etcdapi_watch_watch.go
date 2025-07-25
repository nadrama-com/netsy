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
	"fmt"
	"sync/atomic"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"k8s.io/apimachinery/pkg/util/wait"
)

// Watch is a handler for pb.Watch_WatchServer requests
// It is invoked on the creation of a new 'watcher' server, which is a gRPC
// bidirectional stream (where one kube-apiserver is the main client, though
// we may need to support multiple clients at some point).
// Note that this Watch handler is invoked on its own go routine.
// Watch loops on the gRPC stream until it receives an error, such as when
// a client disconnects or the context is cancelled.
// Watchers/clients can have multiple 'watches', and will coelesce multiple
// 'watches' on the one Watch stream e.g. a kube-apiserver will have a single
// stream but multiple 'kubectl watch' commands would be coalesced into its
// one stream.
// Each watcher has an 'inbox' channel. Watch runs a separate goroutine
// to process any incoming messages on the inbox channel and send back to
// the watcher. The inbox channel messages are expected to already be
// a WatchResponse.
func (cs *ClientAPIServer) Watch(ws pb.Watch_WatchServer) error {
	// create a globally-unique watcher ID
	watcherID := atomic.AddInt64(&watcherIDCounter, 1)

	// instantiate a new watcher
	w := &watcher{
		id:       watcherID,
		client:   ws,
		inboxOk:  true,
		inboxCh:  make(chan pb.WatchResponse), // TODO: use a buffered channel?
		watches:  map[int64]watch{},
		progress: map[int64]bool{},
	}

	// add watcher to map of all watchers
	// obtain write lock, add to map, then release lock immediately
	allWatchers.Lock()
	allWatchers.servers[watcherID] = w
	allWatchers.Unlock()

	// start a goroutine to handle messages on the inbox channel
	go func() {
		for {
			// block until next message is received
			msg, ok := <-w.inboxCh

			// end goroutine once channel is closed
			// this will happen if Cleanup is invoked (at end of Watch method)
			if !ok {
				fmt.Printf("Watch() inboxCh closed\n")
				return
			}

			// send message back to client
			// note that because this should be the only goroutine sending
			// messages to the client, we don't need to lock the watcher
			err := w.client.Send(&msg)
			if err != nil {
				fmt.Printf("Watch() Send() error: %v\n", err)
				return
			}
		}
	}()

	// we use PollUntilContextCancel to invoke progress reporting on an interval
	// it will continue until the context is cancelled or hits a deadline.
	go wait.PollUntilContextCancel(
		w.client.Context(),
		// TODO: add jitter so we don't send updates to all watchers at the same time
		time.Second*5,
		true,
		w.ReportProgressOnInterval(cs.db.LatestRevision),
	)

	// block until gRPC stream is closed
	var err error
	for {
		// wait for next message or error from gRPC stream
		var msg *pb.WatchRequest
		msg, err = w.client.Recv()
		if err != nil {
			fmt.Printf("Watch() cancelled or returning error\n")
			// end watch/exit loop when the stream has an error/is closed
			break
		}
		if cr := msg.GetCreateRequest(); cr != nil {
			// handle watch create request
			latestRevision, _ := cs.db.LatestRevision()
			w.CreateWatch(cr, latestRevision, cs.db.GetRevision)
		}
		if cr := msg.GetCancelRequest(); cr != nil {
			// handle watch cancel request
			revision, _ := cs.db.LatestRevision()
			w.CancelWatch(cr.WatchId, revision, nil)
		}
		if pr := msg.GetProgressRequest(); pr != nil {
			// handle watch progress request
			w.ReportProgressOnInterval(cs.db.LatestRevision)(w.client.Context())
		}
	}

	// if above loop has exited, it means the stream is closed, so cleanup
	w.Cleanup(watcherID)
	return err
}
