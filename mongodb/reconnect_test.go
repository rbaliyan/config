package mongodb_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/mongodb"
)

// mongoReconnectSeq disambiguates per-test collection names so the
// reconnect test can run alongside the other mongo integration tests.
var mongoReconnectSeq atomic.Int64

// TestMongoDBStore_Watch_SurvivesChangeStreamInterruption verifies that
// when the server-side change-stream cursor is forcibly terminated
// (simulating a network blip, a primary step-down, or a cursor timeout),
// the store's watch loop re-establishes the change stream and subsequent
// Set operations continue to deliver events on the existing Watch channel.
//
// This mirrors postgres/reconnect_test.go
// (TestPostgresStore_Watch_SurvivesListenerDisconnect): it synchronizes on
// observed events rather than fixed sleeps, and proves end-to-end recovery
// instead of mocking the reconnect.
//
// Interruption mechanism & limitation: we run the admin command
// `killAllSessions: []`, which terminates every server session in the
// deployment — including the session backing the change-stream cursor.
// The driver's `stream.Next` then returns an error, the store's watch loop
// closes the dead stream and opens a fresh one (subject to the store's
// reconnect backoff, default 5s). We deliberately do NOT attempt a true
// replica-set primary step-down (`replSetStepDown`): on the single-node
// CI replica set there is no secondary to promote, so a step-down would
// leave the set without a primary and wedge the test. Killing the cursor
// session exercises the same store recovery path (error -> re-Watch)
// without that harness constraint.
func TestMongoDBStore_Watch_SurvivesChangeStreamInterruption(t *testing.T) {
	probeMongoOnce(t)

	seq := mongoReconnectSeq.Add(1)
	coll := fmt.Sprintf("entries_reconn_%d_%d", time.Now().UnixNano(), seq)

	client, err := mongo.Connect(options.Client().ApplyURI(getMongoURI()))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = client.Database("config_test").Collection(coll).Drop(cleanupCtx)
		_ = client.Disconnect(cleanupCtx)
	})

	store := mongodb.NewStore(client,
		mongodb.WithDatabase("config_test"),
		mongodb.WithCollection(coll),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(context.Background()) })

	watchCh, err := store.Watch(ctx, config.WatchFilter{Namespaces: []string{"reconn"}})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	// Establish the watch is live BEFORE interrupting it. Change streams
	// are not guaranteed to be registered the instant Watch returns, so we
	// retry a probe write until one of its events is delivered, rather than
	// sleeping a fixed amount.
	if !mongoWatchReady(ctx, t, store, watchCh, "reconn") {
		t.Fatal("change stream never became ready (no probe event delivered)")
	}

	// Sanity: a pre-kill Set must produce a watch event.
	if _, err := store.Set(ctx, "reconn", "before", config.NewValue("v1")); err != nil {
		t.Fatalf("pre-kill Set: %v", err)
	}
	if !waitForMongoChangeEvent(watchCh, "before", 10*time.Second) {
		t.Fatal("no watch event for pre-kill Set")
	}

	// Forcibly terminate all server sessions, killing the change-stream
	// cursor and forcing the store's watch loop to re-establish the stream.
	if err := client.Database("admin").RunCommand(ctx, bson.D{
		{Key: "killAllSessions", Value: bson.A{}},
	}).Err(); err != nil {
		// killAllSessions can transiently report the very session it just
		// killed; that is expected and not a test failure.
		t.Logf("killAllSessions returned (often expected): %v", err)
	}

	// After the interruption, a new Set must still deliver an event. We
	// retry because (a) the store's reconnect backoff (default 5s) means
	// the first post-kill write can land before the new stream is open and
	// (b) the very first command after a session kill may itself error.
	// The change stream resumes from where it left off, so once the new
	// stream is live the buffered/subsequent writes are delivered.
	delivered := false
	deadline := time.Now().Add(40 * time.Second)
	for attempt := 1; time.Now().Before(deadline) && !delivered; attempt++ {
		key := fmt.Sprintf("after-%d", attempt)
		if _, err := store.Set(ctx, "reconn", key, config.NewValue("v2")); err != nil {
			t.Logf("post-kill Set attempt %d failed: %v", attempt, err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if waitForMongoChangeEvent(watchCh, key, 6*time.Second) {
			delivered = true
		}
	}
	if !delivered {
		t.Fatal("no watch event delivered after change-stream interruption")
	}
}

// mongoWatchReady issues probe writes until one of their events is
// observed on ch, proving the change stream is live. Probe events are
// drained so they cannot be mistaken for later assertions.
func mongoWatchReady(ctx context.Context, t *testing.T, store *mongodb.Store, ch <-chan config.ChangeEvent, ns string) bool {
	t.Helper()
	for attempt := 0; attempt < 60; attempt++ {
		probe := fmt.Sprintf("__ready_%d", attempt)
		if _, err := store.Set(ctx, ns, probe, config.NewValue(attempt)); err != nil {
			t.Fatalf("readiness probe Set: %v", err)
		}
		select {
		case ev, ok := <-ch:
			if !ok {
				return false
			}
			if len(ev.Key) >= len("__ready_") && ev.Key[:len("__ready_")] == "__ready_" {
				return true
			}
		case <-time.After(250 * time.Millisecond):
			// retry with a fresh probe
		case <-ctx.Done():
			return false
		}
	}
	return false
}

// waitForMongoChangeEvent drains ch until wantKey is observed or timeout
// elapses. Returns true on success.
func waitForMongoChangeEvent(ch <-chan config.ChangeEvent, wantKey string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case ev, ok := <-ch:
			if !ok {
				return false
			}
			if ev.Key == wantKey {
				return true
			}
		case <-time.After(100 * time.Millisecond):
			// keep polling until deadline
		}
	}
	return false
}
