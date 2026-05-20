package postgres_test

import (
	"context"
	"database/sql"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lib/pq"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/postgres"
)

// reconnectSeq disambiguates the per-test table + channel names so the
// reconnection tests can run alongside other postgres tests without
// stepping on each other's NOTIFY traffic.
var reconnectSeq atomic.Int64

// TestPostgresStore_Watch_SurvivesListenerDisconnect verifies that when
// pq.Listener's underlying connection is terminated by the server
// (simulating a network blip, a backend kill, or a Postgres restart),
// the listener reconnects and subsequent Set operations continue to
// deliver change events through the existing Watch channel.
//
// This catches the class of bug that mocks always miss — the no-op
// callback historically passed to pq.NewListener throws away the only
// signal a test would get for "reconnect happened" so we have to
// observe the end-to-end behavior instead.
func TestPostgresStore_Watch_SurvivesListenerDisconnect(t *testing.T) {
	dsn := getPostgresDSN()

	// Probe connectivity up front. Skip cleanly when CI runs without a
	// postgres service container.
	probeCtx, probeCancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer probeCancel()
	if probe, err := sql.Open("postgres", dsn); err != nil {
		t.Skipf("postgres open: %v", err)
	} else if err := probe.PingContext(probeCtx); err != nil {
		_ = probe.Close()
		t.Skipf("postgres ping: %v", err)
	} else {
		_ = probe.Close()
	}

	seq := reconnectSeq.Add(1)
	table := fmt.Sprintf("config_entries_reconn_%d_%d", time.Now().UnixNano(), seq)
	channel := fmt.Sprintf("config_changes_reconn_%d", seq)

	// adminDB is used to issue pg_terminate_backend; using a separate
	// connection means the kill survives even if the listener takes the
	// store's main *sql.DB connection down too.
	adminDB, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("admin open: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_, _ = adminDB.ExecContext(cleanupCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s", table)) // #nosec G201
		_ = adminDB.Close()
	})

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("store db open: %v", err)
	}

	// Surface every listener lifecycle event into a buffered channel so the
	// test can wait for the reconnect notification rather than time-polling.
	listenerEvents := make(chan pq.ListenerEventType, 16)
	listener := pq.NewListener(dsn, 100*time.Millisecond, time.Second,
		func(ev pq.ListenerEventType, _ error) {
			select {
			case listenerEvents <- ev:
			default:
				// drop excess events to avoid backpressure stalling the listener
			}
		})

	store := postgres.NewStore(db, listener,
		postgres.WithTable(table),
		postgres.WithNotifyChannel(channel),
	)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if err := store.Connect(ctx); err != nil {
		_ = listener.Close()
		_ = db.Close()
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close(context.Background())
		_ = listener.Close()
		_ = db.Close()
	})

	// Wait for the initial ListenerEventConnected so we know the listener's
	// underlying connection is in pg_stat_activity (and therefore killable).
	if !waitForListenerEvent(t, listenerEvents, pq.ListenerEventConnected, 5*time.Second) {
		t.Fatal("listener never reported initial Connected event")
	}

	watchCh, err := store.Watch(ctx, config.WatchFilter{Namespaces: []string{"reconn"}})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	// Sanity: pre-kill, a Set must produce a watch event. This proves the
	// listener is wired up before we start tearing it down.
	if _, err := store.Set(ctx, "reconn", "before", config.NewValue("v1")); err != nil {
		t.Fatalf("pre-kill Set: %v", err)
	}
	waitForChangeEvent(t, watchCh, "before", 5*time.Second)

	// Kill every backend connection except adminDB's own — that includes
	// the listener's session AND the store's main *sql.DB session. The
	// store's autoreconnect logic should bring both back; the test then
	// verifies the watch channel still receives events after the dust
	// settles.
	terminated := terminateOtherBackends(t, adminDB, dsn)
	t.Logf("terminated %d backend connection(s)", terminated)

	// pq.Listener reports Reconnected after a successful reconnect. We
	// also accept ConnectionAttemptFailed -> Reconnected sequences so a
	// transient socket error in the kill window doesn't fail the test.
	if !waitForListenerEvent(t, listenerEvents, pq.ListenerEventReconnected, 15*time.Second) {
		t.Fatal("listener never reported a Reconnected event within 15s")
	}

	// After reconnect, a new Set must deliver an event. We poll with
	// retries because the very first Set after a forced disconnect may
	// race the schema reconnect; subsequent writes always work.
	delivered := false
	for attempt := 1; attempt <= 5 && !delivered; attempt++ {
		key := fmt.Sprintf("after-%d", attempt)
		if _, err := store.Set(ctx, "reconn", key, config.NewValue("v2")); err != nil {
			t.Logf("post-kill Set attempt %d failed: %v", attempt, err)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if waitForChangeEventSoft(watchCh, key, 3*time.Second) {
			delivered = true
		}
	}
	if !delivered {
		t.Fatal("no watch event delivered after listener reconnected (5 attempts)")
	}
}

// terminateOtherBackends issues pg_terminate_backend against every
// connection in the current database except the one owned by adminDB
// itself. Returns the count of terminated backends so the test log
// shows the disruption was non-zero.
func terminateOtherBackends(t *testing.T, adminDB *sql.DB, _ string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// pg_terminate_backend returns boolean; we sum the successful kills.
	const q = `
		SELECT COUNT(*) FROM (
			SELECT pg_terminate_backend(pid) AS killed
			FROM pg_stat_activity
			WHERE datname = current_database()
			  AND pid <> pg_backend_pid()
			  AND backend_type = 'client backend'
		) sub
		WHERE killed
	`
	var n int
	if err := adminDB.QueryRowContext(ctx, q).Scan(&n); err != nil {
		t.Fatalf("pg_terminate_backend: %v", err)
	}
	return n
}

// waitForListenerEvent drains listenerEvents until want is observed or the
// timeout elapses. Returns true on success.
func waitForListenerEvent(t *testing.T, ch <-chan pq.ListenerEventType, want pq.ListenerEventType, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case ev := <-ch:
			if ev == want {
				return true
			}
			t.Logf("listener event %d (waiting for %d)", ev, want)
		case <-time.After(100 * time.Millisecond):
			// keep polling
		}
	}
	return false
}

// waitForChangeEvent fails the test if the named key is not delivered to
// the watch channel within timeout. Drains older events so a sanity
// pre-write does not satisfy a post-kill assertion.
func waitForChangeEvent(t *testing.T, ch <-chan config.ChangeEvent, wantKey string, timeout time.Duration) {
	t.Helper()
	if !waitForChangeEventSoft(ch, wantKey, timeout) {
		t.Fatalf("no event for key %q within %s", wantKey, timeout)
	}
}

func waitForChangeEventSoft(ch <-chan config.ChangeEvent, wantKey string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case ev := <-ch:
			if ev.Key == wantKey {
				return true
			}
		case <-time.After(100 * time.Millisecond):
			// keep polling
		}
	}
	return false
}

