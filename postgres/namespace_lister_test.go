package postgres_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lib/pq"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/internal/storetest"
	"github.com/rbaliyan/config/postgres"
)

// nsListerSeq disambiguates the per-test table names so parallel subtests
// of the conformance suite cannot accidentally share rows.
var nsListerSeq atomic.Int64

// TestPostgres_NamespaceListerConformance runs the shared [storetest] suite
// against the PostgreSQL store. Each subtest gets its own table so the
// scenarios stay independent under t.Parallel().
func TestPostgres_NamespaceListerConformance(t *testing.T) {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://config_test:config_test@localhost:5433/config_test?sslmode=disable"
	}
	// Probe once up front so we report "PostgreSQL not available" cleanly
	// rather than failing every subtest.
	probeCtx, probeCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer probeCancel()
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Skipf("postgres open: %v", err)
	}
	if err := db.PingContext(probeCtx); err != nil {
		_ = db.Close()
		t.Skipf("postgres ping: %v", err)
	}
	_ = db.Close()

	storetest.RunNamespaceListerSuite(t, func(t *testing.T) config.Store {
		seq := nsListerSeq.Add(1)
		table := fmt.Sprintf("config_entries_nstest_%d_%d", time.Now().UnixNano(), seq)
		channel := fmt.Sprintf("config_changes_nstest_%d", seq)

		db, err := sql.Open("postgres", dsn)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		listener := pq.NewListener(dsn, 10*time.Second, time.Minute, func(pq.ListenerEventType, error) {})
		s := postgres.NewStore(db, listener,
			postgres.WithTable(table),
			postgres.WithNotifyChannel(channel),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := s.Connect(ctx); err != nil {
			_ = listener.Close()
			_ = db.Close()
			t.Fatalf("connect: %v", err)
		}

		t.Cleanup(func() {
			cleanupCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
			defer c()
			// Drop the per-test table so a stuck integration test does
			// not leak rows into the next run. The table name was
			// validated by the store on Connect.
			_, _ = db.ExecContext(cleanupCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s", table)) // #nosec G201 -- table name validated
			_ = s.Close(cleanupCtx)
			_ = listener.Close()
			_ = db.Close()
		})
		return s
	})
}
