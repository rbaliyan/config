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

// pgConformanceSeq disambiguates the per-test table names so parallel
// subtests cannot share rows.
var pgConformanceSeq atomic.Int64

func pgDSN() string {
	if v := os.Getenv("POSTGRES_DSN"); v != "" {
		return v
	}
	return "postgres://config_test:config_test@localhost:5433/config_test?sslmode=disable"
}

// probePostgresOnce skips the parent test when PostgreSQL is unreachable.
// Per-subtest factories then assume connectivity, avoiding the 3s probe
// timeout per subtest when CI runs without a postgres container.
func probePostgresOnce(t *testing.T) {
	t.Helper()
	probeCtx, probeCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer probeCancel()
	db, err := sql.Open("postgres", pgDSN())
	if err != nil {
		t.Skipf("postgres open: %v", err)
	}
	if err := db.PingContext(probeCtx); err != nil {
		_ = db.Close()
		t.Skipf("postgres ping: %v", err)
	}
	_ = db.Close()
}

// pgFactory builds a fresh, connected, empty postgres store on a
// uniquely-named table for each invocation. Assumes [probePostgresOnce]
// has already verified connectivity at the parent test level.
func pgFactory(t *testing.T) config.Store {
	t.Helper()
	dsn := pgDSN()

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("postgres open: %v", err)
	}

	seq := pgConformanceSeq.Add(1)
	table := fmt.Sprintf("config_entries_conf_%d_%d", time.Now().UnixNano(), seq)
	channel := fmt.Sprintf("config_changes_conf_%d", seq)

	listener := pq.NewListener(dsn, 10*time.Second, time.Minute, func(pq.ListenerEventType, error) {})
	s := postgres.NewStore(db, listener,
		postgres.WithTable(table),
		postgres.WithNotifyChannel(channel),
	)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
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
		// not leak rows into the next run.
		_, _ = db.ExecContext(cleanupCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s", table)) // #nosec G201 -- table name validated
		_ = s.Close(cleanupCtx)
		_ = listener.Close()
		_ = db.Close()
	})
	return s
}

// TestPostgres_StoreConformance runs the shared [config.Store] suite.
// Locks the universal contract; the per-postgres tests in store_test.go
// keep their backend-specific coverage (LISTEN/NOTIFY behavior, COLLATE
// "C" index, etc.).
func TestPostgres_StoreConformance(t *testing.T) {
	probePostgresOnce(t)
	storetest.RunStoreConformanceSuite(t, pgFactory)
}

// TestPostgres_BulkStoreConformance runs the shared [config.BulkStore]
// suite.
func TestPostgres_BulkStoreConformance(t *testing.T) {
	probePostgresOnce(t)
	storetest.RunBulkStoreSuite(t, pgFactory)
}

// TestPostgres_VersionedStoreConformance is wired so the day the
// postgres backend opts into VersionedStore, every subtest runs
// without a code change. Today the subtests skip uniformly.
func TestPostgres_VersionedStoreConformance(t *testing.T) {
	probePostgresOnce(t)
	storetest.RunVersionedStoreSuite(t, pgFactory)
}
