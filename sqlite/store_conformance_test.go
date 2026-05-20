package sqlite_test

import (
	"context"
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/internal/storetest"
	"github.com/rbaliyan/config/sqlite"
)

// sqliteFactory builds a fresh, connected, empty sqlite store via an
// in-memory database. Each subtest gets its own database; t.Cleanup
// closes both the store and the *sql.DB.
//
// SetMaxOpenConns(1) is required so the in-memory database is shared
// across the store's pool: sqlite ":memory:" databases are per-connection
// by default, and Connect's schema DDL would otherwise land on a separate
// in-memory DB than Get/Set later.
func sqliteFactory(t *testing.T) config.Store {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	db.SetMaxOpenConns(1)

	s := sqlite.NewStore(db)
	ctx := context.Background()
	if err := s.Connect(ctx); err != nil {
		_ = db.Close()
		t.Fatalf("sqlite.Connect: %v", err)
	}
	t.Cleanup(func() {
		_ = s.Close(ctx)
		_ = db.Close()
	})
	return s
}

func TestSQLite_StoreConformance(t *testing.T) {
	storetest.RunStoreConformanceSuite(t, sqliteFactory)
}

func TestSQLite_BulkStoreConformance(t *testing.T) {
	storetest.RunBulkStoreSuite(t, sqliteFactory)
}

// VersionedStore is not implemented by the sqlite backend; the suite
// will skip every subtest. Wire it anyway so the day the backend opts
// in, every subtest runs without a code change.
func TestSQLite_VersionedStoreConformance(t *testing.T) {
	storetest.RunVersionedStoreSuite(t, sqliteFactory)
}
