package sqlite_test

import (
	"context"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"database/sql"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/internal/storetest"
	"github.com/rbaliyan/config/sqlite"
)

// TestSQLite_NamespaceListerConformance runs the shared [storetest] suite
// against the SQLite store. Each subtest gets a fresh in-memory database
// so the conformance scenarios stay independent and can run in parallel.
func TestSQLite_NamespaceListerConformance(t *testing.T) {
	storetest.RunNamespaceListerSuite(t, func(t *testing.T) config.Store {
		db, err := sql.Open("sqlite", ":memory:")
		if err != nil {
			t.Fatalf("open sqlite: %v", err)
		}
		// SQLite's `:memory:` is per-connection — different connections from
		// the database/sql pool see different empty DBs. Pin the pool to a
		// single connection so the table created on Connect remains visible
		// for the duration of the test. Production callers either set
		// cache=shared in the DSN or use a real file; the conformance
		// suite picks the simplest reliable form.
		db.SetMaxOpenConns(1)

		s := sqlite.NewStore(db, sqlite.WithTable("config_entries_nstest"))

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.Connect(ctx); err != nil {
			_ = db.Close()
			t.Fatalf("connect: %v", err)
		}
		t.Cleanup(func() {
			_ = s.Close(context.Background())
			_ = db.Close()
		})
		return s
	})
}
