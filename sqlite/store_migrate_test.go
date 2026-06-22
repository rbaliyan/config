package sqlite_test

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/sqlite"
)

// createLegacyTable builds a config table WITHOUT the expires_at column,
// simulating a schema from before TTL support was added.
func createLegacyTable(t *testing.T, db *sql.DB, table string) {
	t.Helper()
	_, err := db.ExecContext(context.Background(), `
		CREATE TABLE `+table+` (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			key TEXT NOT NULL,
			namespace TEXT NOT NULL,
			value BLOB NOT NULL,
			codec TEXT NOT NULL DEFAULT 'json',
			type INTEGER NOT NULL DEFAULT 0,
			version INTEGER NOT NULL DEFAULT 1,
			created_at TEXT NOT NULL DEFAULT (datetime('now')),
			updated_at TEXT NOT NULL DEFAULT (datetime('now')),
			UNIQUE(namespace, key)
		)
	`)
	if err != nil {
		t.Fatalf("create legacy table: %v", err)
	}
}

// TestMigrateAddTTL_OnLegacyTable verifies MigrateAddTTL adds the expires_at
// column to a legacy table that lacks it. SQLite does not support
// `ADD COLUMN IF NOT EXISTS`, so the migration guards with PRAGMA table_info
// and issues a plain ADD COLUMN; it must also be idempotent.
func TestMigrateAddTTL_OnLegacyTable(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	const table = "legacy_cfg"
	createLegacyTable(t, db, table)
	if columnExists(t, db, table, "expires_at") {
		t.Fatal("legacy table unexpectedly already has expires_at")
	}

	store := sqlite.NewStore(db, sqlite.WithTable(table), sqlite.WithWAL(false))

	if err := store.MigrateAddTTL(ctx); err != nil {
		t.Fatalf("MigrateAddTTL on legacy table: %v", err)
	}
	if !columnExists(t, db, table, "expires_at") {
		t.Fatal("MigrateAddTTL reported success but expires_at column is absent")
	}

	// Idempotent: a second call must not fail (column already present).
	if err := store.MigrateAddTTL(ctx); err != nil {
		t.Fatalf("second MigrateAddTTL (idempotency): %v", err)
	}
}

func TestMigrateAddTTL_OnTableWithColumn(t *testing.T) {
	// A table created via Connect already has expires_at; MigrateAddTTL must be
	// a successful no-op rather than erroring on a duplicate column.
	ctx := context.Background()
	store, _ := newTestStore(t)

	if err := store.MigrateAddTTL(ctx); err != nil {
		t.Fatalf("MigrateAddTTL on table that already has expires_at: %v", err)
	}
}

func columnExists(t *testing.T, db *sql.DB, table, column string) bool {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), "PRAGMA table_info("+table+")")
	if err != nil {
		t.Fatalf("pragma table_info: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			cid        int
			name       string
			ctype      string
			notnull    int
			dfltValue  sql.NullString
			primaryKey int
		)
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dfltValue, &primaryKey); err != nil {
			t.Fatalf("scan table_info: %v", err)
		}
		if name == column {
			return true
		}
	}
	return false
}

func TestSQLiteOptionSetters(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	var dropped []config.ChangeEvent
	onDropped := func(e config.ChangeEvent) { dropped = append(dropped, e) }
	logger := slog.New(slog.NewTextHandler(nopWriter{}, nil))

	// Exercise WithConfig, WithWAL, WithOnDropped, WithLogger together.
	store := sqlite.NewStore(db,
		sqlite.WithConfig(sqlite.Config{
			Table:           "opt_table",
			WatchBufferSize: 5,
			EnableWAL:       false,
		}),
		sqlite.WithWAL(true),
		sqlite.WithOnDropped(onDropped),
		sqlite.WithLogger(logger),
	)
	ctx := context.Background()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(ctx) })

	// Basic sanity that the configured table is usable.
	if _, err := store.Set(ctx, "ns", "k", config.NewValue("v")); err != nil {
		t.Fatalf("Set on configured store: %v", err)
	}

	// DroppedEvents starts at zero before any overflow.
	if got := store.DroppedEvents(); got != 0 {
		t.Errorf("DroppedEvents = %d, want 0 initially", got)
	}
	_ = dropped // onDropped wiring is covered by the dedicated drop test below
}

func TestSQLiteDroppedEvents_OverflowInvokesCallback(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	dropCh := make(chan config.ChangeEvent, 64)
	store := sqlite.NewStore(db,
		sqlite.WithTable("drop_table"),
		sqlite.WithWatchBufferSize(1), // tiny buffer so events overflow quickly
		sqlite.WithOnDropped(func(e config.ChangeEvent) { dropCh <- e }),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(context.Background()) })

	// Subscribe but never drain, so a burst of writes overflows the buffer.
	if _, err := store.Watch(ctx, config.WatchFilter{Namespaces: []string{"burst"}}); err != nil {
		t.Fatalf("Watch: %v", err)
	}

	for i := 0; i < 50; i++ {
		key := "k" + string(rune('a'+i%26)) + string(rune('0'+i/26))
		if _, err := store.Set(ctx, "burst", key, config.NewValue(i)); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}

	// At least one event must have been dropped and the callback fired.
	select {
	case <-dropCh:
	case <-time.After(2 * time.Second):
		t.Fatal("expected onDropped callback to fire under buffer overflow")
	}
	if store.DroppedEvents() == 0 {
		t.Error("DroppedEvents() = 0, want > 0 after overflow")
	}
}

// nopWriter is an io.Writer that discards everything (for the test logger).
type nopWriter struct{}

func (nopWriter) Write(p []byte) (int, error) { return len(p), nil }
