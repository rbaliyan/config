package postgres_test

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/lib/pq"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/postgres"
)

// The universal [config.Store] / [config.BulkStore] contract is exercised by
// the shared suites in store_conformance_test.go. This file keeps the
// PostgreSQL-specific tests: Watch (LISTEN/NOTIFY timing) and SecretValue
// (deeper than the conformance suite: SecretFrom recovery + version
// increment).

func getPostgresDSN() string {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://localhost:5432/config_test?sslmode=disable"
	}
	return dsn
}

func skipIfNoPostgres(t *testing.T) (*postgres.Store, *sql.DB, *pq.Listener) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dsn := getPostgresDSN()

	// Create database connection (app's responsibility)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Skipf("PostgreSQL not available: %v", err)
	}

	// Verify connection
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		t.Skipf("PostgreSQL not available: %v", err)
	}

	// Create listener for LISTEN/NOTIFY (app's responsibility)
	listener := pq.NewListener(dsn, 10*time.Second, time.Minute, func(ev pq.ListenerEventType, err error) {
		// Log errors but don't fail
	})

	// Create store with the db and listener
	store := postgres.NewStore(db, listener,
		postgres.WithTable("config_entries_test"),
		postgres.WithNotifyChannel("config_changes_test"),
	)

	if err := store.Connect(ctx); err != nil {
		listener.Close()
		db.Close()
		t.Skipf("Store connect failed: %v", err)
	}

	return store, db, listener
}

func TestPostgresStore_Watch(t *testing.T) {
	store, db, listener := skipIfNoPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	defer listener.Close()
	defer db.Close()
	defer store.Close(ctx)

	// Start watching
	changes, err := store.Watch(ctx, config.WatchFilter{
		Namespaces: []string{"watchtest"},
	})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}

	// Wait until the LISTEN/NOTIFY subscription is actually live by probing
	// with throwaway writes and draining their events, rather than sleeping
	// a fixed amount and hoping the listener registered in time.
	waitForWatchReady(t, store, changes, "watchtest")

	// Set a value
	value := config.NewValue("test", config.WithValueType(config.TypeString))
	if _, err := store.Set(ctx, "watchtest", "watched/key", value); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Wait for change event
	select {
	case event := <-changes:
		if event.Type != config.ChangeTypeSet {
			t.Errorf("Expected ChangeTypeSet, got %v", event.Type)
		}
		if event.Key != "watched/key" {
			t.Errorf("Expected key 'watched/key', got %s", event.Key)
		}
	case <-ctx.Done():
		t.Fatal("Timeout waiting for change event")
	}

	// Cleanup
	_ = store.Delete(ctx, "watchtest", "watched/key")
}

// TestPostgresStore_SecretValue goes deeper than the shared conformance
// suite's SecretValue subtest: PostgreSQL's Get returns a *val type
// (unlike memory's metadataValue wrapper), so SecretFrom can recover the
// original bytes. This test pins that round-trip AND the version-increment
// behavior on rotation.
func TestPostgresStore_SecretValue(t *testing.T) {
	store, db, listener := skipIfNoPostgres(t)
	ctx := context.Background()
	defer listener.Close()
	defer db.Close()
	defer store.Close(ctx)

	const ns, key = "secrettest", "creds/db_password"
	defer func() { _ = store.Delete(ctx, ns, key) }()

	original := config.NewSecret("super-secret-pw")
	written, err := store.Set(ctx, ns, key, config.NewSecretValue(original))
	if err != nil {
		t.Fatalf("Set: %v", err)
	}
	if written.Type() != config.TypeSecret {
		t.Errorf("Set returned type %v, want TypeSecret", written.Type())
	}

	retrieved, err := store.Get(ctx, ns, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	// Type must survive the round-trip.
	if retrieved.Type() != config.TypeSecret {
		t.Errorf("retrieved type = %v, want TypeSecret", retrieved.Type())
	}

	// String() must always mask — plaintext must never leak.
	str, err := retrieved.String()
	if err != nil {
		t.Fatalf("String(): %v", err)
	}
	if str != "******" {
		t.Errorf("String() = %q, want \"******\" (plaintext leaked)", str)
	}

	// SecretFrom must recover the original bytes.
	got, err := config.SecretFrom(ctx, retrieved)
	if err != nil {
		t.Fatalf("SecretFrom: %v", err)
	}
	defer got.Wipe()
	if !original.Equal(got) {
		t.Errorf("SecretFrom bytes mismatch after round-trip: got %q", got.Bytes())
	}

	// Version increments on update.
	if _, err := store.Set(ctx, ns, key, config.NewSecretValue(config.NewSecret("rotated-secret"))); err != nil {
		t.Fatalf("update Set: %v", err)
	}
	updated, err := store.Get(ctx, ns, key)
	if err != nil {
		t.Fatalf("Get after update: %v", err)
	}
	if updated.Metadata().Version() != 2 {
		t.Errorf("version after update = %d, want 2", updated.Metadata().Version())
	}
	// Updated value must also be masked.
	if s, _ := updated.String(); s != "******" {
		t.Errorf("updated String() = %q, want \"******\"", s)
	}
}

// Compile-time interface checks
var (
	_ config.Store         = (*postgres.Store)(nil)
	_ config.HealthChecker = (*postgres.Store)(nil)
	_ config.StatsProvider = (*postgres.Store)(nil)
	_ config.BulkStore     = (*postgres.Store)(nil)
)
