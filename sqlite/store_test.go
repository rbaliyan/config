package sqlite_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/sqlite"
)

// The universal [config.Store] / [config.BulkStore] contract is exercised by
// the shared suites in store_conformance_test.go. This file keeps the SQLite-
// specific tests: Watch (timing differs from other backends), SecretValue
// (deeper than the conformance suite: SecretFrom recovery + version
// increment), and ExpiredEntryTreatedAsAbsent (write/read symmetry on
// past-TTL rows that is unique to the SQLite TTL implementation).

func newTestStore(t *testing.T) (*sqlite.Store, *sql.DB) {
	t.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open SQLite: %v", err)
	}

	store := sqlite.NewStore(db, sqlite.WithTable("config_entries_test"))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := store.Connect(ctx); err != nil {
		db.Close()
		t.Fatalf("Store connect failed: %v", err)
	}

	t.Cleanup(func() {
		store.Close(context.Background())
		db.Close()
	})

	return store, db
}

func TestSQLiteStore_Watch(t *testing.T) {
	store, _ := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Start watching
	changes, err := store.Watch(ctx, config.WatchFilter{
		Namespaces: []string{"watchtest"},
	})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}

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
}

// TestSQLiteStore_SecretValue goes deeper than the shared conformance
// suite's SecretValue subtest: SQLite's Get returns a *val type (unlike
// memory's metadataValue wrapper), so SecretFrom can recover the original
// bytes. This test pins that round-trip AND the version-increment behavior
// on rotation — both behaviors the conformance suite cannot assume.
func TestSQLiteStore_SecretValue(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	const ns, key = "secrettest", "creds/db_password"

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

// TestSQLiteStore_ExpiredEntryTreatedAsAbsent verifies write semantics
// align with read semantics for entries past their TTL.
func TestSQLiteStore_ExpiredEntryTreatedAsAbsent(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	// Seed with a TTL just in the future, then wait past it. SQLite's
	// datetime() function is second-resolution, so use a buffer of >1s.
	soonExpiry := time.Now().UTC().Add(1 * time.Second)
	val := config.NewValue("stale", config.WithValueExpiresAt(soonExpiry))
	if _, err := store.Set(ctx, "ns", "k", val); err != nil {
		t.Fatalf("seed Set: %v", err)
	}
	time.Sleep(2200 * time.Millisecond)

	if _, err := store.Get(ctx, "ns", "k"); !config.IsNotFound(err) {
		t.Errorf("Get on expired entry: want NotFound, got %v", err)
	}

	upd := config.NewValue("late", config.WithValueWriteMode(config.WriteModeUpdate))
	if _, err := store.Set(ctx, "ns", "k", upd); !config.IsNotFound(err) {
		t.Errorf("Update on expired entry: want NotFound, got %v", err)
	}

	create := config.NewValue("fresh", config.WithValueWriteMode(config.WriteModeCreate))
	if _, err := store.Set(ctx, "ns", "k", create); err != nil {
		t.Fatalf("Create over expired entry: %v", err)
	}

	got, err := store.Get(ctx, "ns", "k")
	if err != nil {
		t.Fatalf("Get after takeover: %v", err)
	}
	gs, _ := got.String()
	if gs != "fresh" {
		t.Errorf("post-takeover value = %q, want %q", gs, "fresh")
	}
}

// Compile-time interface checks
var (
	_ config.Store         = (*sqlite.Store)(nil)
	_ config.HealthChecker = (*sqlite.Store)(nil)
	_ config.StatsProvider = (*sqlite.Store)(nil)
	_ config.BulkStore     = (*sqlite.Store)(nil)
)
