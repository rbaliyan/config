package multi_test

// Cross-backend consistency test for the multi-store. Unlike the white-box
// unit tests in store_test.go (which compose memory stores and mock
// failStores), this exercises two *different real backends* — an in-memory
// store and a SQLite store — composed under StrategyWriteThrough, and
// asserts that both converge after Set and Delete.
//
// SQLite and memory both run without any external service, so this test
// runs in CI with no Docker. It is the closest no-services analogue to a
// "cache (memory) + durable backend (SQL)" production topology.

import (
	"context"
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/memory"
	"github.com/rbaliyan/config/multi"
	"github.com/rbaliyan/config/sqlite"
)

// newSQLiteStore returns a connected, isolated in-memory SQLite store.
// SetMaxOpenConns(1) keeps the ":memory:" database on a single shared
// connection (otherwise the schema DDL and later reads land on different
// per-connection databases).
func newSQLiteStore(t *testing.T) config.Store {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	db.SetMaxOpenConns(1)
	s := sqlite.NewStore(db)
	if err := s.Connect(context.Background()); err != nil {
		_ = db.Close()
		t.Fatalf("sqlite connect: %v", err)
	}
	t.Cleanup(func() {
		_ = s.Close(context.Background())
		_ = db.Close()
	})
	return s
}

func newMemoryStore(t *testing.T) config.Store {
	t.Helper()
	s := memory.NewStore()
	if err := s.Connect(context.Background()); err != nil {
		t.Fatalf("memory connect: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

// requireValue asserts that a single backend holds the expected string
// value for ns/key.
func requireValue(t *testing.T, s config.Store, label, ns, key, want string) {
	t.Helper()
	got, err := s.Get(context.Background(), ns, key)
	if err != nil {
		t.Fatalf("%s: Get(%s/%s): %v", label, ns, key, err)
	}
	if v, _ := got.String(); v != want {
		t.Errorf("%s: Get(%s/%s) = %q, want %q", label, ns, key, v, want)
	}
}

// requireAbsent asserts that a single backend reports ErrNotFound for
// ns/key.
func requireAbsent(t *testing.T, s config.Store, label, ns, key string) {
	t.Helper()
	if _, err := s.Get(context.Background(), ns, key); !config.IsNotFound(err) {
		t.Errorf("%s: Get(%s/%s) err = %v, want ErrNotFound", label, ns, key, err)
	}
}

// TestMulti_WriteThrough_CrossBackendConvergence composes a memory store
// (index 0, also the read primary) and a SQLite store (index 1) under
// StrategyWriteThrough, then asserts that a Set followed by a Delete leaves
// BOTH backends in the same state — proving the write actually reached the
// durable backend, not just the primary.
func TestMulti_WriteThrough_CrossBackendConvergence(t *testing.T) {
	t.Parallel()
	mem := newMemoryStore(t)
	lite := newSQLiteStore(t)

	store := multi.NewStore(
		[]config.Store{mem, lite},
		multi.WithStrategy(multi.StrategyWriteThrough),
		// Strict writes turn any replica divergence into a hard error so a
		// silently-dropped backend write fails the test instead of being
		// masked by the "one store succeeded" availability default.
		multi.WithStrictWrites(),
	)
	ctx := context.Background()

	const ns, key = "conv", "app/setting"

	// Set must land in BOTH backends.
	if _, err := store.Set(ctx, ns, key, config.NewValue("v1")); err != nil {
		t.Fatalf("Set v1: %v", err)
	}
	requireValue(t, mem, "memory", ns, key, "v1")
	requireValue(t, lite, "sqlite", ns, key, "v1")

	// Update must propagate to BOTH backends.
	if _, err := store.Set(ctx, ns, key, config.NewValue("v2")); err != nil {
		t.Fatalf("Set v2: %v", err)
	}
	requireValue(t, mem, "memory", ns, key, "v2")
	requireValue(t, lite, "sqlite", ns, key, "v2")

	// The multi-store read (primary = memory) must agree.
	got, err := store.Get(ctx, ns, key)
	if err != nil {
		t.Fatalf("multi Get: %v", err)
	}
	if v, _ := got.String(); v != "v2" {
		t.Errorf("multi Get = %q, want v2", v)
	}

	// Delete must remove the key from BOTH backends.
	if err := store.Delete(ctx, ns, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	requireAbsent(t, mem, "memory", ns, key)
	requireAbsent(t, lite, "sqlite", ns, key)

	// And the multi-store read must now miss everywhere (no stale fallback).
	if _, err := store.Get(ctx, ns, key); !config.IsNotFound(err) {
		t.Errorf("multi Get after Delete: err = %v, want ErrNotFound", err)
	}

	// No partial writes should have been recorded: every write reached
	// both backends.
	if pw := store.PartialWrites(); pw != 0 {
		t.Errorf("PartialWrites = %d, want 0 (a replica silently diverged)", pw)
	}
}

// TestMulti_WriteThrough_BulkCrossBackendConvergence does the same for the
// BulkStore path (SetMany / DeleteMany), which takes a different code path
// in the multi-store than single-key Set/Delete.
func TestMulti_WriteThrough_BulkCrossBackendConvergence(t *testing.T) {
	t.Parallel()
	mem := newMemoryStore(t)
	lite := newSQLiteStore(t)

	store := multi.NewStore(
		[]config.Store{mem, lite},
		multi.WithStrategy(multi.StrategyWriteThrough),
		multi.WithStrictWrites(),
	)
	ctx := context.Background()
	const ns = "bulkconv"

	values := map[string]config.Value{
		"a": config.NewValue("va"),
		"b": config.NewValue("vb"),
		"c": config.NewValue("vc"),
	}
	if err := store.SetMany(ctx, ns, values); err != nil {
		t.Fatalf("SetMany: %v", err)
	}
	for k, want := range map[string]string{"a": "va", "b": "vb", "c": "vc"} {
		requireValue(t, mem, "memory", ns, k, want)
		requireValue(t, lite, "sqlite", ns, k, want)
	}

	n, err := store.DeleteMany(ctx, ns, []string{"a", "b"})
	if err != nil {
		t.Fatalf("DeleteMany: %v", err)
	}
	if n < 2 {
		t.Errorf("DeleteMany reported %d deletes, want >= 2", n)
	}
	requireAbsent(t, mem, "memory", ns, "a")
	requireAbsent(t, mem, "memory", ns, "b")
	requireAbsent(t, lite, "sqlite", ns, "a")
	requireAbsent(t, lite, "sqlite", ns, "b")
	// "c" untouched in both.
	requireValue(t, mem, "memory", ns, "c", "vc")
	requireValue(t, lite, "sqlite", ns, "c", "vc")

	if pw := store.PartialWrites(); pw != 0 {
		t.Errorf("PartialWrites = %d, want 0", pw)
	}
}
