// miniredis-backed unit tests for the Redis store. These run with `go test
// ./...` — no external Redis required — and close the documented unit-coverage
// gap on the Redis backend's hot paths (Get/Set/Delete/Find/Watch/BulkStore
// plus the Lua upsert/setNX/update scripts).
//
// Integration coverage against a real Redis remains in cache_test.go and any
// future store_integration_test.go; the miniredis layer is the smoke/unit
// gate that runs in CI without service containers.

package redis_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/rbaliyan/config"
	configredis "github.com/rbaliyan/config/redis"
)

// newMiniStore boots a miniredis server, points a configredis.Store at it,
// and registers Close hooks. The store is fully connected and ready for use.
//
// Each call gets a fresh server and a unique key prefix so subtests can run
// in parallel without colliding on the shared pub/sub channel.
func newMiniStore(t *testing.T) (*configredis.Store, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	store := configredis.NewStore(
		configredis.WithAddress(mr.Addr()),
		configredis.WithKeyPrefix(uniqueRedisPrefix(t)),
	)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(context.Background()) })
	return store, mr
}

var miniSeq atomic.Int64

// uniqueRedisPrefix returns a per-test key prefix that is also safe under
// `go test -count=N`. The atomic seq prevents collisions when miniredis is
// reused across `RunT` instances within the same package run.
func uniqueRedisPrefix(t *testing.T) string {
	t.Helper()
	seq := miniSeq.Add(1)
	return "test:" + t.Name() + ":" + itoa(seq)
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func TestRedisStore_Mini_RoundTrip(t *testing.T) {
	t.Parallel()
	store, _ := newMiniStore(t)
	ctx := t.Context()

	written, err := store.Set(ctx, "ns", "k", config.NewValue(42))
	if err != nil {
		t.Fatalf("Set: %v", err)
	}
	if v, _ := written.Int64(); v != 42 {
		t.Fatalf("Set returned %d, want 42", v)
	}

	got, err := store.Get(ctx, "ns", "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if v, _ := got.Int64(); v != 42 {
		t.Fatalf("Get returned %d, want 42", v)
	}
}

func TestRedisStore_Mini_UpsertIncrementsVersion(t *testing.T) {
	t.Parallel()
	store, _ := newMiniStore(t)
	ctx := t.Context()

	v1, err := store.Set(ctx, "ns", "k", config.NewValue("v1"))
	if err != nil {
		t.Fatalf("first Set: %v", err)
	}
	if v1.Metadata().Version() != 1 {
		t.Errorf("first Set version = %d, want 1", v1.Metadata().Version())
	}

	v2, err := store.Set(ctx, "ns", "k", config.NewValue("v2"))
	if err != nil {
		t.Fatalf("second Set: %v", err)
	}
	if v2.Metadata().Version() != 2 {
		t.Errorf("second Set version = %d, want 2 (Lua upsert script must increment)", v2.Metadata().Version())
	}

	// CreatedAt must be preserved across updates (Lua script copies p.ca).
	if !v1.Metadata().CreatedAt().Equal(v2.Metadata().CreatedAt()) {
		t.Errorf("CreatedAt changed across update: %v -> %v", v1.Metadata().CreatedAt(), v2.Metadata().CreatedAt())
	}
}

func TestRedisStore_Mini_NotFound(t *testing.T) {
	t.Parallel()
	store, _ := newMiniStore(t)
	ctx := t.Context()

	_, err := store.Get(ctx, "ns", "missing")
	if !errors.Is(err, config.ErrNotFound) {
		t.Fatalf("Get missing: err = %v, want ErrNotFound", err)
	}
	if !config.IsNotFound(err) {
		t.Fatalf("IsNotFound = false for missing key, err = %v", err)
	}
}

func TestRedisStore_Mini_DeleteRoundTrip(t *testing.T) {
	t.Parallel()
	store, _ := newMiniStore(t)
	ctx := t.Context()

	if _, err := store.Set(ctx, "ns", "k", config.NewValue("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := store.Delete(ctx, "ns", "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := store.Get(ctx, "ns", "k"); !config.IsNotFound(err) {
		t.Fatalf("Get after Delete: err = %v, want ErrNotFound", err)
	}
	// Deleting a missing key is an error in the documented Store contract.
	if err := store.Delete(ctx, "ns", "k"); !config.IsNotFound(err) {
		t.Fatalf("Delete missing key: err = %v, want ErrNotFound", err)
	}
}

func TestRedisStore_Mini_ConditionalWrites(t *testing.T) {
	t.Parallel()
	store, _ := newMiniStore(t)
	ctx := t.Context()

	create := config.NewValue("v1", config.WithValueWriteMode(config.WriteModeCreate))
	if _, err := store.Set(ctx, "ns", "k", create); err != nil {
		t.Fatalf("Create (initial): %v", err)
	}

	// IfNotExists on existing key must fail with ErrKeyExists.
	create2 := config.NewValue("v2", config.WithValueWriteMode(config.WriteModeCreate))
	if _, err := store.Set(ctx, "ns", "k", create2); !config.IsKeyExists(err) {
		t.Fatalf("Create on existing: err = %v, want ErrKeyExists", err)
	}

	// IfExists on existing key must succeed.
	update := config.NewValue("v3", config.WithValueWriteMode(config.WriteModeUpdate))
	if _, err := store.Set(ctx, "ns", "k", update); err != nil {
		t.Fatalf("Update on existing: %v", err)
	}

	// IfExists on missing key must fail with ErrNotFound.
	updateMissing := config.NewValue("v4", config.WithValueWriteMode(config.WriteModeUpdate))
	if _, err := store.Set(ctx, "ns", "missing", updateMissing); !config.IsNotFound(err) {
		t.Fatalf("Update on missing: err = %v, want ErrNotFound", err)
	}
}

func TestRedisStore_Mini_FindPrefix(t *testing.T) {
	t.Parallel()
	store, _ := newMiniStore(t)
	ctx := t.Context()

	for _, k := range []string{"app/a", "app/b", "other/c"} {
		if _, err := store.Set(ctx, "ns", k, config.NewValue(1)); err != nil {
			t.Fatalf("Set %q: %v", k, err)
		}
	}

	page, err := store.Find(ctx, "ns", config.NewFilter().WithPrefix("app/").Build())
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if len(page.Results()) != 2 {
		t.Fatalf("Find prefix=app/ returned %d entries, want 2", len(page.Results()))
	}
	if _, ok := page.Results()["app/a"]; !ok {
		t.Error("missing key app/a in results")
	}
	if _, ok := page.Results()["app/b"]; !ok {
		t.Error("missing key app/b in results")
	}
	if _, ok := page.Results()["other/c"]; ok {
		t.Error("key other/c leaked across prefix")
	}
}

func TestRedisStore_Mini_Health(t *testing.T) {
	t.Parallel()
	store, mr := newMiniStore(t)
	ctx := t.Context()

	if err := store.Health(ctx); err != nil {
		t.Fatalf("Health on connected store: %v", err)
	}

	// After the server is dead, health must fail.
	mr.Close()
	if err := store.Health(ctx); err == nil {
		t.Fatal("Health after server shutdown returned nil, want error")
	}
}

func TestRedisStore_Mini_Stats(t *testing.T) {
	t.Parallel()
	store, _ := newMiniStore(t)
	ctx := t.Context()

	if _, err := store.Set(ctx, "ns1", "a", config.NewValue(1)); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, err := store.Set(ctx, "ns1", "b", config.NewValue("s")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, err := store.Set(ctx, "ns2", "c", config.NewValue(true)); err != nil {
		t.Fatalf("Set: %v", err)
	}

	sp, ok := any(store).(config.StatsProvider)
	if !ok {
		t.Fatal("redis store does not implement StatsProvider")
	}
	stats, err := sp.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.TotalEntries() != 3 {
		t.Errorf("TotalEntries = %d, want 3", stats.TotalEntries())
	}
	if stats.CountForNamespace("ns1") != 2 {
		t.Errorf("CountForNamespace(ns1) = %d, want 2", stats.CountForNamespace("ns1"))
	}
	if stats.CountForNamespace("ns2") != 1 {
		t.Errorf("CountForNamespace(ns2) = %d, want 1", stats.CountForNamespace("ns2"))
	}
}

func TestRedisStore_Mini_Watch(t *testing.T) {
	t.Parallel()
	store, _ := newMiniStore(t)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	ch, err := store.Watch(ctx, config.WatchFilter{Namespaces: []string{"ns"}})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	// Give the pub/sub subscription a beat to register on the server side.
	// Without this, the first published event can race the SUBSCRIBE.
	time.Sleep(50 * time.Millisecond)

	if _, err := store.Set(t.Context(), "ns", "k", config.NewValue("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	select {
	case ev, ok := <-ch:
		if !ok {
			t.Fatal("watch channel closed unexpectedly")
		}
		if ev.Type != config.ChangeTypeSet {
			t.Errorf("ChangeType = %v, want ChangeTypeSet", ev.Type)
		}
		if ev.Namespace != "ns" || ev.Key != "k" {
			t.Errorf("event = %+v, want namespace=ns key=k", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no event within 2s")
	}
}

// TestRedisStore_Mini_BulkStoreNotImplemented pins the documented fact
// that the Redis Store deliberately does not implement BulkStore today —
// see the integration evaluator's gap analysis. A type assertion must
// return ok=false so wrapper stores (otel, multi) take the fallback path
// instead of calling through to nil methods.
//
// If/when BulkStore is added to the Redis backend, flip this test to a
// positive happy-path round-trip covering GetMany/SetMany/DeleteMany
// against miniredis.
func TestRedisStore_Mini_BulkStoreNotImplemented(t *testing.T) {
	t.Parallel()
	store, _ := newMiniStore(t)

	if _, ok := any(store).(config.BulkStore); ok {
		t.Fatal("Redis store now implements BulkStore — convert this test to a positive Bulk round-trip")
	}
}

func TestRedisStore_Mini_ClosedStoreOps(t *testing.T) {
	t.Parallel()
	store, _ := newMiniStore(t)
	ctx := t.Context()

	if err := store.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := store.Get(ctx, "ns", "k"); !errors.Is(err, config.ErrStoreClosed) {
		t.Errorf("Get on closed store: err = %v, want ErrStoreClosed", err)
	}
	if _, err := store.Set(ctx, "ns", "k", config.NewValue(1)); !errors.Is(err, config.ErrStoreClosed) {
		t.Errorf("Set on closed store: err = %v, want ErrStoreClosed", err)
	}
}
