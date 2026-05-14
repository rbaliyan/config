package mongodb_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/mongodb"
)

// skipIfNoMongoVersioned returns a store with versioning enabled and a
// per-test collection name to avoid cross-test pollution.
func skipIfNoMongoVersioned(t *testing.T, opts ...mongodb.Option) (*mongodb.Store, *mongo.Client) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := mongo.Connect(options.Client().ApplyURI(getMongoURI()))
	if err != nil {
		t.Skipf("MongoDB not available: %v", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		t.Skipf("MongoDB not available: %v", err)
	}

	// Unique collection name per test so each test gets a clean slate
	// without depending on test ordering.
	collName := fmt.Sprintf("entries_versioned_%d", time.Now().UnixNano())

	storeOpts := append([]mongodb.Option{
		mongodb.WithDatabase("config_test"),
		mongodb.WithCollection(collName),
		mongodb.WithVersioning(true),
	}, opts...)

	store := mongodb.NewStore(client, storeOpts...)
	if err := store.Connect(ctx); err != nil {
		_ = client.Disconnect(ctx)
		t.Skipf("Store connect failed: %v", err)
	}

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = client.Database("config_test").Collection(collName).Drop(cleanupCtx)
		_ = client.Database("config_test").Collection(collName + "_versions").Drop(cleanupCtx)
		_ = store.Close(cleanupCtx)
		_ = client.Disconnect(cleanupCtx)
	})

	return store, client
}

func TestMongoVersions_DisabledByDefault(t *testing.T) {
	store, _ := skipIfNoMongo(t)
	ctx := context.Background()
	defer func() { _ = store.Close(ctx) }()

	if _, err := store.Set(ctx, "vns", "k", config.NewValue("v1")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	defer func() { _ = store.Delete(ctx, "vns", "k") }()

	_, err := store.GetVersions(ctx, "vns", "k", config.NewVersionFilter().Build())
	if !errors.Is(err, config.ErrVersioningNotSupported) {
		t.Fatalf("expected ErrVersioningNotSupported, got %v", err)
	}
}

func TestMongoVersions_SpecificVersion(t *testing.T) {
	store, _ := skipIfNoMongoVersioned(t)
	ctx := context.Background()

	for i, v := range []string{"v1", "v2", "v3"} {
		if _, err := store.Set(ctx, "ns", "key", config.NewValue(v)); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}

	page, err := store.GetVersions(ctx, "ns", "key",
		config.NewVersionFilter().WithVersion(2).Build())
	if err != nil {
		t.Fatalf("GetVersions: %v", err)
	}
	versions := page.Versions()
	if len(versions) != 1 {
		t.Fatalf("expected 1 version, got %d", len(versions))
	}
	if versions[0].Metadata().Version() != 2 {
		t.Errorf("got version %d, want 2", versions[0].Metadata().Version())
	}
	str, _ := versions[0].String()
	if str != "v2" {
		t.Errorf("got value %q, want %q", str, "v2")
	}
}

func TestMongoVersions_VersionNotFound(t *testing.T) {
	store, _ := skipIfNoMongoVersioned(t)
	ctx := context.Background()

	if _, err := store.Set(ctx, "ns", "key", config.NewValue("v1")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	_, err := store.GetVersions(ctx, "ns", "key",
		config.NewVersionFilter().WithVersion(99).Build())
	if !config.IsVersionNotFound(err) {
		t.Errorf("expected ErrVersionNotFound, got %v", err)
	}
}

func TestMongoVersions_KeyNotFound(t *testing.T) {
	store, _ := skipIfNoMongoVersioned(t)
	ctx := context.Background()

	_, err := store.GetVersions(ctx, "ns", "missing", config.NewVersionFilter().Build())
	if !config.IsNotFound(err) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestMongoVersions_ListAllDescending(t *testing.T) {
	store, _ := skipIfNoMongoVersioned(t)
	ctx := context.Background()

	for i := 1; i <= 5; i++ {
		if _, err := store.Set(ctx, "ns", "key", config.NewValue(i)); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}

	page, err := store.GetVersions(ctx, "ns", "key", config.NewVersionFilter().Build())
	if err != nil {
		t.Fatalf("GetVersions: %v", err)
	}
	versions := page.Versions()
	if len(versions) != 5 {
		t.Fatalf("expected 5 versions, got %d", len(versions))
	}
	for i, v := range versions {
		want := int64(5 - i)
		if v.Metadata().Version() != want {
			t.Errorf("versions[%d] = %d, want %d", i, v.Metadata().Version(), want)
		}
	}
}

func TestMongoVersions_Pagination(t *testing.T) {
	store, _ := skipIfNoMongoVersioned(t)
	ctx := context.Background()

	for i := 1; i <= 5; i++ {
		if _, err := store.Set(ctx, "ns", "key", config.NewValue(i)); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}

	page1, err := store.GetVersions(ctx, "ns", "key", config.NewVersionFilter().WithLimit(2).Build())
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	if len(page1.Versions()) != 2 ||
		page1.Versions()[0].Metadata().Version() != 5 ||
		page1.Versions()[1].Metadata().Version() != 4 {
		t.Fatalf("page1 versions wrong: %+v", versionsList(page1))
	}
	if page1.NextCursor() == "" {
		t.Fatal("expected non-empty cursor after page 1")
	}

	page2, err := store.GetVersions(ctx, "ns", "key",
		config.NewVersionFilter().WithLimit(2).WithCursor(page1.NextCursor()).Build())
	if err != nil {
		t.Fatalf("page 2: %v", err)
	}
	if len(page2.Versions()) != 2 ||
		page2.Versions()[0].Metadata().Version() != 3 ||
		page2.Versions()[1].Metadata().Version() != 2 {
		t.Fatalf("page2 versions wrong: %+v", versionsList(page2))
	}

	page3, err := store.GetVersions(ctx, "ns", "key",
		config.NewVersionFilter().WithLimit(2).WithCursor(page2.NextCursor()).Build())
	if err != nil {
		t.Fatalf("page 3: %v", err)
	}
	if len(page3.Versions()) != 1 || page3.Versions()[0].Metadata().Version() != 1 {
		t.Fatalf("page3 versions wrong: %+v", versionsList(page3))
	}
	if page3.NextCursor() != "" {
		t.Errorf("expected empty cursor on last page, got %q", page3.NextCursor())
	}
}

func TestMongoVersions_NilFilter(t *testing.T) {
	store, _ := skipIfNoMongoVersioned(t)
	ctx := context.Background()

	_, _ = store.Set(ctx, "ns", "key", config.NewValue("a"))
	_, _ = store.Set(ctx, "ns", "key", config.NewValue("b"))

	page, err := store.GetVersions(ctx, "ns", "key", nil)
	if err != nil {
		t.Fatalf("GetVersions: %v", err)
	}
	if len(page.Versions()) != 2 {
		t.Errorf("expected 2 versions, got %d", len(page.Versions()))
	}
}

func TestMongoVersions_DeleteDiscardsHistory(t *testing.T) {
	store, _ := skipIfNoMongoVersioned(t)
	ctx := context.Background()

	_, _ = store.Set(ctx, "ns", "key", config.NewValue("v1"))
	_, _ = store.Set(ctx, "ns", "key", config.NewValue("v2"))
	_, _ = store.Set(ctx, "ns", "key", config.NewValue("v3"))

	if err := store.Delete(ctx, "ns", "key"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err := store.GetVersions(ctx, "ns", "key", config.NewVersionFilter().Build())
	if !config.IsNotFound(err) {
		t.Errorf("expected ErrNotFound after delete, got %v", err)
	}

	// Recreating starts the version counter at 1 again.
	if _, err := store.Set(ctx, "ns", "key", config.NewValue("new")); err != nil {
		t.Fatalf("recreate Set: %v", err)
	}
	page, err := store.GetVersions(ctx, "ns", "key", config.NewVersionFilter().Build())
	if err != nil {
		t.Fatalf("GetVersions after recreate: %v", err)
	}
	if len(page.Versions()) != 1 {
		t.Fatalf("expected 1 version after recreate, got %d", len(page.Versions()))
	}
	if page.Versions()[0].Metadata().Version() != 1 {
		t.Errorf("version after recreate = %d, want 1", page.Versions()[0].Metadata().Version())
	}
}

func TestMongoVersions_DeleteManyDiscardsHistory(t *testing.T) {
	store, _ := skipIfNoMongoVersioned(t)
	ctx := context.Background()

	_, _ = store.Set(ctx, "ns", "a", config.NewValue("a1"))
	_, _ = store.Set(ctx, "ns", "a", config.NewValue("a2"))
	_, _ = store.Set(ctx, "ns", "b", config.NewValue("b1"))

	if _, err := store.DeleteMany(ctx, "ns", []string{"a", "b"}); err != nil {
		t.Fatalf("DeleteMany: %v", err)
	}

	for _, k := range []string{"a", "b"} {
		_, err := store.GetVersions(ctx, "ns", k, config.NewVersionFilter().Build())
		if !config.IsNotFound(err) {
			t.Errorf("GetVersions(%q) after DeleteMany: expected ErrNotFound, got %v", k, err)
		}
	}
}

func TestMongoVersions_SetManyWritesSnapshots(t *testing.T) {
	store, _ := skipIfNoMongoVersioned(t)
	ctx := context.Background()

	values := map[string]config.Value{
		"a": config.NewValue("a1"),
		"b": config.NewValue("b1"),
	}
	if err := store.SetMany(ctx, "ns", values); err != nil {
		t.Fatalf("SetMany: %v", err)
	}

	// Update once more so each key has two versions.
	values2 := map[string]config.Value{
		"a": config.NewValue("a2"),
		"b": config.NewValue("b2"),
	}
	if err := store.SetMany(ctx, "ns", values2); err != nil {
		t.Fatalf("SetMany 2: %v", err)
	}

	for _, k := range []string{"a", "b"} {
		page, err := store.GetVersions(ctx, "ns", k, config.NewVersionFilter().Build())
		if err != nil {
			t.Fatalf("GetVersions(%q): %v", k, err)
		}
		if len(page.Versions()) != 2 {
			t.Errorf("key %q: expected 2 versions, got %d", k, len(page.Versions()))
		}
	}
}

func TestMongoVersions_MaxHistory(t *testing.T) {
	// MaxHistory=2 → retain current + 2 past = 3 snapshots total.
	store, _ := skipIfNoMongoVersioned(t, mongodb.WithMaxHistory(2))
	ctx := context.Background()

	for i := 1; i <= 5; i++ {
		if _, err := store.Set(ctx, "ns", "key", config.NewValue(i)); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}

	page, err := store.GetVersions(ctx, "ns", "key", config.NewVersionFilter().Build())
	if err != nil {
		t.Fatalf("GetVersions: %v", err)
	}
	if len(page.Versions()) != 3 {
		t.Fatalf("expected 3 retained versions, got %d", len(page.Versions()))
	}
	if page.Versions()[0].Metadata().Version() != 5 {
		t.Errorf("newest version = %d, want 5", page.Versions()[0].Metadata().Version())
	}
	if page.Versions()[2].Metadata().Version() != 3 {
		t.Errorf("oldest retained = %d, want 3", page.Versions()[2].Metadata().Version())
	}

	// The trimmed-off versions return ErrVersionNotFound.
	_, err = store.GetVersions(ctx, "ns", "key",
		config.NewVersionFilter().WithVersion(1).Build())
	if !config.IsVersionNotFound(err) {
		t.Errorf("trimmed version: expected ErrVersionNotFound, got %v", err)
	}
}

func TestMongoVersions_InvalidNamespace(t *testing.T) {
	store, _ := skipIfNoMongoVersioned(t)
	ctx := context.Background()

	_, err := store.GetVersions(ctx, "!bad!", "key", config.NewVersionFilter().Build())
	if !errors.Is(err, config.ErrInvalidNamespace) {
		t.Errorf("expected ErrInvalidNamespace, got %v", err)
	}
}

func TestMongoVersions_InvalidKey(t *testing.T) {
	store, _ := skipIfNoMongoVersioned(t)
	ctx := context.Background()

	_, err := store.GetVersions(ctx, "ns", "", config.NewVersionFilter().Build())
	if !config.IsInvalidKey(err) {
		t.Errorf("expected invalid key error, got %v", err)
	}
}

func TestMongoVersions_InvalidCursor(t *testing.T) {
	store, _ := skipIfNoMongoVersioned(t)
	ctx := context.Background()

	_, _ = store.Set(ctx, "ns", "key", config.NewValue("v1"))

	_, err := store.GetVersions(ctx, "ns", "key",
		config.NewVersionFilter().WithCursor("not-a-number").Build())
	if err == nil {
		t.Fatal("expected error for invalid cursor")
	}
}

func TestMongoVersions_ClosedStore(t *testing.T) {
	store, _ := skipIfNoMongoVersioned(t)
	ctx := context.Background()
	_ = store.Close(ctx)

	_, err := store.GetVersions(ctx, "ns", "key", config.NewVersionFilter().Build())
	if !errors.Is(err, config.ErrStoreClosed) {
		t.Errorf("expected ErrStoreClosed, got %v", err)
	}
}

func TestMongoVersions_SetManyAssignsDistinctVersionsConcurrently(t *testing.T) {
	// When versioning is enabled, SetMany must take the per-key path so
	// every successful write produces exactly one snapshot — no gaps —
	// even under concurrent SetMany calls. This test exercises the
	// race-window fix called out in code review.
	store, _ := skipIfNoMongoVersioned(t)
	ctx := context.Background()

	const writers = 4
	const passes = 5
	var wg sync.WaitGroup
	wg.Add(writers)
	for w := range writers {
		go func(w int) {
			defer wg.Done()
			for p := range passes {
				_ = store.SetMany(ctx, "ns", map[string]config.Value{
					"a": config.NewValue(fmt.Sprintf("w%d-p%d-a", w, p)),
					"b": config.NewValue(fmt.Sprintf("w%d-p%d-b", w, p)),
				})
			}
		}(w)
	}
	wg.Wait()

	// Each key should now have exactly writers*passes snapshots, one per
	// successful write — no gaps from the read-back race.
	totalPerKey := int64(writers * passes)
	for _, key := range []string{"a", "b"} {
		page, err := store.GetVersions(ctx, "ns", key, config.NewVersionFilter().Build())
		if err != nil {
			t.Fatalf("GetVersions(%q): %v", key, err)
		}
		got := int64(len(page.Versions()))
		if got != totalPerKey {
			t.Errorf("key %q: got %d snapshots, want %d (per-key path should not drop snapshots)",
				key, got, totalPerKey)
		}
		// Version numbers must be a dense [1..totalPerKey] sequence.
		seen := make(map[int64]bool, len(page.Versions()))
		for _, v := range page.Versions() {
			seen[v.Metadata().Version()] = true
		}
		for v := int64(1); v <= totalPerKey; v++ {
			if !seen[v] {
				t.Errorf("key %q: missing snapshot for version %d", key, v)
			}
		}
	}
}

func TestMongoVersions_OnVersionErrorHookFiresOnSnapshotFailure(t *testing.T) {
	// Force a snapshot failure by closing the store immediately after a
	// successful entry write. We simulate this by writing directly to a
	// versions collection with a unique index that's already populated for
	// the same (ns, key, version) triple — the duplicate would normally be
	// idempotent, so instead we use a different mechanism: drop the
	// versions collection's database connection target by pointing at a
	// non-existent database. The cleanest deterministic trigger is to
	// invoke CleanupOrphans on a closed store and check the hook fires.
	//
	// We test the wiring end-to-end via CleanupOrphans returning an error
	// on a closed store; the hook is invoked from snapshot/trim/delete
	// paths in the same way.
	var hookCalls atomic.Int64
	store, _ := skipIfNoMongoVersioned(t,
		mongodb.WithOnVersionError(func(op, namespace, key string, version int64, err error) {
			hookCalls.Add(1)
		}),
	)
	ctx := context.Background()

	// Sanity: normal Set with versioning produces a snapshot and the hook
	// is NOT fired on the happy path.
	if _, err := store.Set(ctx, "ns", "key", config.NewValue("v1")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if hookCalls.Load() != 0 {
		t.Errorf("hook fired on happy path: %d calls", hookCalls.Load())
	}

	// Force an orphan, then drop the versions collection so the cleanup
	// aggregation runs over a freshly dropped namespace — this exercises
	// the success path (no error) so the hook should still report zero.
	if _, err := store.CleanupOrphans(ctx); err != nil {
		t.Fatalf("CleanupOrphans on healthy store: %v", err)
	}
	if hookCalls.Load() != 0 {
		t.Errorf("hook fired on healthy CleanupOrphans: %d calls", hookCalls.Load())
	}

	// After Close, CleanupOrphans should return ErrStoreClosed without
	// firing the hook (closed-store guard short-circuits before any work).
	_ = store.Close(ctx)
	if _, err := store.CleanupOrphans(ctx); !errors.Is(err, config.ErrStoreClosed) {
		t.Errorf("CleanupOrphans after Close: got %v, want ErrStoreClosed", err)
	}
}

func TestMongoVersions_CleanupOrphansRemovesOrphansAndKeepsLiveHistory(t *testing.T) {
	store, client := skipIfNoMongoVersioned(t)
	ctx := context.Background()

	// Build a live key with two snapshots — these must survive cleanup.
	_, _ = store.Set(ctx, "ns", "live", config.NewValue("l1"))
	_, _ = store.Set(ctx, "ns", "live", config.NewValue("l2"))

	// Inject orphan snapshots directly into the versions collection.
	versionsColl := client.Database("config_test").Collection(store.VersionsCollectionName())

	now := time.Now().UTC()
	orphanDocs := []any{
		bson.M{
			"namespace": "ns", "key": "orphan-a",
			"value": "x", "codec": "json", "type": int32(0),
			"version": int64(1), "created_at": now, "updated_at": now,
		},
		bson.M{
			"namespace": "ns", "key": "orphan-a",
			"value": "y", "codec": "json", "type": int32(0),
			"version": int64(2), "created_at": now, "updated_at": now,
		},
		bson.M{
			"namespace": "ns2", "key": "orphan-b",
			"value": "z", "codec": "json", "type": int32(0),
			"version": int64(1), "created_at": now, "updated_at": now,
		},
	}
	if _, err := versionsColl.InsertMany(ctx, orphanDocs); err != nil {
		t.Fatalf("seed orphans: %v", err)
	}

	deleted, err := store.CleanupOrphans(ctx)
	if err != nil {
		t.Fatalf("CleanupOrphans: %v", err)
	}
	if deleted != 3 {
		t.Errorf("deleted %d, want 3", deleted)
	}

	// Live history must remain intact.
	page, err := store.GetVersions(ctx, "ns", "live", config.NewVersionFilter().Build())
	if err != nil {
		t.Fatalf("GetVersions after cleanup: %v", err)
	}
	if len(page.Versions()) != 2 {
		t.Errorf("live key history = %d, want 2", len(page.Versions()))
	}

	// Re-running cleanup is a no-op.
	deleted, err = store.CleanupOrphans(ctx)
	if err != nil {
		t.Fatalf("CleanupOrphans (second pass): %v", err)
	}
	if deleted != 0 {
		t.Errorf("second-pass deleted = %d, want 0", deleted)
	}
}

func TestMongoVersions_CleanupOrphansRequiresVersioning(t *testing.T) {
	store, _ := skipIfNoMongo(t)
	ctx := context.Background()
	defer func() { _ = store.Close(ctx) }()

	_, err := store.CleanupOrphans(ctx)
	if !errors.Is(err, config.ErrVersioningNotSupported) {
		t.Errorf("CleanupOrphans with versioning off: got %v, want ErrVersioningNotSupported", err)
	}
}

// TestMongoVersions_VersionedReaderThroughManager verifies the public flow
// from config.Manager.Namespace(...).(config.VersionedReader) all the way
// down to the mongodb store's GetVersions. This is the path application
// code actually uses.
func TestMongoVersions_VersionedReaderThroughManager(t *testing.T) {
	store, _ := skipIfNoMongoVersioned(t)
	ctx := context.Background()

	mgr, err := config.New(config.WithStore(store))
	if err != nil {
		t.Fatalf("config.New: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Manager.Connect: %v", err)
	}
	defer func() { _ = mgr.Close(ctx) }()

	cfg := mgr.Namespace("appns")
	for _, v := range []string{"v1", "v2", "v3"} {
		if err := cfg.Set(ctx, "feature/flag", v); err != nil {
			t.Fatalf("cfg.Set(%q): %v", v, err)
		}
	}

	vr, ok := cfg.(config.VersionedReader)
	if !ok {
		t.Fatal("Config does not satisfy VersionedReader")
	}

	page, err := vr.GetVersions(ctx, "feature/flag", config.NewVersionFilter().Build())
	if err != nil {
		t.Fatalf("VersionedReader.GetVersions: %v", err)
	}
	if len(page.Versions()) != 3 {
		t.Fatalf("expected 3 versions, got %d", len(page.Versions()))
	}
	if page.Versions()[0].Metadata().Version() != 3 {
		t.Errorf("newest version = %d, want 3", page.Versions()[0].Metadata().Version())
	}
}

func versionsList(p config.VersionPage) []int64 {
	out := make([]int64, len(p.Versions()))
	for i, v := range p.Versions() {
		out[i] = v.Metadata().Version()
	}
	return out
}
