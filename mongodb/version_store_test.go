package mongodb_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

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
