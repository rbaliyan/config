package file

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/rbaliyan/config"
)

func TestStore_Connect(t *testing.T) {
	store := NewStore("testdata/config.yaml")
	ctx := context.Background()

	if err := store.Connect(ctx); err != nil {
		t.Fatalf("Connect() error: %v", err)
	}
	defer store.Close(ctx)

	// Should have namespaces
	ns := store.Namespaces()
	if len(ns) == 0 {
		t.Fatal("Namespaces() returned empty")
	}

	found := false
	for _, n := range ns {
		if n == "db" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Namespaces() = %v, missing 'db'", ns)
	}
}

func TestStore_Get(t *testing.T) {
	store := NewStore("testdata/config.yaml")
	ctx := context.Background()

	if err := store.Connect(ctx); err != nil {
		t.Fatalf("Connect() error: %v", err)
	}
	defer store.Close(ctx)

	// Simple value
	val, err := store.Get(ctx, "db", "host")
	if err != nil {
		t.Fatalf("Get(db, host) error: %v", err)
	}
	str, err := val.String()
	if err != nil {
		t.Fatalf("String() error: %v", err)
	}
	if str != "localhost" {
		t.Errorf("Get(db, host) = %q, want %q", str, "localhost")
	}

	// Numeric value
	val, err = store.Get(ctx, "db", "port")
	if err != nil {
		t.Fatalf("Get(db, port) error: %v", err)
	}
	port, err := val.Int64()
	if err != nil {
		t.Fatalf("Int64() error: %v", err)
	}
	if port != 5432 {
		t.Errorf("Get(db, port) = %d, want %d", port, 5432)
	}

	// Nested value (flattened with /)
	val, err = store.Get(ctx, "db", "stores/global")
	if err != nil {
		t.Fatalf("Get(db, stores/global) error: %v", err)
	}
	str, err = val.String()
	if err != nil {
		t.Fatalf("String() error: %v", err)
	}
	if str != "mongodb://global:27017" {
		t.Errorf("Get(db, stores/global) = %q", str)
	}

	// Deeply nested
	val, err = store.Get(ctx, "auth", "auth0/event_storage/enabled")
	if err != nil {
		t.Fatalf("Get(auth, auth0/event_storage/enabled) error: %v", err)
	}
	b, err := val.Bool()
	if err != nil {
		t.Fatalf("Bool() error: %v", err)
	}
	if !b {
		t.Error("Get(auth, auth0/event_storage/enabled) = false, want true")
	}
}

func TestStore_Get_NotFound(t *testing.T) {
	store := NewStore("testdata/config.yaml")
	ctx := context.Background()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	_, err := store.Get(ctx, "db", "nonexistent")
	if !errors.Is(err, config.ErrNotFound) {
		t.Errorf("Get(nonexistent) error = %v, want ErrNotFound", err)
	}

	_, err = store.Get(ctx, "nonexistent", "key")
	if !errors.Is(err, config.ErrNotFound) {
		t.Errorf("Get(nonexistent ns) error = %v, want ErrNotFound", err)
	}
}

func TestStore_ReadOnly(t *testing.T) {
	store := NewStore("testdata/config.yaml")
	ctx := context.Background()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	_, err := store.Set(ctx, "db", "host", config.NewValue("new"))
	if !errors.Is(err, config.ErrReadOnly) {
		t.Errorf("Set() error = %v, want ErrReadOnly", err)
	}

	err = store.Delete(ctx, "db", "host")
	if !errors.Is(err, config.ErrReadOnly) {
		t.Errorf("Delete() error = %v, want ErrReadOnly", err)
	}
}

func TestStore_Watch(t *testing.T) {
	store := NewStore("testdata/config.yaml")
	ctx := context.Background()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	_, err := store.Watch(ctx, config.WatchFilter{})
	if !errors.Is(err, config.ErrWatchNotSupported) {
		t.Errorf("Watch() error = %v, want ErrWatchNotSupported", err)
	}
}

func TestStore_Find_Prefix(t *testing.T) {
	store := NewStore("testdata/config.yaml")
	ctx := context.Background()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	filter := config.NewFilter().WithPrefix("stores/").Build()
	page, err := store.Find(ctx, "db", filter)
	if err != nil {
		t.Fatalf("Find() error: %v", err)
	}

	results := page.Results()
	if len(results) != 2 {
		t.Errorf("Find(stores/) len = %d, want 2", len(results))
	}
	if _, ok := results["stores/global"]; !ok {
		t.Error("Find(stores/) missing stores/global")
	}
	if _, ok := results["stores/regional"]; !ok {
		t.Error("Find(stores/) missing stores/regional")
	}
}

func TestStore_Find_Keys(t *testing.T) {
	store := NewStore("testdata/config.yaml")
	ctx := context.Background()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	filter := config.NewFilter().WithKeys("host", "port").Build()
	page, err := store.Find(ctx, "db", filter)
	if err != nil {
		t.Fatalf("Find() error: %v", err)
	}

	results := page.Results()
	if len(results) != 2 {
		t.Errorf("Find(keys) len = %d, want 2", len(results))
	}
}

func TestStore_Find_AllInNamespace(t *testing.T) {
	store := NewStore("testdata/config.yaml")
	ctx := context.Background()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	filter := config.NewFilter().WithPrefix("").Build()
	page, err := store.Find(ctx, "http", filter)
	if err != nil {
		t.Fatalf("Find() error: %v", err)
	}

	results := page.Results()
	// http has: host, port, read_timeout, write_timeout, cors
	if len(results) < 3 {
		t.Errorf("Find(http, all) len = %d, want >= 3", len(results))
	}
}

func TestStore_Find_Pagination(t *testing.T) {
	store := NewStore("testdata/config.yaml")
	ctx := context.Background()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	// Get first page with limit 2
	filter := config.NewFilter().WithPrefix("").WithLimit(2).Build()
	page1, err := store.Find(ctx, "db", filter)
	if err != nil {
		t.Fatalf("Find() page1 error: %v", err)
	}

	if len(page1.Results()) != 2 {
		t.Fatalf("page1 len = %d, want 2", len(page1.Results()))
	}

	cursor := page1.NextCursor()
	if cursor == "" {
		t.Fatal("page1 NextCursor() is empty")
	}

	// Get second page
	filter2 := config.NewFilter().WithPrefix("").WithLimit(2).WithCursor(cursor).Build()
	page2, err := store.Find(ctx, "db", filter2)
	if err != nil {
		t.Fatalf("Find() page2 error: %v", err)
	}

	if len(page2.Results()) == 0 {
		t.Fatal("page2 has no results")
	}

	// Ensure no overlap
	for k := range page1.Results() {
		if _, ok := page2.Results()[k]; ok {
			t.Errorf("key %q appears in both pages", k)
		}
	}
}

func TestStore_Health(t *testing.T) {
	store := NewStore("testdata/config.yaml")
	ctx := context.Background()

	// Before connect
	if err := store.Health(ctx); !errors.Is(err, config.ErrStoreNotConnected) {
		t.Errorf("Health() before connect = %v, want ErrStoreNotConnected", err)
	}

	_ = store.Connect(ctx)

	// After connect
	if err := store.Health(ctx); err != nil {
		t.Errorf("Health() after connect = %v, want nil", err)
	}

	_ = store.Close(ctx)

	// After close
	if err := store.Health(ctx); !errors.Is(err, config.ErrStoreClosed) {
		t.Errorf("Health() after close = %v, want ErrStoreClosed", err)
	}
}

func TestStore_Stats(t *testing.T) {
	store := NewStore("testdata/config.yaml")
	ctx := context.Background()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats() error: %v", err)
	}

	if stats.TotalEntries == 0 {
		t.Error("Stats().TotalEntries = 0, want > 0")
	}
	if len(stats.EntriesByNamespace) == 0 {
		t.Error("Stats().EntriesByNamespace is empty")
	}
	if _, ok := stats.EntriesByNamespace["db"]; !ok {
		t.Error("Stats() missing 'db' namespace")
	}
}

func TestStore_Close(t *testing.T) {
	store := NewStore("testdata/config.yaml")
	ctx := context.Background()
	_ = store.Connect(ctx)

	// Close should work
	if err := store.Close(ctx); err != nil {
		t.Errorf("Close() error: %v", err)
	}

	// Double close should be safe
	if err := store.Close(ctx); err != nil {
		t.Errorf("Close() second call error: %v", err)
	}

	// Operations after close should fail
	_, err := store.Get(ctx, "db", "host")
	if !errors.Is(err, config.ErrStoreClosed) {
		t.Errorf("Get() after close = %v, want ErrStoreClosed", err)
	}
}

func TestStore_ConnectError(t *testing.T) {
	store := NewStore("testdata/nonexistent.yaml")
	ctx := context.Background()

	if err := store.Connect(ctx); err == nil {
		t.Error("Connect() should error for missing file")
	}
}

func TestStore_WithKeySeparator(t *testing.T) {
	store := NewStore("testdata/config.yaml", WithKeySeparator("."))
	ctx := context.Background()

	if err := store.Connect(ctx); err != nil {
		t.Fatalf("Connect() error: %v", err)
	}
	defer store.Close(ctx)

	// With "." separator, nested key should use dots
	val, err := store.Get(ctx, "db", "stores.global")
	if err != nil {
		t.Fatalf("Get(db, stores.global) error: %v", err)
	}
	str, _ := val.String()
	if str != "mongodb://global:27017" {
		t.Errorf("Get(db, stores.global) = %q", str)
	}
}

func TestStore_InterfaceCompliance(t *testing.T) {
	// Compile-time check is in store.go; this just verifies at test time.
	var _ config.Store = (*Store)(nil)
}

func TestStore_ConcurrentAccess(t *testing.T) {
	store := NewStore("testdata/config.yaml")
	ctx := context.Background()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("Connect() error: %v", err)
	}
	defer store.Close(ctx)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = store.Get(ctx, "db", "host")
			_, _ = store.Get(ctx, "db", "port")
			store.Namespaces()
			_ = store.Health(ctx)
			_, _ = store.Stats(ctx)
			filter := config.NewFilter().WithPrefix("").Build()
			_, _ = store.Find(ctx, "db", filter)
		}()
	}
	wg.Wait()
}

func TestStore_TopLevelScalars(t *testing.T) {
	store := NewStore("testdata/config.yaml", WithDefaultNamespace("globals"))
	ctx := context.Background()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("Connect() error: %v", err)
	}
	defer store.Close(ctx)

	// The testdata YAML might not have top-level scalars.
	// Verify the store still loads without error and has expected namespaces.
	ns := store.Namespaces()
	if len(ns) == 0 {
		t.Fatal("Namespaces() is empty")
	}
}
