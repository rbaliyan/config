package multi

import (
	"context"
	"testing"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/memory"
)

func TestNewStore(t *testing.T) {
	store1 := memory.NewStore()
	store2 := memory.NewStore()

	ms := NewStore([]config.Store{store1, store2})
	if ms == nil {
		t.Fatal("NewStore returned nil")
	}
	if len(ms.stores) != 2 {
		t.Errorf("Expected 2 stores, got %d", len(ms.stores))
	}
}

func TestMultiStore_Connect(t *testing.T) {
	store1 := memory.NewStore()
	store2 := memory.NewStore()

	ms := NewStore([]config.Store{store1, store2})
	ctx := context.Background()

	if err := ms.Connect(ctx); err != nil {
		t.Errorf("Connect failed: %v", err)
	}
}

func TestMultiStore_Close(t *testing.T) {
	store1 := memory.NewStore()
	store2 := memory.NewStore()

	ms := NewStore([]config.Store{store1, store2})
	ctx := context.Background()

	ms.Connect(ctx)
	if err := ms.Close(ctx); err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestMultiStore_SetAndGet_Fallback(t *testing.T) {
	store1 := memory.NewStore()
	store2 := memory.NewStore()

	ms := NewStore([]config.Store{store1, store2}, WithStrategy(StrategyFallback))
	ctx := context.Background()

	ms.Connect(ctx)
	defer ms.Close(ctx)

	// Create a value
	val := config.NewValue(42)

	// Set should write to both stores
	if _, err := ms.Set(ctx, "ns", "key", val); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Get from multi-store should work
	got, err := ms.Get(ctx, "ns", "key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	intVal, _ := got.Int64()
	if intVal != 42 {
		t.Errorf("Expected 42, got %d", intVal)
	}

	// Both stores should have the value
	got1, _ := store1.Get(ctx, "ns", "key")
	got2, _ := store2.Get(ctx, "ns", "key")

	int1, _ := got1.Int64()
	int2, _ := got2.Int64()

	if int1 != 42 || int2 != 42 {
		t.Error("Expected both stores to have the value")
	}
}

func TestMultiStore_SetAndGet_ReadThrough(t *testing.T) {
	cache := memory.NewStore()
	backend := memory.NewStore()

	ms := NewStore([]config.Store{cache, backend}, WithStrategy(StrategyReadThrough))
	ctx := context.Background()

	ms.Connect(ctx)
	defer ms.Close(ctx)

	// Directly set value in backend only
	val := config.NewValue("backend-value")
	backend.Set(ctx, "ns", "key", val)

	// Get should read from backend and populate cache
	got, err := ms.Get(ctx, "ns", "key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	strVal, _ := got.String()
	if strVal != "backend-value" {
		t.Errorf("Expected 'backend-value', got %s", strVal)
	}

	// Cache should now have the value
	cacheVal, err := cache.Get(ctx, "ns", "key")
	if err != nil {
		t.Fatalf("Cache should have value: %v", err)
	}

	cacheStr, _ := cacheVal.String()
	if cacheStr != "backend-value" {
		t.Errorf("Cache should have 'backend-value', got %s", cacheStr)
	}
}

func TestMultiStore_SetAndGet_ReadThrough_WriteAll(t *testing.T) {
	cache := memory.NewStore()
	backend := memory.NewStore()

	ms := NewStore([]config.Store{cache, backend}, WithStrategy(StrategyReadThrough))
	ctx := context.Background()

	ms.Connect(ctx)
	defer ms.Close(ctx)

	// Set via multi-store writes to all stores for consistency
	val := config.NewValue("new-value")
	if _, err := ms.Set(ctx, "ns", "key", val); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Cache should have it
	cacheVal, err := cache.Get(ctx, "ns", "key")
	if err != nil {
		t.Fatalf("Cache should have value: %v", err)
	}
	cacheStr, _ := cacheVal.String()
	if cacheStr != "new-value" {
		t.Error("Cache should have the value")
	}

	// Backend should also have it (all stores are written for consistency)
	backendVal, err := backend.Get(ctx, "ns", "key")
	if err != nil {
		t.Fatalf("Backend should have value: %v", err)
	}
	backendStr, _ := backendVal.String()
	if backendStr != "new-value" {
		t.Error("Backend should have the value")
	}
}

func TestMultiStore_Delete_Fallback(t *testing.T) {
	store1 := memory.NewStore()
	store2 := memory.NewStore()

	ms := NewStore([]config.Store{store1, store2}, WithStrategy(StrategyFallback))
	ctx := context.Background()

	ms.Connect(ctx)
	defer ms.Close(ctx)

	// Set value in both stores
	val := config.NewValue("value")
	_, _ = ms.Set(ctx, "ns", "key", val)

	// Delete should remove from both
	if err := ms.Delete(ctx, "ns", "key"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Both stores should return NotFound
	_, err1 := store1.Get(ctx, "ns", "key")
	_, err2 := store2.Get(ctx, "ns", "key")

	if !config.IsNotFound(err1) || !config.IsNotFound(err2) {
		t.Error("Expected both stores to have deleted the value")
	}
}

func TestMultiStore_Fallback_PrimaryFailure(t *testing.T) {
	store1 := memory.NewStore()
	store2 := memory.NewStore()

	ms := NewStore([]config.Store{store1, store2}, WithStrategy(StrategyFallback))
	ctx := context.Background()

	ms.Connect(ctx)
	defer ms.Close(ctx)

	// Only set value in secondary store
	val := config.NewValue("secondary-value")
	store2.Set(ctx, "ns", "key", val)

	// Get should fall back to secondary
	got, err := ms.Get(ctx, "ns", "key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	strVal, _ := got.String()
	if strVal != "secondary-value" {
		t.Errorf("Expected 'secondary-value', got %s", strVal)
	}
}

func TestMultiStore_Find(t *testing.T) {
	store1 := memory.NewStore()
	store2 := memory.NewStore()

	ms := NewStore([]config.Store{store1, store2})
	ctx := context.Background()

	ms.Connect(ctx)
	defer ms.Close(ctx)

	// Set values in primary store
	val1 := config.NewValue("value1")
	val2 := config.NewValue("value2")
	store1.Set(ctx, "ns", "app/key1", val1)
	store1.Set(ctx, "ns", "app/key2", val2)

	// Find should search primary only
	page, err := ms.Find(ctx, "ns", config.NewFilter().WithPrefix("app/").Build())
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}

	results := page.Results()
	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}
}

func TestMultiStore_Watch(t *testing.T) {
	store1 := memory.NewStore()
	store2 := memory.NewStore()

	ms := NewStore([]config.Store{store1, store2})
	ctx := context.Background()

	ms.Connect(ctx)
	defer ms.Close(ctx)

	// Watch should work through primary
	ch, err := ms.Watch(ctx, config.WatchFilter{Namespaces: []string{"ns"}})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}

	if ch == nil {
		t.Error("Expected watch channel, got nil")
	}
}

func TestMultiStore_Primary(t *testing.T) {
	store1 := memory.NewStore()
	store2 := memory.NewStore()

	ms := NewStore([]config.Store{store1, store2})

	primary := ms.Primary()
	if primary != store1 {
		t.Error("Expected Primary to return first store")
	}
}

func TestMultiStore_Stores(t *testing.T) {
	store1 := memory.NewStore()
	store2 := memory.NewStore()

	ms := NewStore([]config.Store{store1, store2})

	stores := ms.Stores()
	if len(stores) != 2 {
		t.Errorf("Expected 2 stores, got %d", len(stores))
	}
}

func TestMultiStore_EmptyStores(t *testing.T) {
	ms := NewStore(nil)
	ctx := context.Background()

	// Find should fail gracefully
	_, err := ms.Find(ctx, "ns", config.NewFilter().Build())
	if err != config.ErrStoreNotConnected {
		t.Errorf("Expected ErrStoreNotConnected, got %v", err)
	}

	// Watch should fail gracefully
	_, err = ms.Watch(ctx, config.WatchFilter{})
	if err != config.ErrStoreNotConnected {
		t.Errorf("Expected ErrStoreNotConnected, got %v", err)
	}

	// Primary should return nil
	if ms.Primary() != nil {
		t.Error("Expected Primary to return nil for empty store")
	}
}

func TestMultiStore_WriteThrough(t *testing.T) {
	store1 := memory.NewStore()
	store2 := memory.NewStore()

	ms := NewStore([]config.Store{store1, store2}, WithStrategy(StrategyWriteThrough))
	ctx := context.Background()

	ms.Connect(ctx)
	defer ms.Close(ctx)

	// Set should write to all stores
	val := config.NewValue("sync-value")
	if _, err := ms.Set(ctx, "ns", "key", val); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Both stores should have the value
	got1, _ := store1.Get(ctx, "ns", "key")
	got2, _ := store2.Get(ctx, "ns", "key")

	str1, _ := got1.String()
	str2, _ := got2.String()

	if str1 != "sync-value" || str2 != "sync-value" {
		t.Error("Expected both stores to have the value")
	}
}

func TestMultiStore_Health(t *testing.T) {
	store1 := memory.NewStore()
	store2 := memory.NewStore()

	ms := NewStore([]config.Store{store1, store2})
	ctx := context.Background()

	ms.Connect(ctx)
	defer ms.Close(ctx)

	// Memory stores implement HealthChecker, should be healthy
	if err := ms.Health(ctx); err != nil {
		t.Errorf("Health check failed: %v", err)
	}
}

func TestMultiStore_Health_EmptyStores(t *testing.T) {
	ms := NewStore(nil)
	ctx := context.Background()

	err := ms.Health(ctx)
	if err != config.ErrStoreNotConnected {
		t.Errorf("Expected ErrStoreNotConnected, got %v", err)
	}
}

func TestMultiStore_Stats(t *testing.T) {
	store1 := memory.NewStore()
	store2 := memory.NewStore()

	ms := NewStore([]config.Store{store1, store2})
	ctx := context.Background()

	ms.Connect(ctx)
	defer ms.Close(ctx)

	// Set some values
	_, _ = ms.Set(ctx, "ns1", "key1", config.NewValue("value1"))
	_, _ = ms.Set(ctx, "ns1", "key2", config.NewValue(42))
	_, _ = ms.Set(ctx, "ns2", "key3", config.NewValue(true))

	// Memory stores implement StatsProvider
	stats, err := ms.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}

	if stats == nil {
		t.Fatal("Expected stats, got nil")
	}

	if stats.TotalEntries < 3 {
		t.Errorf("Expected at least 3 entries, got %d", stats.TotalEntries)
	}
}

func TestMultiStore_Stats_EmptyStores(t *testing.T) {
	ms := NewStore(nil)
	ctx := context.Background()

	_, err := ms.Stats(ctx)
	if err != config.ErrStoreNotConnected {
		t.Errorf("Expected ErrStoreNotConnected, got %v", err)
	}
}
