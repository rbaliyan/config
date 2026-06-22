package config_test

import (
	"context"
	"testing"
	"time"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/memory"
)

// TestCacheResilienceFallback verifies that when the store fails,
// previously cached values are still returned (resilience pattern).
func TestCacheResilienceFallback(t *testing.T) {
	ctx := context.Background()

	// Create a store that can simulate failures
	underlying := memory.NewStore()
	store := &failingStore{Store: underlying}

	mgr, err := config.New(config.WithStore(store))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("test")

	// Set a value (this also populates the cache)
	if err := cfg.Set(ctx, "app/timeout", 30); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// First Get - should work and cache the value
	val, err := cfg.Get(ctx, "app/timeout")
	if err != nil {
		t.Fatalf("First Get failed: %v", err)
	}
	i, _ := val.Int64()
	if i != 30 {
		t.Errorf("Expected 30, got %d", i)
	}

	// Simulate store failure
	store.failGet.Store(true)

	// Second Get - store fails, but should return cached value
	val, err = cfg.Get(ctx, "app/timeout")
	if err != nil {
		t.Fatalf("Get during store failure should return cached value, got error: %v", err)
	}
	i, _ = val.Int64()
	if i != 30 {
		t.Errorf("Expected cached value 30, got %d", i)
	}

	// Restore store
	store.failGet.Store(false)

	// Third Get - should work normally again
	val, err = cfg.Get(ctx, "app/timeout")
	if err != nil {
		t.Fatalf("Get after store recovery failed: %v", err)
	}
	i, _ = val.Int64()
	if i != 30 {
		t.Errorf("Expected 30, got %d", i)
	}
}

// TestCacheDoesNotHideNotFound verifies that NotFound errors are NOT
// hidden by the cache - if a key doesn't exist, NotFound is returned.
func TestCacheDoesNotHideNotFound(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("test")

	// Get a key that doesn't exist - should return NotFound, not a cached value
	_, getErr := cfg.Get(ctx, "nonexistent/key")
	if !config.IsNotFound(getErr) {
		t.Errorf("Expected NotFound error for nonexistent key, got: %v", getErr)
	}
}

func TestMarkStale(t *testing.T) {
	// nil should return nil
	if config.MarkStale(nil) != nil {
		t.Error("MarkStale(nil) should return nil")
	}

	// Mark a concrete value as stale
	val := config.NewValue("hello",
		config.WithValueMetadata(1, time.Now(), time.Now()),
	)
	stale := config.MarkStale(val)

	if stale == nil {
		t.Fatal("MarkStale should return non-nil")
	}
	if !stale.Metadata().IsStale() {
		t.Error("Stale value should have IsStale() = true")
	}
	// Original should not be stale
	if val.Metadata().IsStale() {
		t.Error("Original value should not be stale")
	}
	// Value should be preserved
	s, _ := stale.String()
	if s != "hello" {
		t.Errorf("Stale value = %q, want %q", s, "hello")
	}
	// Version should be preserved
	if stale.Metadata().Version() != 1 {
		t.Errorf("Stale version = %d, want 1", stale.Metadata().Version())
	}

	// Mark a value without metadata
	val2 := config.NewValue(42)
	stale2 := config.MarkStale(val2)
	if !stale2.Metadata().IsStale() {
		t.Error("Stale value without original metadata should still be stale")
	}
}

func TestCacheStats(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("test")

	// Set and get some values to generate cache activity
	_ = cfg.Set(ctx, "key", "value")
	_, _ = cfg.Get(ctx, "key")
	_, _ = cfg.Get(ctx, "key") // cache hit

	obs, ok := mgr.(config.ManagerObserver)
	if !ok {
		t.Fatal("Manager should implement ManagerObserver")
	}

	stats := obs.CacheStats()
	// Just verify it returns without error and has reasonable values
	if stats.HitRate() < 0 || stats.HitRate() > 1 {
		t.Errorf("HitRate() = %f, want between 0 and 1", stats.HitRate())
	}
}

// TestStaleValueWrapper tests MarkStale with a non-*val Value implementation.
func TestStaleValueWrapper(t *testing.T) {
	// Create a custom Value implementation to trigger the staleValueWrapper path
	custom := &customValue{
		raw: "hello",
		meta: &customMetadata{
			version: 5,
		},
	}

	stale := config.MarkStale(custom)
	if stale == nil {
		t.Fatal("MarkStale should return non-nil")
	}
	if !stale.Metadata().IsStale() {
		t.Error("Stale wrapper should have IsStale() = true")
	}
	// Verify original metadata is preserved through wrapper
	if stale.Metadata().Version() != 5 {
		t.Errorf("Version = %d, want 5", stale.Metadata().Version())
	}
}

// TestCacheResilienceStaleMarker verifies stale values returned from cache have IsStale() = true.
func TestCacheResilienceStaleMarker(t *testing.T) {
	ctx := context.Background()

	underlying := memory.NewStore()
	store := &failingStore{Store: underlying}

	mgr, err := config.New(config.WithStore(store))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("test")

	// Set a value and read it (populates cache)
	_ = cfg.Set(ctx, "app/timeout", 30)
	val, err := cfg.Get(ctx, "app/timeout")
	if err != nil {
		t.Fatalf("First Get failed: %v", err)
	}
	if val.Metadata().IsStale() {
		t.Error("First Get should not be stale")
	}

	// Simulate store failure
	store.failGet.Store(true)

	// Second Get returns stale cached value
	val, err = cfg.Get(ctx, "app/timeout")
	if err != nil {
		t.Fatalf("Get during failure should return cached value, got: %v", err)
	}
	if !val.Metadata().IsStale() {
		t.Error("Value from cache fallback should be marked stale")
	}
	i, _ := val.Int64()
	if i != 30 {
		t.Errorf("Expected 30, got %d", i)
	}
}

// TestCacheStatsHitRateZero tests HitRate when there are no lookups.
func TestCacheStatsHitRateZero(t *testing.T) {
	stats := &config.CacheStats{}
	if stats.HitRate() != 0 {
		t.Errorf("HitRate() = %f, want 0 for zero lookups", stats.HitRate())
	}

	// With only hits
	stats2 := &config.CacheStats{Hits: 10, Misses: 0}
	if stats2.HitRate() != 1.0 {
		t.Errorf("HitRate() = %f, want 1.0 for all hits", stats2.HitRate())
	}

	// Mixed
	stats3 := &config.CacheStats{Hits: 3, Misses: 7}
	if stats3.HitRate() != 0.3 {
		t.Errorf("HitRate() = %f, want 0.3", stats3.HitRate())
	}
}
