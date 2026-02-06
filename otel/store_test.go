package otel

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/memory"
)

// minimalStore implements only config.Store (not BulkStore, HealthChecker, or StatsProvider).
// Used to test the fallback paths in InstrumentedStore.
type minimalStore struct {
	data map[string]config.Value // keyed by "namespace\x00key"
}

func newMinimalStore() *minimalStore {
	return &minimalStore{data: make(map[string]config.Value)}
}

func (s *minimalStore) key(ns, k string) string { return ns + "\x00" + k }

func (s *minimalStore) Connect(ctx context.Context) error { return nil }
func (s *minimalStore) Close(ctx context.Context) error   { return nil }

func (s *minimalStore) Get(ctx context.Context, namespace, key string) (config.Value, error) {
	v, ok := s.data[s.key(namespace, key)]
	if !ok {
		return nil, &config.KeyNotFoundError{Key: key, Namespace: namespace}
	}
	return v, nil
}

func (s *minimalStore) Set(ctx context.Context, namespace, key string, value config.Value) (config.Value, error) {
	s.data[s.key(namespace, key)] = value
	return value, nil
}

func (s *minimalStore) Delete(ctx context.Context, namespace, key string) error {
	ek := s.key(namespace, key)
	if _, ok := s.data[ek]; !ok {
		return &config.KeyNotFoundError{Key: key, Namespace: namespace}
	}
	delete(s.data, ek)
	return nil
}

func (s *minimalStore) Find(ctx context.Context, namespace string, filter config.Filter) (config.Page, error) {
	results := make(map[string]config.Value)
	return config.NewPage(results, "", 0), nil
}

func (s *minimalStore) Watch(ctx context.Context, filter config.WatchFilter) (<-chan config.ChangeEvent, error) {
	ch := make(chan config.ChangeEvent)
	return ch, nil
}

// Compile-time check: minimalStore does NOT implement BulkStore, HealthChecker, StatsProvider
var _ config.Store = (*minimalStore)(nil)

func TestWrapStore(t *testing.T) {
	store := memory.NewStore()

	wrapped, err := WrapStore(store)
	if err != nil {
		t.Fatalf("WrapStore failed: %v", err)
	}

	if wrapped == nil {
		t.Fatal("WrapStore returned nil")
	}
}

func TestWrapStore_WithOptions(t *testing.T) {
	store := memory.NewStore()

	wrapped, err := WrapStore(store,
		WithServiceName("test-service"),
		WithBackendName("memory"),
		WithTracesEnabled(true),
		WithMetricsEnabled(true),
	)
	if err != nil {
		t.Fatalf("WrapStore failed: %v", err)
	}

	if wrapped == nil {
		t.Fatal("WrapStore returned nil")
	}
}

func TestInstrumentedStore_Unwrap(t *testing.T) {
	store := memory.NewStore()
	wrapped, _ := WrapStore(store)

	unwrapped := wrapped.Unwrap()
	if unwrapped != store {
		t.Error("Unwrap should return original store")
	}
}

func TestInstrumentedStore_Connect(t *testing.T) {
	store := memory.NewStore()
	wrapped, _ := WrapStore(store, WithTracesEnabled(true), WithMetricsEnabled(true))

	ctx := context.Background()
	if err := wrapped.Connect(ctx); err != nil {
		t.Errorf("Connect failed: %v", err)
	}
}

func TestInstrumentedStore_Close(t *testing.T) {
	store := memory.NewStore()
	wrapped, _ := WrapStore(store, WithTracesEnabled(true), WithMetricsEnabled(true))

	ctx := context.Background()
	_ = wrapped.Connect(ctx)
	if err := wrapped.Close(ctx); err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestInstrumentedStore_Get(t *testing.T) {
	store := memory.NewStore()
	wrapped, _ := WrapStore(store, WithTracesEnabled(true), WithMetricsEnabled(true))

	ctx := context.Background()
	_ = wrapped.Connect(ctx)
	defer wrapped.Close(ctx)

	// Set a value first
	val := config.NewValue("test-value")
	store.Set(ctx, "ns", "key", val)

	// Get via instrumented store
	got, err := wrapped.Get(ctx, "ns", "key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	strVal, _ := got.String()
	if strVal != "test-value" {
		t.Errorf("Expected 'test-value', got %s", strVal)
	}
}

func TestInstrumentedStore_Get_NotFound(t *testing.T) {
	store := memory.NewStore()
	wrapped, _ := WrapStore(store, WithTracesEnabled(true), WithMetricsEnabled(true))

	ctx := context.Background()
	_ = wrapped.Connect(ctx)
	defer wrapped.Close(ctx)

	_, err := wrapped.Get(ctx, "ns", "nonexistent")
	if !config.IsNotFound(err) {
		t.Errorf("Expected NotFound error, got: %v", err)
	}
}

func TestInstrumentedStore_Set(t *testing.T) {
	store := memory.NewStore()
	wrapped, _ := WrapStore(store, WithTracesEnabled(true), WithMetricsEnabled(true))

	ctx := context.Background()
	_ = wrapped.Connect(ctx)
	defer wrapped.Close(ctx)

	val := config.NewValue(42)
	if _, err := wrapped.Set(ctx, "ns", "key", val); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Verify it was set
	got, err := wrapped.Get(ctx, "ns", "key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	intVal, _ := got.Int64()
	if intVal != 42 {
		t.Errorf("Expected 42, got %d", intVal)
	}
}

func TestInstrumentedStore_Delete(t *testing.T) {
	store := memory.NewStore()
	wrapped, _ := WrapStore(store, WithTracesEnabled(true), WithMetricsEnabled(true))

	ctx := context.Background()
	_ = wrapped.Connect(ctx)
	defer wrapped.Close(ctx)

	// Set and then delete
	val := config.NewValue("to-delete")
	_, _ = wrapped.Set(ctx, "ns", "key", val)
	if err := wrapped.Delete(ctx, "ns", "key"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify it was deleted
	_, err := wrapped.Get(ctx, "ns", "key")
	if !config.IsNotFound(err) {
		t.Error("Expected NotFound after delete")
	}
}

func TestInstrumentedStore_Find(t *testing.T) {
	store := memory.NewStore()
	wrapped, _ := WrapStore(store, WithTracesEnabled(true), WithMetricsEnabled(true))

	ctx := context.Background()
	_ = wrapped.Connect(ctx)
	defer wrapped.Close(ctx)

	// Set some values
	_, _ = wrapped.Set(ctx, "ns", "app/key1", config.NewValue("value1"))
	_, _ = wrapped.Set(ctx, "ns", "app/key2", config.NewValue("value2"))

	// Find
	page, err := wrapped.Find(ctx, "ns", config.NewFilter().WithPrefix("app/").Build())
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}

	results := page.Results()
	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}
}

func TestInstrumentedStore_Watch(t *testing.T) {
	store := memory.NewStore()
	wrapped, _ := WrapStore(store, WithTracesEnabled(true), WithMetricsEnabled(true))

	ctx := context.Background()
	_ = wrapped.Connect(ctx)
	defer wrapped.Close(ctx)

	ch, err := wrapped.Watch(ctx, config.WatchFilter{Namespaces: []string{"ns"}})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}

	if ch == nil {
		t.Error("Expected watch channel, got nil")
	}
}

func TestInstrumentedStore_Health(t *testing.T) {
	store := memory.NewStore()
	wrapped, _ := WrapStore(store, WithTracesEnabled(true))

	ctx := context.Background()
	_ = wrapped.Connect(ctx)
	defer wrapped.Close(ctx)

	// Memory store implements HealthChecker
	if err := wrapped.Health(ctx); err != nil {
		t.Errorf("Health check failed: %v", err)
	}
}

func TestInstrumentedStore_Stats(t *testing.T) {
	store := memory.NewStore()
	wrapped, _ := WrapStore(store, WithTracesEnabled(true))

	ctx := context.Background()
	_ = wrapped.Connect(ctx)
	defer wrapped.Close(ctx)

	// Memory store implements StatsProvider
	stats, err := wrapped.Stats(ctx)
	if err != nil {
		t.Errorf("Stats failed: %v", err)
	}
	// May be nil if not implemented
	_ = stats
}

func TestInstrumentedStore_GetMany(t *testing.T) {
	store := memory.NewStore()
	wrapped, _ := WrapStore(store, WithTracesEnabled(true), WithMetricsEnabled(true))

	ctx := context.Background()
	_ = wrapped.Connect(ctx)
	defer wrapped.Close(ctx)

	// Set values
	_, _ = wrapped.Set(ctx, "ns", "key1", config.NewValue("value1"))
	_, _ = wrapped.Set(ctx, "ns", "key2", config.NewValue("value2"))

	// GetMany
	results, err := wrapped.GetMany(ctx, "ns", []string{"key1", "key2", "key3"})
	if err != nil {
		t.Fatalf("GetMany failed: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results (key3 doesn't exist), got %d", len(results))
	}
}

func TestInstrumentedStore_SetMany(t *testing.T) {
	store := memory.NewStore()
	wrapped, _ := WrapStore(store, WithTracesEnabled(true), WithMetricsEnabled(true))

	ctx := context.Background()
	_ = wrapped.Connect(ctx)
	defer wrapped.Close(ctx)

	// SetMany
	values := map[string]config.Value{
		"key1": config.NewValue("value1"),
		"key2": config.NewValue("value2"),
	}
	if err := wrapped.SetMany(ctx, "ns", values); err != nil {
		t.Fatalf("SetMany failed: %v", err)
	}

	// Verify
	got, _ := wrapped.Get(ctx, "ns", "key1")
	str, _ := got.String()
	if str != "value1" {
		t.Errorf("Expected 'value1', got %s", str)
	}
}

func TestInstrumentedStore_DeleteMany(t *testing.T) {
	store := memory.NewStore()
	wrapped, _ := WrapStore(store, WithTracesEnabled(true), WithMetricsEnabled(true))

	ctx := context.Background()
	_ = wrapped.Connect(ctx)
	defer wrapped.Close(ctx)

	// Set values
	_, _ = wrapped.Set(ctx, "ns", "key1", config.NewValue("value1"))
	_, _ = wrapped.Set(ctx, "ns", "key2", config.NewValue("value2"))

	// DeleteMany
	deleted, err := wrapped.DeleteMany(ctx, "ns", []string{"key1", "key2"})
	if err != nil {
		t.Fatalf("DeleteMany failed: %v", err)
	}

	if deleted != 2 {
		t.Errorf("Expected 2 deleted, got %d", deleted)
	}

	// Verify deleted
	_, err = wrapped.Get(ctx, "ns", "key1")
	if !config.IsNotFound(err) {
		t.Error("Expected NotFound after delete")
	}
}

func TestInstrumentedStore_TracingDisabled(t *testing.T) {
	store := memory.NewStore()
	wrapped, _ := WrapStore(store, WithTracesEnabled(false), WithMetricsEnabled(true))

	ctx := context.Background()
	_ = wrapped.Connect(ctx)
	defer wrapped.Close(ctx)

	// Operations should still work without tracing
	val := config.NewValue("test")
	if _, err := wrapped.Set(ctx, "ns", "key", val); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	got, err := wrapped.Get(ctx, "ns", "key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	str, _ := got.String()
	if str != "test" {
		t.Errorf("Expected 'test', got %s", str)
	}
}

func TestInstrumentedStore_MetricsDisabled(t *testing.T) {
	store := memory.NewStore()
	wrapped, _ := WrapStore(store, WithTracesEnabled(true), WithMetricsEnabled(false))

	ctx := context.Background()
	_ = wrapped.Connect(ctx)
	defer wrapped.Close(ctx)

	// Operations should still work without metrics
	val := config.NewValue("test")
	if _, err := wrapped.Set(ctx, "ns", "key", val); err != nil {
		t.Fatalf("Set failed: %v", err)
	}
}

func TestErrorType(t *testing.T) {
	tests := []struct {
		err      error
		expected string
	}{
		{config.ErrNotFound, "not_found"},
		{config.ErrTypeMismatch, "type_mismatch"},
		{config.ErrInvalidKey, "internal"},
	}

	for _, tt := range tests {
		result := errorType(tt.err)
		if result != tt.expected {
			t.Errorf("errorType(%v) = %s, expected %s", tt.err, result, tt.expected)
		}
	}
}

// --- Additional coverage tests ---

// Test all operations with tracing disabled but metrics enabled (the !s.opts.enableTraces early-return paths).
func TestInstrumentedStore_TracingDisabledMetricsEnabled_AllOps(t *testing.T) {
	store := memory.NewStore()
	wrapped, err := WrapStore(store, WithTracesEnabled(false), WithMetricsEnabled(true))
	if err != nil {
		t.Fatalf("WrapStore failed: %v", err)
	}

	ctx := context.Background()

	// Connect
	if err := wrapped.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer wrapped.Close(ctx)

	// Set
	result, err := wrapped.Set(ctx, "ns", "key1", config.NewValue("value1"))
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if result == nil {
		t.Fatal("Set returned nil")
	}

	// Get
	got, err := wrapped.Get(ctx, "ns", "key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	str, _ := got.String()
	if str != "value1" {
		t.Errorf("Get = %q, want %q", str, "value1")
	}

	// Get not found (exercises error recording with metrics but no trace)
	_, err = wrapped.Get(ctx, "ns", "nonexistent")
	if !config.IsNotFound(err) {
		t.Errorf("Expected NotFound, got: %v", err)
	}

	// Find
	_, _ = wrapped.Set(ctx, "ns", "app/key2", config.NewValue("value2"))
	page, err := wrapped.Find(ctx, "ns", config.NewFilter().WithPrefix("app/").Build())
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}
	if len(page.Results()) != 1 {
		t.Errorf("Find expected 1 result, got %d", len(page.Results()))
	}

	// Watch
	ch, err := wrapped.Watch(ctx, config.WatchFilter{})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	if ch == nil {
		t.Error("Watch returned nil channel")
	}

	// Delete
	if err := wrapped.Delete(ctx, "ns", "key1"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Delete not found (exercises error recording)
	err = wrapped.Delete(ctx, "ns", "nonexistent")
	if !config.IsNotFound(err) {
		t.Errorf("Expected NotFound, got: %v", err)
	}

	// Health (tracing disabled path)
	if err := wrapped.Health(ctx); err != nil {
		t.Fatalf("Health failed: %v", err)
	}

	// Close
	if err := wrapped.Close(ctx); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

// Test all operations with both tracing AND metrics disabled (complete no-op paths).
func TestInstrumentedStore_BothDisabled_AllOps(t *testing.T) {
	store := memory.NewStore()
	wrapped, err := WrapStore(store, WithTracesEnabled(false), WithMetricsEnabled(false))
	if err != nil {
		t.Fatalf("WrapStore failed: %v", err)
	}

	ctx := context.Background()

	// Connect
	if err := wrapped.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	// Set
	result, err := wrapped.Set(ctx, "ns", "key1", config.NewValue("value1"))
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if result == nil {
		t.Fatal("Set returned nil result")
	}

	// Get
	got, err := wrapped.Get(ctx, "ns", "key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	str, _ := got.String()
	if str != "value1" {
		t.Errorf("Get = %q, want %q", str, "value1")
	}

	// Find
	page, err := wrapped.Find(ctx, "ns", config.NewFilter().Build())
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}
	if len(page.Results()) != 1 {
		t.Errorf("Find expected 1, got %d", len(page.Results()))
	}

	// Watch
	ch, err := wrapped.Watch(ctx, config.WatchFilter{})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	if ch == nil {
		t.Error("Watch returned nil channel")
	}

	// Delete
	if err := wrapped.Delete(ctx, "ns", "key1"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// GetMany (BulkStore, tracing/metrics both disabled)
	_, _ = wrapped.Set(ctx, "ns", "a", config.NewValue("va"))
	_, _ = wrapped.Set(ctx, "ns", "b", config.NewValue("vb"))
	results, err := wrapped.GetMany(ctx, "ns", []string{"a", "b"})
	if err != nil {
		t.Fatalf("GetMany failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("GetMany expected 2, got %d", len(results))
	}

	// SetMany
	if err := wrapped.SetMany(ctx, "ns", map[string]config.Value{
		"c": config.NewValue("vc"),
	}); err != nil {
		t.Fatalf("SetMany failed: %v", err)
	}

	// DeleteMany
	deleted, err := wrapped.DeleteMany(ctx, "ns", []string{"a", "b", "c"})
	if err != nil {
		t.Fatalf("DeleteMany failed: %v", err)
	}
	if deleted != 3 {
		t.Errorf("DeleteMany expected 3, got %d", deleted)
	}

	// Health
	if err := wrapped.Health(ctx); err != nil {
		t.Fatalf("Health failed: %v", err)
	}

	// Stats
	stats, err := wrapped.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}
	if stats == nil {
		t.Error("Stats returned nil")
	}

	// Close
	if err := wrapped.Close(ctx); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

// Test bulk operations with a non-BulkStore underlying store (fallback paths).
func TestInstrumentedStore_NonBulkStore_GetMany(t *testing.T) {
	store := newMinimalStore()
	wrapped, err := WrapStore(store, WithTracesEnabled(false), WithMetricsEnabled(true))
	if err != nil {
		t.Fatalf("WrapStore failed: %v", err)
	}

	ctx := context.Background()
	_ = wrapped.Connect(ctx)
	defer wrapped.Close(ctx)

	// Set values via the instrumented store (uses individual Set)
	_, _ = wrapped.Set(ctx, "ns", "key1", config.NewValue("value1"))
	_, _ = wrapped.Set(ctx, "ns", "key2", config.NewValue("value2"))

	// GetMany should fall back to individual Gets
	results, err := wrapped.GetMany(ctx, "ns", []string{"key1", "key2", "nonexistent"})
	if err != nil {
		t.Fatalf("GetMany failed: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}
}

func TestInstrumentedStore_NonBulkStore_SetMany(t *testing.T) {
	store := newMinimalStore()
	wrapped, err := WrapStore(store, WithTracesEnabled(false), WithMetricsEnabled(true))
	if err != nil {
		t.Fatalf("WrapStore failed: %v", err)
	}

	ctx := context.Background()
	_ = wrapped.Connect(ctx)
	defer wrapped.Close(ctx)

	// SetMany should fall back to individual Sets
	values := map[string]config.Value{
		"key1": config.NewValue("value1"),
		"key2": config.NewValue("value2"),
	}
	if err := wrapped.SetMany(ctx, "ns", values); err != nil {
		t.Fatalf("SetMany failed: %v", err)
	}

	// Verify both were set
	got1, err := wrapped.Get(ctx, "ns", "key1")
	if err != nil {
		t.Fatalf("Get key1 failed: %v", err)
	}
	str1, _ := got1.String()
	if str1 != "value1" {
		t.Errorf("key1 = %q, want %q", str1, "value1")
	}
}

func TestInstrumentedStore_NonBulkStore_DeleteMany(t *testing.T) {
	store := newMinimalStore()
	wrapped, err := WrapStore(store, WithTracesEnabled(false), WithMetricsEnabled(true))
	if err != nil {
		t.Fatalf("WrapStore failed: %v", err)
	}

	ctx := context.Background()
	_ = wrapped.Connect(ctx)
	defer wrapped.Close(ctx)

	// Set up data
	_, _ = wrapped.Set(ctx, "ns", "key1", config.NewValue("value1"))
	_, _ = wrapped.Set(ctx, "ns", "key2", config.NewValue("value2"))

	// DeleteMany should fall back to individual Deletes
	deleted, err := wrapped.DeleteMany(ctx, "ns", []string{"key1", "key2", "nonexistent"})
	if err != nil {
		t.Fatalf("DeleteMany failed: %v", err)
	}

	// Only 2 should succeed (nonexistent fails silently)
	if deleted != 2 {
		t.Errorf("Expected 2 deleted, got %d", deleted)
	}
}

// Test bulk operations with a non-BulkStore using tracing enabled.
func TestInstrumentedStore_NonBulkStore_WithTracing(t *testing.T) {
	store := newMinimalStore()
	wrapped, err := WrapStore(store, WithTracesEnabled(true), WithMetricsEnabled(true))
	if err != nil {
		t.Fatalf("WrapStore failed: %v", err)
	}

	ctx := context.Background()
	_ = wrapped.Connect(ctx)
	defer wrapped.Close(ctx)

	_, _ = wrapped.Set(ctx, "ns", "k1", config.NewValue("v1"))
	_, _ = wrapped.Set(ctx, "ns", "k2", config.NewValue("v2"))

	// GetMany fallback with tracing
	results, err := wrapped.GetMany(ctx, "ns", []string{"k1", "k2"})
	if err != nil {
		t.Fatalf("GetMany with tracing failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("GetMany expected 2, got %d", len(results))
	}

	// SetMany fallback with tracing
	if err := wrapped.SetMany(ctx, "ns", map[string]config.Value{
		"k3": config.NewValue("v3"),
	}); err != nil {
		t.Fatalf("SetMany with tracing failed: %v", err)
	}

	// DeleteMany fallback with tracing
	deleted, err := wrapped.DeleteMany(ctx, "ns", []string{"k1", "k2", "k3"})
	if err != nil {
		t.Fatalf("DeleteMany with tracing failed: %v", err)
	}
	if deleted != 3 {
		t.Errorf("DeleteMany expected 3, got %d", deleted)
	}
}

// Test bulk operations with a BulkStore underlying store, with tracing enabled.
// The existing tests already cover this partially, but let's be thorough.
func TestInstrumentedStore_BulkStore_WithTracingEnabled(t *testing.T) {
	store := memory.NewStore()
	wrapped, err := WrapStore(store, WithTracesEnabled(true), WithMetricsEnabled(true))
	if err != nil {
		t.Fatalf("WrapStore failed: %v", err)
	}

	ctx := context.Background()
	_ = wrapped.Connect(ctx)
	defer wrapped.Close(ctx)

	// SetMany with BulkStore + tracing
	values := map[string]config.Value{
		"bulk1": config.NewValue("v1"),
		"bulk2": config.NewValue("v2"),
		"bulk3": config.NewValue("v3"),
	}
	if err := wrapped.SetMany(ctx, "ns", values); err != nil {
		t.Fatalf("SetMany failed: %v", err)
	}

	// GetMany with BulkStore + tracing
	results, err := wrapped.GetMany(ctx, "ns", []string{"bulk1", "bulk2", "bulk3", "nonexistent"})
	if err != nil {
		t.Fatalf("GetMany failed: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("GetMany expected 3, got %d", len(results))
	}

	// DeleteMany with BulkStore + tracing
	deleted, err := wrapped.DeleteMany(ctx, "ns", []string{"bulk1", "bulk2", "bulk3"})
	if err != nil {
		t.Fatalf("DeleteMany failed: %v", err)
	}
	if deleted != 3 {
		t.Errorf("DeleteMany expected 3, got %d", deleted)
	}
}

// Test bulk operations with BulkStore + tracing disabled + metrics enabled.
func TestInstrumentedStore_BulkStore_TracingDisabledMetricsEnabled(t *testing.T) {
	store := memory.NewStore()
	wrapped, err := WrapStore(store, WithTracesEnabled(false), WithMetricsEnabled(true))
	if err != nil {
		t.Fatalf("WrapStore failed: %v", err)
	}

	ctx := context.Background()
	_ = wrapped.Connect(ctx)
	defer wrapped.Close(ctx)

	// SetMany
	if err := wrapped.SetMany(ctx, "ns", map[string]config.Value{
		"a": config.NewValue("va"),
		"b": config.NewValue("vb"),
	}); err != nil {
		t.Fatalf("SetMany failed: %v", err)
	}

	// GetMany
	results, err := wrapped.GetMany(ctx, "ns", []string{"a", "b"})
	if err != nil {
		t.Fatalf("GetMany failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("GetMany expected 2, got %d", len(results))
	}

	// DeleteMany
	deleted, err := wrapped.DeleteMany(ctx, "ns", []string{"a", "b"})
	if err != nil {
		t.Fatalf("DeleteMany failed: %v", err)
	}
	if deleted != 2 {
		t.Errorf("DeleteMany expected 2, got %d", deleted)
	}
}

// Test errorType with different error types, including wrapped errors.
func TestErrorType_Comprehensive(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{"not_found sentinel", config.ErrNotFound, "not_found"},
		{"type_mismatch sentinel", config.ErrTypeMismatch, "type_mismatch"},
		{"internal (invalid key)", config.ErrInvalidKey, "internal"},
		{"internal (store closed)", config.ErrStoreClosed, "internal"},
		{"internal (generic error)", errors.New("something broke"), "internal"},
		{"wrapped not_found", &config.KeyNotFoundError{Key: "k", Namespace: "ns"}, "not_found"},
		{"wrapped type_mismatch", &config.TypeMismatchError{Key: "k", Expected: config.TypeInt, Actual: config.TypeString}, "type_mismatch"},
		{"internal (fmt wrapped)", fmt.Errorf("oops: %w", config.ErrInvalidKey), "internal"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := errorType(tt.err)
			if result != tt.expected {
				t.Errorf("errorType(%v) = %q, expected %q", tt.err, result, tt.expected)
			}
		})
	}
}

// Test Health() with a store that doesn't implement HealthChecker.
func TestInstrumentedStore_Health_NotImplemented(t *testing.T) {
	store := newMinimalStore()

	// Test without tracing
	wrapped, err := WrapStore(store, WithTracesEnabled(false))
	if err != nil {
		t.Fatalf("WrapStore failed: %v", err)
	}

	ctx := context.Background()
	if err := wrapped.Health(ctx); err != nil {
		t.Errorf("Health with non-HealthChecker store should return nil, got: %v", err)
	}
}

func TestInstrumentedStore_Health_NotImplemented_WithTracing(t *testing.T) {
	store := newMinimalStore()

	wrapped, err := WrapStore(store, WithTracesEnabled(true))
	if err != nil {
		t.Fatalf("WrapStore failed: %v", err)
	}

	ctx := context.Background()
	if err := wrapped.Health(ctx); err != nil {
		t.Errorf("Health with non-HealthChecker store should return nil, got: %v", err)
	}
}

// Test Stats() with a store that doesn't implement StatsProvider.
func TestInstrumentedStore_Stats_NotImplemented(t *testing.T) {
	store := newMinimalStore()
	wrapped, err := WrapStore(store, WithTracesEnabled(false))
	if err != nil {
		t.Fatalf("WrapStore failed: %v", err)
	}

	ctx := context.Background()
	stats, err := wrapped.Stats(ctx)
	if err != nil {
		t.Errorf("Stats with non-StatsProvider store should return nil error, got: %v", err)
	}
	if stats != nil {
		t.Error("Stats with non-StatsProvider store should return nil stats")
	}
}

// Test WrapStore with custom tracer/meter names.
func TestWrapStore_CustomTracerMeterNames(t *testing.T) {
	store := memory.NewStore()
	wrapped, err := WrapStore(store,
		WithTracerName("custom-tracer"),
		WithMeterName("custom-meter"),
		WithTracesEnabled(true),
		WithMetricsEnabled(true),
	)
	if err != nil {
		t.Fatalf("WrapStore failed: %v", err)
	}
	if wrapped == nil {
		t.Fatal("WrapStore returned nil")
	}
}

// Test commonAttributes with and without service name.
func TestInstrumentedStore_CommonAttributes(t *testing.T) {
	store := memory.NewStore()

	// Without service name
	wrapped1, _ := WrapStore(store, WithBackendName("test-backend"))
	attrs1 := wrapped1.commonAttributes()
	if len(attrs1) != 1 {
		t.Errorf("Expected 1 attribute (no service name), got %d", len(attrs1))
	}

	// With service name
	wrapped2, _ := WrapStore(store, WithBackendName("test-backend"), WithServiceName("my-service"))
	attrs2 := wrapped2.commonAttributes()
	if len(attrs2) != 2 {
		t.Errorf("Expected 2 attributes (with service name), got %d", len(attrs2))
	}
}

// Test Set with error via instrumented store (tracing enabled, error path).
func TestInstrumentedStore_Set_Error_WithTracing(t *testing.T) {
	store := memory.NewStore()
	wrapped, _ := WrapStore(store, WithTracesEnabled(true), WithMetricsEnabled(true))

	ctx := context.Background()
	_ = wrapped.Connect(ctx)
	defer wrapped.Close(ctx)

	// Set with invalid namespace should fail
	_, err := wrapped.Set(ctx, "!bad!", "key", config.NewValue("val"))
	if err == nil {
		t.Error("Expected error for invalid namespace")
	}
}

// Test Delete error with tracing enabled.
func TestInstrumentedStore_Delete_Error_WithTracing(t *testing.T) {
	store := memory.NewStore()
	wrapped, _ := WrapStore(store, WithTracesEnabled(true), WithMetricsEnabled(true))

	ctx := context.Background()
	_ = wrapped.Connect(ctx)
	defer wrapped.Close(ctx)

	err := wrapped.Delete(ctx, "ns", "nonexistent")
	if !config.IsNotFound(err) {
		t.Errorf("Expected NotFound, got: %v", err)
	}
}

// Test Find error with tracing enabled.
func TestInstrumentedStore_Find_Error_WithTracing(t *testing.T) {
	store := memory.NewStore()
	wrapped, _ := WrapStore(store, WithTracesEnabled(true), WithMetricsEnabled(true))

	ctx := context.Background()
	_ = wrapped.Connect(ctx)
	defer wrapped.Close(ctx)

	_, err := wrapped.Find(ctx, "!bad!", config.NewFilter().Build())
	if err == nil {
		t.Error("Expected error for invalid namespace")
	}
}

// Test Watch error with tracing enabled.
func TestInstrumentedStore_Watch_Error_WithTracing(t *testing.T) {
	store := memory.NewStore()
	wrapped, _ := WrapStore(store, WithTracesEnabled(true), WithMetricsEnabled(true))

	ctx := context.Background()
	_ = wrapped.Connect(ctx)

	// Close store to force Watch error
	_ = wrapped.Close(ctx)

	_, err := wrapped.Watch(ctx, config.WatchFilter{})
	if err == nil {
		t.Error("Expected error for watch on closed store")
	}
}

// Test recordOperation with metrics disabled (no-op path).
func TestInstrumentedStore_RecordOperation_MetricsDisabled(t *testing.T) {
	store := memory.NewStore()
	wrapped, _ := WrapStore(store, WithTracesEnabled(false), WithMetricsEnabled(false))

	ctx := context.Background()
	_ = wrapped.Connect(ctx)
	defer wrapped.Close(ctx)

	// These operations go through the no-metrics path in recordOperation
	_, _ = wrapped.Set(ctx, "ns", "key", config.NewValue("val"))
	_, _ = wrapped.Get(ctx, "ns", "key")
	_ = wrapped.Delete(ctx, "ns", "key")
}

// Test default options (nothing enabled).
func TestWrapStore_DefaultOptions(t *testing.T) {
	store := memory.NewStore()
	wrapped, err := WrapStore(store)
	if err != nil {
		t.Fatalf("WrapStore failed: %v", err)
	}

	// Neither tracing nor metrics should be enabled
	if wrapped.opts.enableTraces {
		t.Error("Traces should be disabled by default")
	}
	if wrapped.opts.enableMetrics {
		t.Error("Metrics should be disabled by default")
	}

	// Operations should still work fine
	ctx := context.Background()
	_ = wrapped.Connect(ctx)
	defer wrapped.Close(ctx)

	_, err = wrapped.Set(ctx, "ns", "key", config.NewValue("val"))
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	got, err := wrapped.Get(ctx, "ns", "key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	str, _ := got.String()
	if str != "val" {
		t.Errorf("Get = %q, want %q", str, "val")
	}
}

// Test GetMany error path with tracing enabled on BulkStore.
func TestInstrumentedStore_GetMany_Error_WithTracing(t *testing.T) {
	store := memory.NewStore()
	wrapped, _ := WrapStore(store, WithTracesEnabled(true), WithMetricsEnabled(true))

	ctx := context.Background()
	_ = wrapped.Connect(ctx)

	// Close the underlying store to force errors
	_ = store.Close(ctx)

	_, err := wrapped.GetMany(ctx, "ns", []string{"key1"})
	if err == nil {
		t.Error("Expected error for GetMany on closed store")
	}
}

// Test SetMany error path with tracing enabled on BulkStore.
func TestInstrumentedStore_SetMany_Error_WithTracing(t *testing.T) {
	store := memory.NewStore()
	wrapped, _ := WrapStore(store, WithTracesEnabled(true), WithMetricsEnabled(true))

	ctx := context.Background()
	_ = wrapped.Connect(ctx)

	// Close underlying store
	_ = store.Close(ctx)

	err := wrapped.SetMany(ctx, "ns", map[string]config.Value{
		"key1": config.NewValue("val"),
	})
	if err == nil {
		t.Error("Expected error for SetMany on closed store")
	}
}

// Test DeleteMany error path with tracing enabled on BulkStore.
func TestInstrumentedStore_DeleteMany_Error_WithTracing(t *testing.T) {
	store := memory.NewStore()
	wrapped, _ := WrapStore(store, WithTracesEnabled(true), WithMetricsEnabled(true))

	ctx := context.Background()
	_ = wrapped.Connect(ctx)

	// Close underlying store
	_ = store.Close(ctx)

	_, err := wrapped.DeleteMany(ctx, "ns", []string{"key1"})
	if err == nil {
		t.Error("Expected error for DeleteMany on closed store")
	}
}

// Test Health with HealthChecker that returns error, tracing enabled.
func TestInstrumentedStore_Health_Error_WithTracing(t *testing.T) {
	store := memory.NewStore()
	wrapped, _ := WrapStore(store, WithTracesEnabled(true), WithMetricsEnabled(true))

	ctx := context.Background()

	// Close store so Health returns error
	_ = store.Close(ctx)

	err := wrapped.Health(ctx)
	if err == nil {
		t.Error("Expected health error on closed store")
	}
}

// Test the Set nil result path (tracing enabled, result is nil).
func TestInstrumentedStore_Set_ReturnsResult_WithTracing(t *testing.T) {
	store := memory.NewStore()
	wrapped, _ := WrapStore(store, WithTracesEnabled(true), WithMetricsEnabled(true))

	ctx := context.Background()
	_ = wrapped.Connect(ctx)
	defer wrapped.Close(ctx)

	result, err := wrapped.Set(ctx, "ns", "key", config.NewValue("hello"))
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if result == nil {
		t.Fatal("Expected non-nil result from Set")
	}
	if result.Metadata().Version() != 1 {
		t.Errorf("Expected version 1, got %d", result.Metadata().Version())
	}
}
