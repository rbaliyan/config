package otel

import (
	"context"
	"testing"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/memory"
)

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
	wrapped.Connect(ctx)
	if err := wrapped.Close(ctx); err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestInstrumentedStore_Get(t *testing.T) {
	store := memory.NewStore()
	wrapped, _ := WrapStore(store, WithTracesEnabled(true), WithMetricsEnabled(true))

	ctx := context.Background()
	wrapped.Connect(ctx)
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
	wrapped.Connect(ctx)
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
	wrapped.Connect(ctx)
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
	wrapped.Connect(ctx)
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
	wrapped.Connect(ctx)
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
	wrapped.Connect(ctx)
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
	wrapped.Connect(ctx)
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
	wrapped.Connect(ctx)
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
	wrapped.Connect(ctx)
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
	wrapped.Connect(ctx)
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
	wrapped.Connect(ctx)
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
	wrapped.Connect(ctx)
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
	wrapped.Connect(ctx)
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
