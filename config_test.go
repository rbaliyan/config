package config_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/memory"
)

func TestManagerBasicOperations(t *testing.T) {
	ctx := context.Background()

	// Create manager with memory store
	mgr := config.New(config.WithStore(memory.NewStore()))

	// Connect
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	// Get config for a namespace
	cfg := mgr.Namespace("test")

	// Set a value
	if err := cfg.Set(ctx, "app/timeout", 30); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Get the value
	val, err := cfg.Get(ctx, "app/timeout")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Cast to Val for type conversion
	v, ok := val.(*config.Val)
	if !ok {
		t.Fatalf("Expected *config.Val, got %T", val)
	}

	if v.Int() != 30 {
		t.Errorf("Expected 30, got %d", v.Int())
	}
}

func TestValueTypeConversions(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected any
		check    func(*config.Val) any
	}{
		{"int", 42, 42, func(v *config.Val) any { return v.Int() }},
		{"float", 3.14, 3.14, func(v *config.Val) any { return v.Float() }},
		{"string", "hello", "hello", func(v *config.Val) any { return v.StringValue() }},
		{"bool", true, true, func(v *config.Val) any { return v.BoolValue() }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val := config.NewValue(tt.input)
			result := tt.check(val)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestNamespaceIsolation(t *testing.T) {
	ctx := context.Background()

	mgr := config.New(config.WithStore(memory.NewStore()))
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	// Set same key in different namespaces
	prod := mgr.Namespace("production")
	dev := mgr.Namespace("development")

	if err := prod.Set(ctx, "timeout", 60); err != nil {
		t.Fatalf("Set prod failed: %v", err)
	}
	if err := dev.Set(ctx, "timeout", 10); err != nil {
		t.Fatalf("Set dev failed: %v", err)
	}

	// Verify values are isolated
	prodVal, _ := prod.Get(ctx, "timeout")
	devVal, _ := dev.Get(ctx, "timeout")

	if prodVal.(*config.Val).Int() != 60 {
		t.Errorf("Production timeout expected 60, got %d", prodVal.(*config.Val).Int())
	}
	if devVal.(*config.Val).Int() != 10 {
		t.Errorf("Development timeout expected 10, got %d", devVal.(*config.Val).Int())
	}
}

func TestDelete(t *testing.T) {
	ctx := context.Background()

	mgr := config.New(config.WithStore(memory.NewStore()))
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("test")

	// Set a value
	if err := cfg.Set(ctx, "to-delete", "value"); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Delete the value
	if err := cfg.Delete(ctx, "to-delete"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify it's gone
	_, err := cfg.Get(ctx, "to-delete")
	if !config.IsNotFound(err) {
		t.Errorf("Expected ErrNotFound, got %v", err)
	}
}

func TestFind(t *testing.T) {
	ctx := context.Background()

	mgr := config.New(config.WithStore(memory.NewStore()))
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("test")

	// Set multiple values
	cfg.Set(ctx, "app/db/host", "localhost")
	cfg.Set(ctx, "app/db/port", 5432)
	cfg.Set(ctx, "app/cache/ttl", 300)

	// Find with prefix filter
	page, err := cfg.Find(ctx, config.NewFilter().WithPrefix("app/db").Build())
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}

	entries := page.Results()
	if len(entries) != 2 {
		t.Errorf("Expected 2 entries with prefix 'app/db', got %d", len(entries))
	}
}

func TestContextHelpers(t *testing.T) {
	ctx := context.Background()

	mgr := config.New(config.WithStore(memory.NewStore()))
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	// Add manager to context
	ctx = config.ContextWithManager(ctx, mgr)
	ctx = config.ContextWithNamespace(ctx, "test")

	// Use context helpers
	if err := config.Set(ctx, "key", "value"); err != nil {
		t.Fatalf("Set via context failed: %v", err)
	}

	val, err := config.Get(ctx, "key")
	if err != nil {
		t.Fatalf("Get via context failed: %v", err)
	}

	strVal, err := val.String()
	if err != nil {
		t.Fatalf("String conversion failed: %v", err)
	}
	if strVal != "value" {
		t.Errorf("Expected 'value', got %s", strVal)
	}
}

// failingStore wraps a store and can simulate failures for resilience testing
type failingStore struct {
	config.Store
	failGet atomic.Bool
}

func (s *failingStore) Get(ctx context.Context, namespace, key string, tags ...config.Tag) (config.Value, error) {
	if s.failGet.Load() {
		return nil, errors.New("simulated store failure")
	}
	return s.Store.Get(ctx, namespace, key, tags...)
}

// TestCacheResilienceFallback verifies that when the store fails,
// previously cached values are still returned (resilience pattern).
func TestCacheResilienceFallback(t *testing.T) {
	ctx := context.Background()

	// Create a store that can simulate failures
	underlying := memory.NewStore()
	store := &failingStore{Store: underlying}

	mgr := config.New(config.WithStore(store))
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
	v := val.(*config.Val)
	if v.Int() != 30 {
		t.Errorf("Expected 30, got %d", v.Int())
	}

	// Simulate store failure
	store.failGet.Store(true)

	// Second Get - store fails, but should return cached value
	val, err = cfg.Get(ctx, "app/timeout")
	if err != nil {
		t.Fatalf("Get during store failure should return cached value, got error: %v", err)
	}
	v = val.(*config.Val)
	if v.Int() != 30 {
		t.Errorf("Expected cached value 30, got %d", v.Int())
	}

	// Restore store
	store.failGet.Store(false)

	// Third Get - should work normally again
	val, err = cfg.Get(ctx, "app/timeout")
	if err != nil {
		t.Fatalf("Get after store recovery failed: %v", err)
	}
	v = val.(*config.Val)
	if v.Int() != 30 {
		t.Errorf("Expected 30, got %d", v.Int())
	}
}

// TestCacheDoesNotHideNotFound verifies that NotFound errors are NOT
// hidden by the cache - if a key doesn't exist, NotFound is returned.
func TestCacheDoesNotHideNotFound(t *testing.T) {
	ctx := context.Background()

	mgr := config.New(config.WithStore(memory.NewStore()))
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("test")

	// Get a key that doesn't exist - should return NotFound, not a cached value
	_, err := cfg.Get(ctx, "nonexistent/key")
	if !config.IsNotFound(err) {
		t.Errorf("Expected NotFound error for nonexistent key, got: %v", err)
	}
}

// TestManagerHealth verifies the Health() method
func TestManagerHealth(t *testing.T) {
	ctx := context.Background()

	mgr := config.New(config.WithStore(memory.NewStore()))

	// Health should fail before connect
	if err := mgr.Health(ctx); err != config.ErrManagerClosed {
		t.Errorf("Expected ErrManagerClosed before connect, got: %v", err)
	}

	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	// Health should succeed after connect
	if err := mgr.Health(ctx); err != nil {
		t.Errorf("Health should succeed when connected, got: %v", err)
	}

	mgr.Close(ctx)

	// Health should fail after close
	if err := mgr.Health(ctx); err != config.ErrManagerClosed {
		t.Errorf("Expected ErrManagerClosed after close, got: %v", err)
	}
}

// TestManagerCacheStats verifies the CacheStats() method
func TestManagerCacheStats(t *testing.T) {
	ctx := context.Background()

	mgr := config.New(config.WithStore(memory.NewStore()))
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("test")

	// Set some values
	cfg.Set(ctx, "key1", "value1")
	cfg.Set(ctx, "key2", "value2")

	// Get values to populate cache hits
	cfg.Get(ctx, "key1")
	cfg.Get(ctx, "key1") // Second access should be from store (no external cache)

	stats := mgr.CacheStats()
	if stats.Size < 0 {
		t.Errorf("Cache size should be non-negative, got: %d", stats.Size)
	}
}

// TestManagerRefresh verifies the Refresh() method
func TestManagerRefresh(t *testing.T) {
	ctx := context.Background()

	mgr := config.New(config.WithStore(memory.NewStore()))
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("test")

	// Set a value
	cfg.Set(ctx, "app/timeout", 30)

	// Refresh should succeed for existing key
	if err := mgr.Refresh(ctx, "test", "app/timeout"); err != nil {
		t.Errorf("Refresh failed for existing key: %v", err)
	}

	// Refresh should fail for non-existing key
	err := mgr.Refresh(ctx, "test", "nonexistent")
	if !config.IsNotFound(err) {
		t.Errorf("Refresh should return NotFound for nonexistent key, got: %v", err)
	}
}

// TestManagerRefreshAll verifies the RefreshAll() method
func TestManagerRefreshAll(t *testing.T) {
	ctx := context.Background()

	mgr := config.New(config.WithStore(memory.NewStore()))
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("test")

	// Set values
	cfg.Set(ctx, "key1", "value1")
	cfg.Set(ctx, "key2", "value2")

	// RefreshAll should succeed
	if err := mgr.RefreshAll(ctx); err != nil {
		t.Errorf("RefreshAll failed: %v", err)
	}
}

// TestValidateKey tests key validation
func TestValidateKey(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		wantErr bool
	}{
		{"valid simple", "timeout", false},
		{"valid with slash", "app/timeout", false},
		{"valid with dots", "app.timeout", false},
		{"valid with dashes", "app-timeout", false},
		{"valid with underscore", "app_timeout", false},
		{"valid complex", "app/db/host-primary_1.local", false},
		{"empty", "", true},
		{"path traversal", "app/../secret", true},
		{"starts with slash", "/app/timeout", true},
		{"ends with slash", "app/timeout/", true},
		{"invalid characters", "app timeout", true},
		{"special chars", "app@timeout", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := config.ValidateKey(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateKey(%q) error = %v, wantErr = %v", tt.key, err, tt.wantErr)
			}
			if err != nil && tt.wantErr {
				if !config.IsInvalidKey(err) {
					t.Errorf("Expected IsInvalidKey to return true for error: %v", err)
				}
			}
		})
	}
}

// TestValidateNamespace tests namespace validation
func TestValidateNamespace(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		wantErr   bool
	}{
		{"empty (default)", "", false},
		{"simple", "production", false},
		{"with underscore", "my_namespace", false},
		{"with dash", "my-namespace", false},
		{"with numbers", "namespace123", false},
		{"invalid slash", "my/namespace", true},
		{"invalid space", "my namespace", true},
		{"invalid special", "my@namespace", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := config.ValidateNamespace(tt.namespace)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateNamespace(%q) error = %v, wantErr = %v", tt.namespace, err, tt.wantErr)
			}
		})
	}
}

// TestKeyValidationInOperations verifies key validation in Get/Set/Delete
func TestKeyValidationInOperations(t *testing.T) {
	ctx := context.Background()

	mgr := config.New(config.WithStore(memory.NewStore()))
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("test")

	// Test Set with invalid key
	err := cfg.Set(ctx, "", "value")
	if !config.IsInvalidKey(err) {
		t.Errorf("Set with empty key should return InvalidKey error, got: %v", err)
	}

	err = cfg.Set(ctx, "../secret", "value")
	if !config.IsInvalidKey(err) {
		t.Errorf("Set with path traversal should return InvalidKey error, got: %v", err)
	}

	// Test Get with invalid key
	_, err = cfg.Get(ctx, "")
	if !config.IsInvalidKey(err) {
		t.Errorf("Get with empty key should return InvalidKey error, got: %v", err)
	}

	// Test Delete with invalid key
	err = cfg.Delete(ctx, "")
	if !config.IsInvalidKey(err) {
		t.Errorf("Delete with empty key should return InvalidKey error, got: %v", err)
	}
}

// TestErrorTypes tests error type checking functions
func TestErrorTypes(t *testing.T) {
	// Test KeyNotFoundError
	notFoundErr := &config.KeyNotFoundError{Key: "test", Namespace: "ns"}
	if !config.IsNotFound(notFoundErr) {
		t.Error("IsNotFound should return true for KeyNotFoundError")
	}
	if !errors.Is(notFoundErr, config.ErrNotFound) {
		t.Error("KeyNotFoundError should unwrap to ErrNotFound")
	}

	// Test TypeMismatchError
	typeMismatchErr := &config.TypeMismatchError{Key: "test", Expected: config.TypeInt, Actual: config.TypeString}
	if !config.IsTypeMismatch(typeMismatchErr) {
		t.Error("IsTypeMismatch should return true for TypeMismatchError")
	}
	if !errors.Is(typeMismatchErr, config.ErrTypeMismatch) {
		t.Error("TypeMismatchError should unwrap to ErrTypeMismatch")
	}

	// Test InvalidKeyError
	invalidKeyErr := &config.InvalidKeyError{Key: "", Reason: "empty"}
	if !config.IsInvalidKey(invalidKeyErr) {
		t.Error("IsInvalidKey should return true for InvalidKeyError")
	}
	if !errors.Is(invalidKeyErr, config.ErrInvalidKey) {
		t.Error("InvalidKeyError should unwrap to ErrInvalidKey")
	}

	// Test StoreError
	storeErr := config.WrapStoreError("get", "memory", "key", errors.New("underlying"))
	var se *config.StoreError
	if !errors.As(storeErr, &se) {
		t.Error("WrapStoreError should return *StoreError")
	}
	if se.Op != "get" || se.Backend != "memory" || se.Key != "key" {
		t.Error("StoreError fields not set correctly")
	}

	// Test WrapStoreError with nil
	if config.WrapStoreError("op", "backend", "key", nil) != nil {
		t.Error("WrapStoreError should return nil for nil error")
	}

	// Test double-wrapping prevention
	doubleWrapped := config.WrapStoreError("op2", "backend2", "key2", storeErr)
	if doubleWrapped != storeErr {
		t.Error("WrapStoreError should not double-wrap StoreError")
	}
}

// TestFilterBuilder tests the filter builder
func TestFilterBuilder(t *testing.T) {
	// Test prefix filter
	f := config.NewFilter().WithPrefix("app/").WithLimit(100).Build()
	if f.Prefix() != "app/" {
		t.Errorf("Expected prefix 'app/', got %q", f.Prefix())
	}
	if f.Limit() != 100 {
		t.Errorf("Expected limit 100, got %d", f.Limit())
	}
	if len(f.Keys()) != 0 {
		t.Error("Expected empty keys for prefix filter")
	}

	// Test keys filter
	f = config.NewFilter().WithKeys("key1", "key2").Build()
	if len(f.Keys()) != 2 {
		t.Errorf("Expected 2 keys, got %d", len(f.Keys()))
	}
	if f.Prefix() != "" {
		t.Error("Expected empty prefix for keys filter")
	}

	// Test cursor
	f = config.NewFilter().WithCursor("123").Build()
	if f.Cursor() != "123" {
		t.Errorf("Expected cursor '123', got %q", f.Cursor())
	}

	// Test tags
	f = config.NewFilter().WithTags(config.MustTag("env", "prod")).Build()
	if len(f.Tags()) != 1 {
		t.Errorf("Expected 1 tag, got %d", len(f.Tags()))
	}

	// Test mutually exclusive - WithKeys clears prefix
	f = config.NewFilter().WithPrefix("app/").WithKeys("key1").Build()
	if f.Prefix() != "" {
		t.Error("WithKeys should clear prefix")
	}

	// Test mutually exclusive - WithPrefix clears keys
	f = config.NewFilter().WithKeys("key1").WithPrefix("app/").Build()
	if len(f.Keys()) != 0 {
		t.Error("WithPrefix should clear keys")
	}
}

// TestClosedManagerOperations verifies errors on closed manager
func TestClosedManagerOperations(t *testing.T) {
	ctx := context.Background()

	mgr := config.New(config.WithStore(memory.NewStore()))
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	cfg := mgr.Namespace("test")
	cfg.Set(ctx, "key", "value")

	// Close the manager
	mgr.Close(ctx)

	// All operations should fail
	_, err := cfg.Get(ctx, "key")
	if err != config.ErrManagerClosed {
		t.Errorf("Get on closed manager should return ErrManagerClosed, got: %v", err)
	}

	err = cfg.Set(ctx, "key2", "value2")
	if err != config.ErrManagerClosed {
		t.Errorf("Set on closed manager should return ErrManagerClosed, got: %v", err)
	}

	err = cfg.Delete(ctx, "key")
	if err != config.ErrManagerClosed {
		t.Errorf("Delete on closed manager should return ErrManagerClosed, got: %v", err)
	}

	_, err = cfg.Find(ctx, config.NewFilter().Build())
	if err != config.ErrManagerClosed {
		t.Errorf("Find on closed manager should return ErrManagerClosed, got: %v", err)
	}

	err = mgr.Refresh(ctx, "test", "key")
	if err != config.ErrManagerClosed {
		t.Errorf("Refresh on closed manager should return ErrManagerClosed, got: %v", err)
	}

	err = mgr.RefreshAll(ctx)
	if err != config.ErrManagerClosed {
		t.Errorf("RefreshAll on closed manager should return ErrManagerClosed, got: %v", err)
	}
}

// TestManagerConnectWithoutStore verifies connect fails without store
func TestManagerConnectWithoutStore(t *testing.T) {
	ctx := context.Background()

	mgr := config.New() // No store
	err := mgr.Connect(ctx)
	if err != config.ErrStoreNotConnected {
		t.Errorf("Connect without store should return ErrStoreNotConnected, got: %v", err)
	}
}

// TestTagOperations tests set/get/delete with tags
func TestTagOperations(t *testing.T) {
	ctx := context.Background()

	mgr := config.New(config.WithStore(memory.NewStore()))
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("test")

	// Set with tags
	err := cfg.Set(ctx, "db/host", "prod-db.example.com",
		config.WithTags(
			config.MustTag("env", "production"),
			config.MustTag("region", "us-west"),
		),
	)
	if err != nil {
		t.Fatalf("Set with tags failed: %v", err)
	}

	// Set same key with different tags (creates separate entry)
	err = cfg.Set(ctx, "db/host", "dev-db.example.com",
		config.WithTags(config.MustTag("env", "development")),
	)
	if err != nil {
		t.Fatalf("Set with different tags failed: %v", err)
	}

	// Get with tags (must match exactly)
	val, err := cfg.Get(ctx, "db/host",
		config.MustTag("env", "production"),
		config.MustTag("region", "us-west"),
	)
	if err != nil {
		t.Fatalf("Get with tags failed: %v", err)
	}
	strVal, _ := val.String()
	if strVal != "prod-db.example.com" {
		t.Errorf("Expected 'prod-db.example.com', got %q", strVal)
	}

	// Get with different tags
	val, err = cfg.Get(ctx, "db/host", config.MustTag("env", "development"))
	if err != nil {
		t.Fatalf("Get with different tags failed: %v", err)
	}
	strVal, _ = val.String()
	if strVal != "dev-db.example.com" {
		t.Errorf("Expected 'dev-db.example.com', got %q", strVal)
	}

	// Delete with specific tags
	err = cfg.Delete(ctx, "db/host", config.MustTag("env", "development"))
	if err != nil {
		t.Fatalf("Delete with tags failed: %v", err)
	}

	// Verify deleted
	_, err = cfg.Get(ctx, "db/host", config.MustTag("env", "development"))
	if !config.IsNotFound(err) {
		t.Errorf("Expected NotFound after delete, got: %v", err)
	}

	// Production entry should still exist
	_, err = cfg.Get(ctx, "db/host",
		config.MustTag("env", "production"),
		config.MustTag("region", "us-west"),
	)
	if err != nil {
		t.Errorf("Production entry should still exist, got: %v", err)
	}
}

// TestConcurrentAccess tests concurrent read/write operations
func TestConcurrentAccess(t *testing.T) {
	ctx := context.Background()

	mgr := config.New(config.WithStore(memory.NewStore()))
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("test")

	// Set initial value
	cfg.Set(ctx, "counter", 0)

	var wg sync.WaitGroup
	numReaders := 10
	numWriters := 5
	iterations := 100

	// Start readers
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_, _ = cfg.Get(ctx, "counter")
			}
		}()
	}

	// Start writers
	var writeCount atomic.Int64
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				count := writeCount.Add(1)
				_ = cfg.Set(ctx, "counter", count)
			}
		}()
	}

	wg.Wait()

	// Verify final value exists
	val, err := cfg.Get(ctx, "counter")
	if err != nil {
		t.Fatalf("Final Get failed: %v", err)
	}

	v := val.(*config.Val)
	if v.Int() == 0 {
		t.Error("Counter should have been incremented")
	}
}

// TestNamespaceReuse verifies namespace instances are reused
func TestNamespaceReuse(t *testing.T) {
	ctx := context.Background()

	mgr := config.New(config.WithStore(memory.NewStore()))
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	// Get same namespace multiple times
	cfg1 := mgr.Namespace("test")
	cfg2 := mgr.Namespace("test")

	// They should be the same instance
	if cfg1 != cfg2 {
		t.Error("Expected same namespace instance to be reused")
	}

	// Different namespaces should be different
	cfg3 := mgr.Namespace("other")
	if cfg1 == cfg3 {
		t.Error("Different namespaces should return different instances")
	}
}

// TestFindWithPagination tests Find with pagination
func TestFindWithPagination(t *testing.T) {
	ctx := context.Background()

	mgr := config.New(config.WithStore(memory.NewStore()))
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("test")

	// Create multiple entries
	for i := 0; i < 10; i++ {
		cfg.Set(ctx, "key"+string(rune('a'+i)), i)
	}

	// Get first page
	page, err := cfg.Find(ctx, config.NewFilter().WithLimit(3).Build())
	if err != nil {
		t.Fatalf("First page failed: %v", err)
	}
	if len(page.Results()) != 3 {
		t.Errorf("Expected 3 results, got %d", len(page.Results()))
	}

	// Get second page using cursor
	cursor := page.NextCursor()
	if cursor == "" {
		t.Error("Expected non-empty cursor")
	}

	page2, err := cfg.Find(ctx, config.NewFilter().WithLimit(3).WithCursor(cursor).Build())
	if err != nil {
		t.Fatalf("Second page failed: %v", err)
	}
	if len(page2.Results()) != 3 {
		t.Errorf("Expected 3 results, got %d", len(page2.Results()))
	}
}

// TestFindWithTags tests Find with tag filtering
func TestFindWithTags(t *testing.T) {
	ctx := context.Background()

	mgr := config.New(config.WithStore(memory.NewStore()))
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("test")

	// Create entries with different tags
	cfg.Set(ctx, "db/host", "prod-db",
		config.WithTags(config.MustTag("env", "prod")))
	cfg.Set(ctx, "db/port", "5432",
		config.WithTags(config.MustTag("env", "prod")))
	cfg.Set(ctx, "db/host", "dev-db",
		config.WithTags(config.MustTag("env", "dev")))

	// Find all prod entries
	page, err := cfg.Find(ctx, config.NewFilter().
		WithPrefix("db/").
		WithTags(config.MustTag("env", "prod")).
		Build())
	if err != nil {
		t.Fatalf("Find with tags failed: %v", err)
	}

	if len(page.Results()) != 2 {
		t.Errorf("Expected 2 prod entries, got %d", len(page.Results()))
	}
}

// TestValueMetadata tests value metadata
func TestValueMetadata(t *testing.T) {
	ctx := context.Background()

	mgr := config.New(config.WithStore(memory.NewStore()))
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("test")

	// Set a value
	before := time.Now()
	cfg.Set(ctx, "key", "value")
	after := time.Now()

	// Get and check metadata
	val, _ := cfg.Get(ctx, "key")
	meta := val.Metadata()

	if meta == nil {
		t.Fatal("Expected non-nil metadata")
	}

	if meta.Version() != 1 {
		t.Errorf("Expected version 1, got %d", meta.Version())
	}

	if meta.CreatedAt().Before(before) || meta.CreatedAt().After(after) {
		t.Error("CreatedAt should be between before and after")
	}

	// Update and check version increments
	time.Sleep(time.Millisecond) // Ensure time difference
	cfg.Set(ctx, "key", "value2")

	val, _ = cfg.Get(ctx, "key")
	meta = val.Metadata()

	if meta.Version() != 2 {
		t.Errorf("Expected version 2 after update, got %d", meta.Version())
	}

	if !meta.UpdatedAt().After(meta.CreatedAt()) {
		t.Error("UpdatedAt should be after CreatedAt")
	}
}
