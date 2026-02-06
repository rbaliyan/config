package config_test

import (
	"context"
	"errors"
	"fmt"
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
	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

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

	// Use Value interface methods
	i, err := val.Int64()
	if err != nil {
		t.Fatalf("Int64 failed: %v", err)
	}
	if i != 30 {
		t.Errorf("Expected 30, got %d", i)
	}
}

func TestValueTypeConversions(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected any
		check    func(config.Value) any
	}{
		{"int", 42, int64(42), func(v config.Value) any { i, _ := v.Int64(); return i }},
		{"float", 3.14, 3.14, func(v config.Value) any { f, _ := v.Float64(); return f }},
		{"string", "hello", "hello", func(v config.Value) any { s, _ := v.String(); return s }},
		{"bool", true, true, func(v config.Value) any { b, _ := v.Bool(); return b }},
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

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
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

	prodInt, _ := prodVal.Int64()
	if prodInt != 60 {
		t.Errorf("Production timeout expected 60, got %d", prodInt)
	}
	devInt, _ := devVal.Int64()
	if devInt != 10 {
		t.Errorf("Development timeout expected 10, got %d", devInt)
	}
}

func TestDelete(t *testing.T) {
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

	// Set a value
	if err := cfg.Set(ctx, "to-delete", "value"); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Delete the value
	if err := cfg.Delete(ctx, "to-delete"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify it's gone
	_, getErr := cfg.Get(ctx, "to-delete")
	if !config.IsNotFound(getErr) {
		t.Errorf("Expected ErrNotFound, got %v", err)
	}
}

func TestFind(t *testing.T) {
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

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
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

func (s *failingStore) Get(ctx context.Context, namespace, key string) (config.Value, error) {
	if s.failGet.Load() {
		return nil, errors.New("simulated store failure")
	}
	return s.Store.Get(ctx, namespace, key)
}

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

// TestManagerHealth verifies the Health() method
func TestManagerHealth(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

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

// TestManagerRefresh verifies the Refresh() method
func TestManagerRefresh(t *testing.T) {
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

	// Set a value
	cfg.Set(ctx, "app/timeout", 30)

	// Refresh should succeed for existing key
	if err := mgr.Refresh(ctx, "test", "app/timeout"); err != nil {
		t.Errorf("Refresh failed for existing key: %v", err)
	}

	// Refresh should fail for non-existing key
	err = mgr.Refresh(ctx, "test", "nonexistent")
	if !config.IsNotFound(err) {
		t.Errorf("Refresh should return NotFound for nonexistent key, got: %v", err)
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

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("test")

	// Test Set with invalid key
	err = cfg.Set(ctx, "", "value")
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

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	cfg := mgr.Namespace("test")
	cfg.Set(ctx, "key", "value")

	// Close the manager
	mgr.Close(ctx)

	// All operations should fail
	_, err = cfg.Get(ctx, "key")
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
}

// TestManagerConnectWithoutStore verifies connect fails without store
func TestManagerConnectWithoutStore(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New() // No store
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	err = mgr.Connect(ctx)
	if err != config.ErrStoreNotConnected {
		t.Errorf("Connect without store should return ErrStoreNotConnected, got: %v", err)
	}
}

// TestConcurrentAccess tests concurrent read/write operations
func TestConcurrentAccess(t *testing.T) {
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

	i, _ := val.Int64()
	if i == 0 {
		t.Error("Counter should have been incremented")
	}
}

// TestNamespaceReuse verifies namespace instances are reused
func TestNamespaceReuse(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
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

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
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

// TestValueMetadata tests value metadata
func TestValueMetadata(t *testing.T) {
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

// TestSetWithIfNotExists tests the WithIfNotExists option
func TestSetWithIfNotExists(t *testing.T) {
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

	// First set with IfNotExists should succeed
	err = cfg.Set(ctx, "lock/owner", "instance-1", config.WithIfNotExists())
	if err != nil {
		t.Fatalf("First Set with IfNotExists failed: %v", err)
	}

	// Second set with IfNotExists should fail
	err = cfg.Set(ctx, "lock/owner", "instance-2", config.WithIfNotExists())
	if !config.IsKeyExists(err) {
		t.Errorf("Expected KeyExists error, got: %v", err)
	}

	// Verify original value is unchanged
	val, _ := cfg.Get(ctx, "lock/owner")
	str, _ := val.String()
	if str != "instance-1" {
		t.Errorf("Value = %q, expected %q", str, "instance-1")
	}
}

// TestSetWithIfExists tests the WithIfExists option
func TestSetWithIfExists(t *testing.T) {
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

	// Set with IfExists should fail for non-existing key
	err = cfg.Set(ctx, "app/timeout", 60, config.WithIfExists())
	if !config.IsNotFound(err) {
		t.Errorf("Expected NotFound error, got: %v", err)
	}

	// Create the key normally
	cfg.Set(ctx, "app/timeout", 30)

	// Now set with IfExists should succeed
	err = cfg.Set(ctx, "app/timeout", 60, config.WithIfExists())
	if err != nil {
		t.Fatalf("Set with IfExists failed: %v", err)
	}

	// Verify value was updated
	val, _ := cfg.Get(ctx, "app/timeout")
	i, _ := val.Int64()
	if i != 60 {
		t.Errorf("Value = %d, expected %d", i, 60)
	}
}

// TestKeyExistsError tests the KeyExistsError type
func TestKeyExistsError(t *testing.T) {
	err := &config.KeyExistsError{Key: "test", Namespace: "ns"}

	// Should be detected by IsKeyExists
	if !config.IsKeyExists(err) {
		t.Error("IsKeyExists should return true for KeyExistsError")
	}

	// Should unwrap to ErrKeyExists
	if !errors.Is(err, config.ErrKeyExists) {
		t.Error("KeyExistsError should unwrap to ErrKeyExists")
	}

	// Should have correct error message with namespace
	expected := `config: key "test" already exists in namespace "ns"`
	if err.Error() != expected {
		t.Errorf("Error message = %q, expected %q", err.Error(), expected)
	}

	// Without namespace
	err2 := &config.KeyExistsError{Key: "test"}
	expected2 := `config: key "test" already exists`
	if err2.Error() != expected2 {
		t.Errorf("Error message = %q, expected %q", err2.Error(), expected2)
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

func TestTypeHelpers(t *testing.T) {
	// String()
	tests := []struct {
		typ  config.Type
		want string
	}{
		{config.TypeInt, "int"},
		{config.TypeFloat, "float"},
		{config.TypeString, "string"},
		{config.TypeBool, "bool"},
		{config.TypeMapStringInt, "map[string]int"},
		{config.TypeMapStringFloat, "map[string]float64"},
		{config.TypeMapStringString, "map[string]string"},
		{config.TypeListInt, "[]int"},
		{config.TypeListFloat, "[]float64"},
		{config.TypeListString, "[]string"},
		{config.TypeCustom, "custom"},
		{config.TypeUnknown, "unknown(0)"},
	}

	for _, tt := range tests {
		if got := tt.typ.String(); got != tt.want {
			t.Errorf("Type(%d).String() = %q, want %q", tt.typ, got, tt.want)
		}
	}

	// ParseType round-trips
	for _, tt := range tests {
		if tt.typ == config.TypeUnknown {
			continue // unknown doesn't round-trip
		}
		parsed := config.ParseType(tt.want)
		if parsed != tt.typ {
			t.Errorf("ParseType(%q) = %v, want %v", tt.want, parsed, tt.typ)
		}
	}

	// IsPrimitive
	if !config.TypeInt.IsPrimitive() {
		t.Error("TypeInt should be primitive")
	}
	if config.TypeMapStringInt.IsPrimitive() {
		t.Error("TypeMapStringInt should not be primitive")
	}

	// IsMap
	if !config.TypeMapStringString.IsMap() {
		t.Error("TypeMapStringString should be a map")
	}
	if config.TypeInt.IsMap() {
		t.Error("TypeInt should not be a map")
	}

	// IsList
	if !config.TypeListInt.IsList() {
		t.Error("TypeListInt should be a list")
	}
	if config.TypeString.IsList() {
		t.Error("TypeString should not be a list")
	}
}

func TestWriteModeString(t *testing.T) {
	tests := []struct {
		mode config.WriteMode
		want string
	}{
		{config.WriteModeUpsert, "upsert"},
		{config.WriteModeCreate, "create"},
		{config.WriteModeUpdate, "update"},
		{config.WriteMode(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.mode.String(); got != tt.want {
			t.Errorf("WriteMode(%d).String() = %q, want %q", tt.mode, got, tt.want)
		}
	}
}

func TestChangeTypeString(t *testing.T) {
	if config.ChangeTypeSet.String() != "set" {
		t.Errorf("ChangeTypeSet.String() = %q, want %q", config.ChangeTypeSet.String(), "set")
	}
	if config.ChangeTypeDelete.String() != "delete" {
		t.Errorf("ChangeTypeDelete.String() = %q, want %q", config.ChangeTypeDelete.String(), "delete")
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
	cfg.Set(ctx, "key", "value")
	cfg.Get(ctx, "key")
	cfg.Get(ctx, "key") // cache hit

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

func TestWatchStatus(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	obs, ok := mgr.(config.ManagerObserver)
	if !ok {
		t.Fatal("Manager should implement ManagerObserver")
	}

	ws := obs.WatchStatus()
	if !ws.Connected {
		t.Error("Expected Connected = true")
	}
}

func TestValueUnmarshal(t *testing.T) {
	val := config.NewValue(map[string]any{"name": "test", "count": float64(42)})

	var result map[string]any
	if err := val.Unmarshal(&result); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if result["name"] != "test" {
		t.Errorf("name = %v, want %q", result["name"], "test")
	}
}

func TestValueTypeConversionsExtended(t *testing.T) {
	// Bool conversion
	val := config.NewValue(true)
	b, err := val.Bool()
	if err != nil || !b {
		t.Errorf("Bool() = %v, %v; want true, nil", b, err)
	}

	// Float conversion
	val = config.NewValue(3.14)
	f, err := val.Float64()
	if err != nil || f != 3.14 {
		t.Errorf("Float64() = %v, %v; want 3.14, nil", f, err)
	}

	// Int from float (exact)
	val = config.NewValue(42.0)
	i, err := val.Int64()
	if err != nil {
		t.Errorf("Int64() from exact float failed: %v", err)
	}
	if i != 42 {
		t.Errorf("Int64() = %d, want 42", i)
	}

	// String from non-string
	val = config.NewValue(42)
	s, err := val.String()
	if err != nil {
		t.Errorf("String() from int failed: %v", err)
	}
	if s != "42" {
		t.Errorf("String() = %q, want %q", s, "42")
	}
}

func TestValueMarshal(t *testing.T) {
	val := config.NewValue("hello")
	data, err := val.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	if len(data) == 0 {
		t.Error("Marshal returned empty data")
	}
}

func TestGetWriteMode(t *testing.T) {
	// Value without write mode
	val := config.NewValue("test")
	mode := config.GetWriteMode(val)
	if mode != config.WriteModeUpsert {
		t.Errorf("Default write mode = %v, want Upsert", mode)
	}

	// Value with explicit write mode
	val = config.NewValue("test", config.WithValueWriteMode(config.WriteModeCreate))
	mode = config.GetWriteMode(val)
	if mode != config.WriteModeCreate {
		t.Errorf("Write mode = %v, want Create", mode)
	}
}

func TestConfigNamespace(t *testing.T) {
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
	if cfg.Namespace() != "test" {
		t.Errorf("Namespace() = %q, want %q", cfg.Namespace(), "test")
	}
}

func TestNewValueTypeDetection(t *testing.T) {
	tests := []struct {
		input any
		want  config.Type
	}{
		{42, config.TypeInt},
		{int64(42), config.TypeInt},
		{uint(42), config.TypeInt},
		{3.14, config.TypeFloat},
		{float32(3.14), config.TypeFloat},
		{42.0, config.TypeInt}, // exact float → int (JSON compat)
		{"hello", config.TypeString},
		{true, config.TypeBool},
		// Slices, maps, and nil are all TypeCustom
		{[]int{1, 2}, config.TypeCustom},
		{map[string]int{"a": 1}, config.TypeCustom},
		{struct{}{}, config.TypeCustom},
	}

	for _, tt := range tests {
		got := config.NewValue(tt.input).Type()
		if got != tt.want {
			t.Errorf("NewValue(%T).Type() = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestStoreError(t *testing.T) {
	inner := errors.New("connection refused")
	err := &config.StoreError{
		Op:      "get",
		Backend: "postgres",
		Key:     "app/name",
		Err:     inner,
	}

	if !errors.Is(err, inner) {
		t.Error("StoreError should unwrap to inner error")
	}

	msg := err.Error()
	if msg == "" {
		t.Error("StoreError.Error() should not be empty")
	}
}

func TestInvalidKeyError(t *testing.T) {
	err := &config.InvalidKeyError{Key: "../bad", Reason: "contains path traversal"}
	if !errors.Is(err, config.ErrInvalidKey) {
		t.Error("InvalidKeyError should unwrap to ErrInvalidKey")
	}

	msg := err.Error()
	if msg == "" {
		t.Error("InvalidKeyError.Error() should not be empty")
	}
}

func TestTypeMismatchError(t *testing.T) {
	err := &config.TypeMismatchError{
		Key:      "count",
		Expected: config.TypeInt,
		Actual:   config.TypeString,
	}
	if !errors.Is(err, config.ErrTypeMismatch) {
		t.Error("TypeMismatchError should unwrap to ErrTypeMismatch")
	}

	msg := err.Error()
	if msg == "" {
		t.Error("TypeMismatchError.Error() should not be empty")
	}
}

func TestManagerConnectTwice(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	// First connect
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("First Connect failed: %v", err)
	}

	// Second connect should be idempotent (already connected)
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Second Connect failed: %v", err)
	}

	mgr.Close(ctx)

	// Connect after close should fail
	err = mgr.Connect(ctx)
	if !errors.Is(err, config.ErrManagerClosed) {
		t.Errorf("Connect after Close = %v, want ErrManagerClosed", err)
	}
}

func TestNewValueFromBytesError(t *testing.T) {
	// Invalid codec name falls back to default
	val, err := config.NewValueFromBytes([]byte(`"hello"`), "nonexistent")
	if err != nil {
		t.Fatalf("NewValueFromBytes with unknown codec should fallback, got: %v", err)
	}
	if val == nil {
		t.Error("Expected non-nil value")
	}

	// Invalid bytes should error
	_, err = config.NewValueFromBytes([]byte("not-json"), "json")
	if err == nil {
		t.Error("Expected error for invalid JSON bytes")
	}
}

func TestErrorHelpers(t *testing.T) {
	// IsNotFound
	if !config.IsNotFound(config.ErrNotFound) {
		t.Error("IsNotFound should be true for ErrNotFound")
	}
	if !config.IsNotFound(&config.KeyNotFoundError{Key: "k"}) {
		t.Error("IsNotFound should be true for KeyNotFoundError")
	}
	if config.IsNotFound(errors.New("other")) {
		t.Error("IsNotFound should be false for random error")
	}

	// Wrapped errors
	if !config.IsNotFound(fmt.Errorf("wrapped: %w", config.ErrNotFound)) {
		t.Error("IsNotFound should work with wrapped errors")
	}

	// IsKeyExists
	if !config.IsKeyExists(config.ErrKeyExists) {
		t.Error("IsKeyExists should be true for ErrKeyExists")
	}
	if config.IsKeyExists(errors.New("other")) {
		t.Error("IsKeyExists should be false for random error")
	}

	// WrapStoreError
	wrapped := config.WrapStoreError("get", "memory", "mykey", errors.New("fail"))
	if wrapped == nil {
		t.Fatal("WrapStoreError should return non-nil")
	}
	var storeErr *config.StoreError
	if !errors.As(wrapped, &storeErr) {
		t.Error("WrapStoreError should return *StoreError")
	}

	// WrapStoreError with nil should return nil
	if config.WrapStoreError("get", "memory", "k", nil) != nil {
		t.Error("WrapStoreError(nil) should return nil")
	}

	// WrapStoreError should not double-wrap
	rewrapped := config.WrapStoreError("set", "memory", "k", wrapped)
	if rewrapped != wrapped {
		t.Error("WrapStoreError should not double-wrap StoreError")
	}
}
