package config_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/codec"
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
	_ = cfg.Set(ctx, "app/db/host", "localhost")
	_ = cfg.Set(ctx, "app/db/port", 5432)
	_ = cfg.Set(ctx, "app/cache/ttl", 300)

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

	_ = mgr.Close(ctx)

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
	_ = cfg.Set(ctx, "app/timeout", 30)

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
	_ = cfg.Set(ctx, "key", "value")

	// Close the manager
	_ = mgr.Close(ctx)

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
	_ = cfg.Set(ctx, "counter", 0)

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
		_ = cfg.Set(ctx, "key"+string(rune('a'+i)), i)
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
	_ = cfg.Set(ctx, "key", "value")
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
	_ = cfg.Set(ctx, "key", "value2")

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
	_ = cfg.Set(ctx, "app/timeout", 30)

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

	_ = mgr.Close(ctx)

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

// --- Additional tests for coverage ---

// TestContextHelpersNoManager tests context helpers when no manager is set.
func TestContextHelpersNoManager(t *testing.T) {
	ctx := context.Background()

	// Get without manager in context
	_, err := config.Get(ctx, "key")
	if err != config.ErrManagerClosed {
		t.Errorf("Get without manager should return ErrManagerClosed, got: %v", err)
	}

	// Set without manager in context
	err = config.Set(ctx, "key", "value")
	if err != config.ErrManagerClosed {
		t.Errorf("Set without manager should return ErrManagerClosed, got: %v", err)
	}
}

// TestContextManagerReturnsNil tests ContextManager returns nil for missing manager.
func TestContextManagerReturnsNil(t *testing.T) {
	ctx := context.Background()
	mgr := config.ContextManager(ctx)
	if mgr != nil {
		t.Error("ContextManager should return nil when no manager in context")
	}
}

// TestContextNamespaceReturnsEmpty tests ContextNamespace returns empty for missing namespace.
func TestContextNamespaceReturnsEmpty(t *testing.T) {
	ctx := context.Background()
	ns := config.ContextNamespace(ctx)
	if ns != "" {
		t.Errorf("ContextNamespace should return empty string, got: %q", ns)
	}
}

// TestContextGetWithDefaultNamespace tests Get uses default namespace when none is set.
func TestContextGetWithDefaultNamespace(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	// Set via default namespace
	cfg := mgr.Namespace("")
	_ = cfg.Set(ctx, "key", "value")

	// Use context helper without setting namespace (defaults to "")
	ctx = config.ContextWithManager(ctx, mgr)
	val, err := config.Get(ctx, "key")
	if err != nil {
		t.Fatalf("Get via context failed: %v", err)
	}
	s, _ := val.String()
	if s != "value" {
		t.Errorf("Expected 'value', got %q", s)
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

// customValue is a Value implementation that is NOT the internal *val type,
// used to test the staleValueWrapper path in MarkStale.
type customValue struct {
	raw  string
	meta *customMetadata
}

type customMetadata struct {
	version int64
}

func (m *customMetadata) Version() int64       { return m.version }
func (m *customMetadata) CreatedAt() time.Time { return time.Time{} }
func (m *customMetadata) UpdatedAt() time.Time { return time.Time{} }
func (m *customMetadata) IsStale() bool        { return false }

func (v *customValue) Marshal() ([]byte, error) { return json.Marshal(v.raw) }
func (v *customValue) Unmarshal(target any) error {
	data, _ := json.Marshal(v.raw)
	return json.Unmarshal(data, target)
}
func (v *customValue) Type() config.Type         { return config.TypeString }
func (v *customValue) Codec() string             { return "json" }
func (v *customValue) Metadata() config.Metadata { return v.meta }
func (v *customValue) Int64() (int64, error) {
	return 0, errors.New("not an int")
}
func (v *customValue) Float64() (float64, error) {
	return 0, errors.New("not a float")
}
func (v *customValue) String() (string, error) {
	return v.raw, nil
}
func (v *customValue) Bool() (bool, error) {
	return false, errors.New("not a bool")
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

// TestProcessWatchEventsSetAndDelete tests that watch events update the cache.
func TestProcessWatchEventsSetAndDelete(t *testing.T) {
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

	// Set and Get a value
	_ = cfg.Set(ctx, "app/name", "myapp")
	val, err := cfg.Get(ctx, "app/name")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	s, _ := val.String()
	if s != "myapp" {
		t.Errorf("Expected 'myapp', got %q", s)
	}

	// Delete it
	if err := cfg.Delete(ctx, "app/name"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Wait a moment for watch events to propagate
	time.Sleep(50 * time.Millisecond)

	// Verify deletion
	_, getErr := cfg.Get(ctx, "app/name")
	if !config.IsNotFound(getErr) {
		t.Errorf("Expected NotFound after delete, got: %v", getErr)
	}
}

// TestWatchHealthErrorMessages tests WatchHealthError message formatting.
func TestWatchHealthErrorMessages(t *testing.T) {
	// With last error
	err1 := &config.WatchHealthError{
		ConsecutiveFailures: 5,
		LastError:           "connection refused",
	}
	msg1 := err1.Error()
	if msg1 == "" {
		t.Error("WatchHealthError.Error() should not be empty")
	}
	if !config.IsWatchUnhealthy(err1) {
		t.Error("IsWatchUnhealthy should return true for WatchHealthError")
	}

	// Without last error
	err2 := &config.WatchHealthError{
		ConsecutiveFailures: 3,
		LastError:           "",
	}
	msg2 := err2.Error()
	if msg2 == "" {
		t.Error("WatchHealthError.Error() without LastError should not be empty")
	}

	// Verify unwrap
	if !errors.Is(err1, config.ErrWatchUnhealthy) {
		t.Error("WatchHealthError should unwrap to ErrWatchUnhealthy")
	}
}

// TestBulkWriteErrorMethods tests BulkWriteError helper methods.
func TestBulkWriteErrorMethods(t *testing.T) {
	bwe := &config.BulkWriteError{
		Errors: map[string]error{
			"key1": errors.New("fail1"),
			"key2": errors.New("fail2"),
		},
		Succeeded: []string{"key3", "key4"},
	}

	// Error message
	msg := bwe.Error()
	if msg == "" {
		t.Error("BulkWriteError.Error() should not be empty")
	}

	// Unwrap
	if !errors.Is(bwe, config.ErrBulkWritePartial) {
		t.Error("BulkWriteError should unwrap to ErrBulkWritePartial")
	}

	// KeyErrors
	keyErrors := bwe.KeyErrors()
	if len(keyErrors) != 2 {
		t.Errorf("Expected 2 key errors, got %d", len(keyErrors))
	}

	// FailedKeys
	failedKeys := bwe.FailedKeys()
	if len(failedKeys) != 2 {
		t.Errorf("Expected 2 failed keys, got %d", len(failedKeys))
	}

	// IsBulkWritePartial
	if !config.IsBulkWritePartial(bwe) {
		t.Error("IsBulkWritePartial should return true for BulkWriteError")
	}
	if config.IsBulkWritePartial(errors.New("other")) {
		t.Error("IsBulkWritePartial should return false for random error")
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

// TestValueInt64EdgeCases tests Int64 conversion for edge case types.
func TestValueInt64EdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		input   any
		want    int64
		wantErr bool
	}{
		{"int8", int8(42), 42, false},
		{"int16", int16(42), 42, false},
		{"int32", int32(42), 42, false},
		{"int64", int64(42), 42, false},
		{"float32 exact", float32(42.0), 42, false},
		{"float32 truncation", float32(3.14), 0, true},
		{"float64 non-integer", 3.14, 0, true},
		{"float64 Inf", math.Inf(1), 0, true},
		{"float64 NaN", math.NaN(), 0, true},
		{"string parseable", "123", 123, false},
		{"string not parseable", "abc", 0, true},
		{"json.Number int", json.Number("42"), 42, false},
		{"json.Number not int", json.Number("3.14"), 0, true},
		{"unsupported type", []int{1}, 0, true},
		{"nil", nil, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val := config.NewValue(tt.input)
			got, err := val.Int64()
			if (err != nil) != tt.wantErr {
				t.Errorf("Int64() error = %v, wantErr = %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("Int64() = %d, want %d", got, tt.want)
			}
		})
	}
}

// TestValueFloat64EdgeCases tests Float64 conversion for edge case types.
func TestValueFloat64EdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		input   any
		want    float64
		wantErr bool
	}{
		{"float32", float32(3.14), float64(float32(3.14)), false},
		{"int", 42, 42.0, false},
		{"int8", int8(10), 10.0, false},
		{"int16", int16(10), 10.0, false},
		{"int32", int32(10), 10.0, false},
		{"int64", int64(10), 10.0, false},
		{"string parseable", "3.14", 3.14, false},
		{"string not parseable", "abc", 0, true},
		{"json.Number", json.Number("3.14"), 3.14, false},
		{"json.Number invalid", json.Number("abc"), 0, true},
		{"unsupported type", []int{1}, 0, true},
		{"nil", nil, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val := config.NewValue(tt.input)
			got, err := val.Float64()
			if (err != nil) != tt.wantErr {
				t.Errorf("Float64() error = %v, wantErr = %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("Float64() = %f, want %f", got, tt.want)
			}
		})
	}
}

// TestValueBoolEdgeCases tests Bool conversion for edge case types.
func TestValueBoolEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		input   any
		want    bool
		wantErr bool
	}{
		{"true", true, true, false},
		{"false", false, false, false},
		{"int nonzero", 1, true, false},
		{"int zero", 0, false, false},
		{"int64 nonzero", int64(1), true, false},
		{"int64 zero", int64(0), false, false},
		{"float64 nonzero", 1.0, true, false},
		{"float64 zero", 0.0, false, false},
		{"string true", "true", true, false},
		{"string false", "false", false, false},
		{"string invalid", "maybe", false, true},
		{"unsupported type", []int{}, false, true},
		{"nil", nil, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val := config.NewValue(tt.input)
			got, err := val.Bool()
			if (err != nil) != tt.wantErr {
				t.Errorf("Bool() error = %v, wantErr = %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("Bool() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestValueStringEdgeCases tests String conversion for edge case types.
func TestValueStringEdgeCases(t *testing.T) {
	// String from bytes
	val := config.NewValue([]byte("hello"))
	s, err := val.String()
	if err != nil {
		t.Fatalf("String() from []byte failed: %v", err)
	}
	if s != "hello" {
		t.Errorf("String() = %q, want %q", s, "hello")
	}

	// String from nil
	val = config.NewValue(nil)
	_, err = val.String()
	if err == nil {
		t.Error("String() from nil should return error")
	}
}

// TestValueMarshalNil tests Marshal with nil raw value.
func TestValueMarshalNil(t *testing.T) {
	val := config.NewValue(nil)
	_, err := val.Marshal()
	if err == nil {
		t.Error("Marshal() with nil should return error")
	}
	if !errors.Is(err, config.ErrInvalidValue) {
		t.Errorf("Expected ErrInvalidValue, got: %v", err)
	}
}

// TestValueUnmarshalNil tests Unmarshal with nil raw value.
func TestValueUnmarshalNil(t *testing.T) {
	val := config.NewValue(nil)
	var target string
	err := val.Unmarshal(&target)
	if err == nil {
		t.Error("Unmarshal() with nil should return error")
	}
	if !errors.Is(err, config.ErrInvalidValue) {
		t.Errorf("Expected ErrInvalidValue, got: %v", err)
	}
}

// TestValueMarshalWithPreEncodedData tests Marshal when data is already encoded.
func TestValueMarshalWithPreEncodedData(t *testing.T) {
	val, err := config.NewValueFromBytes([]byte(`"hello"`), "json")
	if err != nil {
		t.Fatalf("NewValueFromBytes failed: %v", err)
	}

	data, err := val.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	if string(data) != `"hello"` {
		t.Errorf("Marshal = %q, want %q", string(data), `"hello"`)
	}
}

// TestValueUnmarshalWithCodecData tests Unmarshal using codec path.
func TestValueUnmarshalWithCodecData(t *testing.T) {
	val, err := config.NewValueFromBytes([]byte(`{"name":"test"}`), "json")
	if err != nil {
		t.Fatalf("NewValueFromBytes failed: %v", err)
	}

	var result map[string]any
	if err := val.Unmarshal(&result); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if result["name"] != "test" {
		t.Errorf("name = %v, want %q", result["name"], "test")
	}
}

// TestGetWriteModeNonWriteModer tests GetWriteMode with a Value that does not implement WriteModer.
func TestGetWriteModeNonWriteModer(t *testing.T) {
	custom := &customValue{raw: "test", meta: &customMetadata{}}
	mode := config.GetWriteMode(custom)
	if mode != config.WriteModeUpsert {
		t.Errorf("GetWriteMode for non-WriteModer = %v, want Upsert", mode)
	}
}

// TestKeyNotFoundErrorWithoutNamespace tests KeyNotFoundError message without namespace.
func TestKeyNotFoundErrorWithoutNamespace(t *testing.T) {
	err := &config.KeyNotFoundError{Key: "mykey"}
	expected := `config: key "mykey" not found`
	if err.Error() != expected {
		t.Errorf("Error() = %q, want %q", err.Error(), expected)
	}
}

// TestKeyNotFoundErrorWithNamespace tests KeyNotFoundError message with namespace.
func TestKeyNotFoundErrorWithNamespace(t *testing.T) {
	err := &config.KeyNotFoundError{Key: "mykey", Namespace: "ns"}
	expected := `config: key "mykey" not found in namespace "ns"`
	if err.Error() != expected {
		t.Errorf("Error() = %q, want %q", err.Error(), expected)
	}
}

// TestStoreErrorWithoutKey tests StoreError message without key.
func TestStoreErrorWithoutKey(t *testing.T) {
	err := &config.StoreError{Op: "connect", Backend: "postgres", Err: errors.New("refused")}
	msg := err.Error()
	if msg == "" {
		t.Error("StoreError.Error() should not be empty")
	}
	// Verify it doesn't include key= in the message
	expected := "config: connect [postgres]: refused"
	if msg != expected {
		t.Errorf("Error() = %q, want %q", msg, expected)
	}
}

// TestStoreErrorWithKey tests StoreError message with key.
func TestStoreErrorWithKey(t *testing.T) {
	err := &config.StoreError{Op: "get", Backend: "postgres", Key: "app/name", Err: errors.New("refused")}
	expected := `config: get [postgres] key="app/name": refused`
	if err.Error() != expected {
		t.Errorf("Error() = %q, want %q", err.Error(), expected)
	}
}

// TestManagerOptions tests option functions.
func TestManagerOptions(t *testing.T) {
	ctx := context.Background()

	// Test WithCodec, WithLogger, and backoff options
	logger := slog.Default()
	mgr, err := config.New(
		config.WithStore(memory.NewStore()),
		config.WithCodec(codec.Default()),
		config.WithLogger(logger),
		config.WithWatchInitialBackoff(200*time.Millisecond),
		config.WithWatchMaxBackoff(60*time.Second),
		config.WithWatchBackoffFactor(3.0),
	)
	if err != nil {
		t.Fatalf("New with options failed: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	// Test nil-safe option guards
	mgr2, err := config.New(
		config.WithStore(nil),             // should be ignored
		config.WithCodec(nil),             // should be ignored
		config.WithLogger(nil),            // should be ignored
		config.WithWatchInitialBackoff(0), // should be ignored (non-positive)
		config.WithWatchMaxBackoff(0),     // should be ignored
		config.WithWatchBackoffFactor(0),  // should be ignored
	)
	if err != nil {
		t.Fatalf("New with nil options failed: %v", err)
	}
	// Manager without store should fail to connect
	err = mgr2.Connect(ctx)
	if err != config.ErrStoreNotConnected {
		t.Errorf("Connect without store = %v, want ErrStoreNotConnected", err)
	}
}

// TestSetOptions tests set option functions.
func TestSetOptions(t *testing.T) {
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

	// Set with explicit type
	err = cfg.Set(ctx, "typed-value", 42, config.WithType(config.TypeInt))
	if err != nil {
		t.Fatalf("Set with type failed: %v", err)
	}

	// Set with codec
	err = cfg.Set(ctx, "coded-value", "hello", config.WithSetCodec(codec.Default()))
	if err != nil {
		t.Fatalf("Set with codec failed: %v", err)
	}

	// Set with nil codec (should be ignored, use default)
	err = cfg.Set(ctx, "nil-codec-value", "hello", config.WithSetCodec(nil))
	if err != nil {
		t.Fatalf("Set with nil codec failed: %v", err)
	}
}

// TestValueMetadataOptions tests value creation with metadata options.
func TestValueMetadataOptions(t *testing.T) {
	now := time.Now()
	val := config.NewValue("hello",
		config.WithValueMetadata(5, now, now.Add(time.Hour)),
		config.WithValueEntryID("entry-123"),
		config.WithValueStale(true),
	)

	meta := val.Metadata()
	if meta.Version() != 5 {
		t.Errorf("Version = %d, want 5", meta.Version())
	}
	if !meta.CreatedAt().Equal(now) {
		t.Error("CreatedAt mismatch")
	}
	if !meta.UpdatedAt().Equal(now.Add(time.Hour)) {
		t.Error("UpdatedAt mismatch")
	}
	if !meta.IsStale() {
		t.Error("Expected stale = true")
	}
}

// TestValueEntryIDWithoutPriorMetadata tests WithValueEntryID when metadata is nil.
func TestValueEntryIDWithoutPriorMetadata(t *testing.T) {
	val := config.NewValue("test",
		config.WithValueEntryID("id-1"),
	)
	// Should not panic; metadata is created internally
	if val.Metadata() == nil {
		t.Error("Metadata should not be nil")
	}
}

// TestValueStaleWithoutPriorMetadata tests WithValueStale when metadata is nil.
func TestValueStaleWithoutPriorMetadata(t *testing.T) {
	val := config.NewValue("test",
		config.WithValueStale(true),
	)
	if !val.Metadata().IsStale() {
		t.Error("Expected stale = true")
	}
}

// TestValueMetadataNil tests Metadata when no metadata was set.
func TestValueMetadataNil(t *testing.T) {
	val := config.NewValue("hello")
	meta := val.Metadata()
	if meta == nil {
		t.Fatal("Metadata should never be nil")
	}
	if meta.Version() != 0 {
		t.Errorf("Default version = %d, want 0", meta.Version())
	}
	if meta.IsStale() {
		t.Error("Default stale should be false")
	}
}

// TestNewValueFromBytesValid tests NewValueFromBytes with valid JSON.
func TestNewValueFromBytesValid(t *testing.T) {
	val, err := config.NewValueFromBytes([]byte(`42`), "json",
		config.WithValueType(config.TypeInt),
		config.WithValueMetadata(1, time.Now(), time.Now()),
	)
	if err != nil {
		t.Fatalf("NewValueFromBytes failed: %v", err)
	}
	if val.Type() != config.TypeInt {
		t.Errorf("Type = %v, want TypeInt", val.Type())
	}
}

// TestValueCodecName tests the Codec() method.
func TestValueCodecName(t *testing.T) {
	val := config.NewValue("hello")
	codecName := val.Codec()
	if codecName != "json" {
		t.Errorf("Codec() = %q, want %q", codecName, "json")
	}
}

// TestDetectTypeJsonNumber tests detectType with json.Number.
func TestDetectTypeJsonNumber(t *testing.T) {
	// json.Number that parses as int
	val := config.NewValue(json.Number("42"))
	if val.Type() != config.TypeInt {
		t.Errorf("json.Number('42').Type() = %v, want TypeInt", val.Type())
	}

	// json.Number that doesn't parse as int (float)
	val = config.NewValue(json.Number("3.14"))
	if val.Type() != config.TypeFloat {
		t.Errorf("json.Number('3.14').Type() = %v, want TypeFloat", val.Type())
	}
}

// TestDetectTypeUnsigned tests detectType with unsigned int types.
func TestDetectTypeUnsigned(t *testing.T) {
	tests := []struct {
		input any
		want  config.Type
	}{
		{uint8(42), config.TypeInt},
		{uint16(42), config.TypeInt},
		{uint32(42), config.TypeInt},
		{uint64(42), config.TypeInt},
	}

	for _, tt := range tests {
		val := config.NewValue(tt.input)
		if val.Type() != tt.want {
			t.Errorf("NewValue(%T).Type() = %v, want %v", tt.input, val.Type(), tt.want)
		}
	}
}

// TestChangeTypeStringUnknown tests ChangeType.String() for an unknown type.
func TestChangeTypeStringUnknown(t *testing.T) {
	ct := config.ChangeType(99)
	if ct.String() != "unknown" {
		t.Errorf("ChangeType(99).String() = %q, want %q", ct.String(), "unknown")
	}
}

// TestManagerCloseTwice tests that closing a manager twice is safe.
func TestManagerCloseTwice(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	// First close
	err = mgr.Close(ctx)
	if err != nil {
		t.Fatalf("First Close failed: %v", err)
	}

	// Second close should be safe (no-op)
	err = mgr.Close(ctx)
	if err != nil {
		t.Fatalf("Second Close should not error, got: %v", err)
	}
}

// TestManagerCloseBeforeConnect tests closing a manager that was never connected.
func TestManagerCloseBeforeConnect(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	// Close without connecting should be safe (no-op)
	err = mgr.Close(ctx)
	if err != nil {
		t.Fatalf("Close before connect should not error, got: %v", err)
	}
}

// watchFailStore is a store that fails Watch on demand, to test watchChanges error paths.
type watchFailStore struct {
	config.Store
	watchFails atomic.Int32
	watchCalls atomic.Int32
}

func (s *watchFailStore) Watch(ctx context.Context, filter config.WatchFilter) (<-chan config.ChangeEvent, error) {
	call := s.watchCalls.Add(1)
	remaining := s.watchFails.Load()
	if remaining > 0 && call <= remaining {
		return nil, errors.New("simulated watch failure")
	}
	return s.Store.Watch(ctx, filter)
}

// TestWatchReconnectsAfterFailure tests that watchChanges retries on failure.
func TestWatchReconnectsAfterFailure(t *testing.T) {
	ctx := context.Background()

	underlying := memory.NewStore()
	store := &watchFailStore{Store: underlying}
	store.watchFails.Store(2) // Fail first 2 watch attempts

	mgr, err := config.New(
		config.WithStore(store),
		config.WithWatchInitialBackoff(10*time.Millisecond),
		config.WithWatchMaxBackoff(50*time.Millisecond),
		config.WithWatchBackoffFactor(2.0),
	)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	// Wait for watch to recover (2 failures * backoff + some margin)
	time.Sleep(200 * time.Millisecond)

	// After recovery, watch should work - verify via observer
	obs, ok := mgr.(config.ManagerObserver)
	if !ok {
		t.Fatal("Manager should implement ManagerObserver")
	}

	ws := obs.WatchStatus()
	if ws.ConsecutiveFailures != 0 {
		t.Errorf("ConsecutiveFailures = %d after recovery, want 0", ws.ConsecutiveFailures)
	}

	_ = mgr.Close(ctx)
}

// watchNotSupportedStore is a store that returns ErrWatchNotSupported.
type watchNotSupportedStore struct {
	config.Store
}

func (s *watchNotSupportedStore) Watch(ctx context.Context, filter config.WatchFilter) (<-chan config.ChangeEvent, error) {
	return nil, config.ErrWatchNotSupported
}

// TestWatchNotSupported tests that watch exits gracefully when not supported.
func TestWatchNotSupported(t *testing.T) {
	ctx := context.Background()

	store := &watchNotSupportedStore{Store: memory.NewStore()}
	mgr, err := config.New(config.WithStore(store))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	// Give watch goroutine time to exit
	time.Sleep(50 * time.Millisecond)

	// Close should work cleanly
	_ = mgr.Close(ctx)
}

// healthCheckStore is a store that implements HealthChecker.
type healthCheckStore struct {
	config.Store
	healthErr error
}

func (s *healthCheckStore) Health(ctx context.Context) error {
	return s.healthErr
}

// TestHealthWithHealthChecker tests Health when store implements HealthChecker.
func TestHealthWithHealthChecker(t *testing.T) {
	ctx := context.Background()

	hcStore := &healthCheckStore{
		Store:     memory.NewStore(),
		healthErr: nil,
	}

	mgr, err := config.New(config.WithStore(hcStore))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	// Healthy
	if err := mgr.Health(ctx); err != nil {
		t.Errorf("Health should be nil, got: %v", err)
	}

	// Unhealthy store
	hcStore.healthErr = errors.New("store unhealthy")
	if err := mgr.Health(ctx); err == nil {
		t.Error("Health should return error when store is unhealthy")
	}
}

// TestHealthWithWatchFailures tests Health when watch has consecutive failures.
func TestHealthWithWatchFailures(t *testing.T) {
	ctx := context.Background()

	underlying := memory.NewStore()
	store := &watchFailStore{Store: underlying}
	// Make watch fail many times
	store.watchFails.Store(100)

	mgr, err := config.New(
		config.WithStore(store),
		config.WithWatchInitialBackoff(5*time.Millisecond),
		config.WithWatchMaxBackoff(10*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	// Wait for at least 3 failures to accumulate
	time.Sleep(200 * time.Millisecond)

	err = mgr.Health(ctx)
	if err == nil {
		t.Error("Health should fail with watch failures")
	}
	if !config.IsWatchUnhealthy(err) {
		t.Errorf("Expected watch unhealthy error, got: %v", err)
	}

	// Check WatchStatus shows failures
	obs := mgr.(config.ManagerObserver)
	ws := obs.WatchStatus()
	if ws.ConsecutiveFailures < 3 {
		t.Errorf("ConsecutiveFailures = %d, want >= 3", ws.ConsecutiveFailures)
	}
	if ws.LastError == "" {
		t.Error("LastError should not be empty after failures")
	}
	if ws.LastAttempt.IsZero() {
		t.Error("LastAttempt should not be zero")
	}
}

// TestWatchStatusAfterClose tests WatchStatus after manager is closed.
func TestWatchStatusAfterClose(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	obs := mgr.(config.ManagerObserver)

	_ = mgr.Close(ctx)

	ws := obs.WatchStatus()
	if ws.Connected {
		t.Error("Expected Connected = false after close")
	}
}

// TestNewPage tests the NewPage constructor.
func TestNewPage(t *testing.T) {
	results := map[string]config.Value{
		"key1": config.NewValue("val1"),
		"key2": config.NewValue("val2"),
	}
	p := config.NewPage(results, "cursor123", 10)
	if len(p.Results()) != 2 {
		t.Errorf("Results = %d, want 2", len(p.Results()))
	}
	if p.NextCursor() != "cursor123" {
		t.Errorf("NextCursor = %q, want %q", p.NextCursor(), "cursor123")
	}
	if p.Limit() != 10 {
		t.Errorf("Limit = %d, want 10", p.Limit())
	}
}

// TestParseTypeUnknown tests ParseType with an unrecognized string.
func TestParseTypeUnknown(t *testing.T) {
	got := config.ParseType("foobar")
	if got != config.TypeUnknown {
		t.Errorf("ParseType('foobar') = %v, want TypeUnknown", got)
	}
}

// TestWriteModeStringValues tests all WriteMode.String() values.
func TestWriteModeStringValues(t *testing.T) {
	if config.WriteModeUpsert.String() != "upsert" {
		t.Errorf("WriteModeUpsert.String() = %q", config.WriteModeUpsert.String())
	}
	if config.WriteModeCreate.String() != "create" {
		t.Errorf("WriteModeCreate.String() = %q", config.WriteModeCreate.String())
	}
	if config.WriteModeUpdate.String() != "update" {
		t.Errorf("WriteModeUpdate.String() = %q", config.WriteModeUpdate.String())
	}
}

// TestDefaultNamespace tests DefaultNamespace constant.
func TestDefaultNamespace(t *testing.T) {
	if config.DefaultNamespace != "" {
		t.Errorf("DefaultNamespace = %q, want empty string", config.DefaultNamespace)
	}
}

// TestValueWriteMode tests Value's WriteMode when set via option.
func TestValueWriteMode(t *testing.T) {
	val := config.NewValue("test", config.WithValueWriteMode(config.WriteModeCreate))
	mode := config.GetWriteMode(val)
	if mode != config.WriteModeCreate {
		t.Errorf("WriteMode = %v, want Create", mode)
	}
}

// TestIsWatchUnhealthyFalse tests IsWatchUnhealthy with non-matching error.
func TestIsWatchUnhealthyFalse(t *testing.T) {
	if config.IsWatchUnhealthy(errors.New("other")) {
		t.Error("IsWatchUnhealthy should return false for random error")
	}
}

// TestConcurrentNamespaceCreation tests concurrent namespace creation.
func TestConcurrentNamespaceCreation(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	var wg sync.WaitGroup
	results := make([]config.Config, 20)

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = mgr.Namespace("concurrent")
		}(i)
	}
	wg.Wait()

	// All should return the same instance
	for i := 1; i < 20; i++ {
		if results[i] != results[0] {
			t.Error("All concurrent Namespace calls should return the same instance")
			break
		}
	}
}

// TestFindWithKeys tests Find using specific keys mode.
func TestFindWithKeys(t *testing.T) {
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
	_ = cfg.Set(ctx, "key1", "val1")
	_ = cfg.Set(ctx, "key2", "val2")
	_ = cfg.Set(ctx, "key3", "val3")

	// Find specific keys
	page, err := cfg.Find(ctx, config.NewFilter().WithKeys("key1", "key3").Build())
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}
	if len(page.Results()) != 2 {
		t.Errorf("Expected 2 results, got %d", len(page.Results()))
	}
}

// TestRefreshNotFoundClearsCache tests that refreshing a deleted key clears cache.
func TestRefreshNotFoundClearsCache(t *testing.T) {
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

	// Set and get (populates cache)
	_ = cfg.Set(ctx, "temp-key", "temp-val")
	_, _ = cfg.Get(ctx, "temp-key")

	// Delete directly from store, then refresh
	_ = cfg.Delete(ctx, "temp-key")

	// Refresh should return NotFound (key is deleted)
	err = mgr.Refresh(ctx, "test", "temp-key")
	if !config.IsNotFound(err) {
		t.Errorf("Refresh after delete should return NotFound, got: %v", err)
	}
}

// TestRefreshBeforeConnect tests that Refresh fails when not connected.
func TestRefreshBeforeConnect(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	err = mgr.Refresh(ctx, "test", "key")
	if err != config.ErrManagerClosed {
		t.Errorf("Refresh before connect = %v, want ErrManagerClosed", err)
	}
}

// TestDetectTypeFloat64Special tests detectType with special float64 values.
func TestDetectTypeFloat64Special(t *testing.T) {
	// Inf should be detected as float
	val := config.NewValue(math.Inf(1))
	if val.Type() != config.TypeFloat {
		t.Errorf("Inf type = %v, want TypeFloat", val.Type())
	}

	// NaN should be detected as float
	val = config.NewValue(math.NaN())
	if val.Type() != config.TypeFloat {
		t.Errorf("NaN type = %v, want TypeFloat", val.Type())
	}
}
