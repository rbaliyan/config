package config_test

import (
	"context"
	"testing"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/memory"
)

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
		{"with dot", "my.namespace", false},
		{"with colon", "internal:config:system", false},
		{"with dot and colon", "org.example:env.prod", false},
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
	for i := range 10 {
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

// TestDefaultNamespace tests DefaultNamespace constant.
func TestDefaultNamespace(t *testing.T) {
	if config.DefaultNamespace != "" {
		t.Errorf("DefaultNamespace = %q, want empty string", config.DefaultNamespace)
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
