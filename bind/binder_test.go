package bind

import (
	"context"
	"testing"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/memory"
)

func setupTestConfig(t *testing.T) config.Config {
	ctx := context.Background()
	store := memory.NewStore()
	mgr := config.New(config.WithStore(store))
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect error: %v", err)
	}
	t.Cleanup(func() { mgr.Close(ctx) })
	return mgr.Namespace("test")
}

func TestNew(t *testing.T) {
	cfg := setupTestConfig(t)
	b := New(cfg)

	if b == nil {
		t.Fatal("expected binder to not be nil")
	}
	if b.cfg != cfg {
		t.Error("expected cfg to be set")
	}
	if b.fieldTag != "json" {
		t.Errorf("expected default fieldTag 'json', got %q", b.fieldTag)
	}
}

func TestNewWithOptions(t *testing.T) {
	cfg := setupTestConfig(t)
	b := New(cfg,
		WithFieldTag("yaml"),
	)

	if b.fieldTag != "yaml" {
		t.Errorf("expected fieldTag 'yaml', got %q", b.fieldTag)
	}
}

func TestBind(t *testing.T) {
	cfg := setupTestConfig(t)
	b := New(cfg)
	bound := b.Bind()

	if bound == nil {
		t.Fatal("expected bound config to not be nil")
	}

	// Should implement config.Config
	if bound.Namespace() != cfg.Namespace() {
		t.Error("expected bound config to have same namespace")
	}
}

func TestConfig(t *testing.T) {
	cfg := setupTestConfig(t)
	b := New(cfg)

	if b.Config() != cfg {
		t.Error("expected Config() to return original config")
	}
}

func TestGetStruct(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	// Set up test data
	type DBConfig struct {
		Host string `json:"host"`
		Port int    `json:"port"`
	}
	testValue := DBConfig{Host: "localhost", Port: 5432}
	if err := cfg.Set(ctx, "database", testValue); err != nil {
		t.Fatalf("Set error: %v", err)
	}

	b := New(cfg)
	bound := b.Bind()

	var result DBConfig
	if err := bound.GetStruct(ctx, "database", &result); err != nil {
		t.Fatalf("GetStruct error: %v", err)
	}

	if result.Host != testValue.Host {
		t.Errorf("expected host %q, got %q", testValue.Host, result.Host)
	}
	if result.Port != testValue.Port {
		t.Errorf("expected port %d, got %d", testValue.Port, result.Port)
	}
}

func TestGetStructNotFound(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)
	b := New(cfg)
	bound := b.Bind()

	var result struct{}
	err := bound.GetStruct(ctx, "nonexistent", &result)
	if err == nil {
		t.Error("expected error for nonexistent key")
	}
	if !config.IsNotFound(err) {
		t.Errorf("expected NotFound error, got %v", err)
	}
}

func TestSetStruct(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)
	b := New(cfg)
	bound := b.Bind()

	type AppConfig struct {
		Name    string `json:"name"`
		Version string `json:"version"`
	}
	testValue := AppConfig{Name: "myapp", Version: "1.0.0"}

	if err := bound.SetStruct(ctx, "app", testValue); err != nil {
		t.Fatalf("SetStruct error: %v", err)
	}

	// Verify it was stored correctly
	var result AppConfig
	if err := bound.GetStruct(ctx, "app", &result); err != nil {
		t.Fatalf("GetStruct error: %v", err)
	}

	if result.Name != testValue.Name {
		t.Errorf("expected name %q, got %q", testValue.Name, result.Name)
	}
	if result.Version != testValue.Version {
		t.Errorf("expected version %q, got %q", testValue.Version, result.Version)
	}
}

func TestMustGetStruct(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	// Set up test data
	if err := cfg.Set(ctx, "key", "value"); err != nil {
		t.Fatalf("Set error: %v", err)
	}

	b := New(cfg)
	bound := b.Bind()

	var result string
	// Should not panic
	bound.MustGetStruct(ctx, "key", &result)

	if result != "value" {
		t.Errorf("expected 'value', got %q", result)
	}
}

func TestMustGetStructPanics(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)
	b := New(cfg)
	bound := b.Bind()

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for nonexistent key")
		}
	}()

	var result struct{}
	bound.MustGetStruct(ctx, "nonexistent", &result)
}

func TestBindPrefix(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	// Set up hierarchical config
	if err := cfg.Set(ctx, "database/host", "localhost"); err != nil {
		t.Fatalf("Set error: %v", err)
	}
	if err := cfg.Set(ctx, "database/port", 5432); err != nil {
		t.Fatalf("Set error: %v", err)
	}
	if err := cfg.Set(ctx, "database/name", "testdb"); err != nil {
		t.Fatalf("Set error: %v", err)
	}

	b := New(cfg)
	bound := b.Bind()

	type DBConfig struct {
		Host string `json:"host"`
		Port int    `json:"port"`
		Name string `json:"name"`
	}

	var result DBConfig
	if err := bound.BindPrefix(ctx, "database", &result); err != nil {
		t.Fatalf("BindPrefix error: %v", err)
	}

	if result.Host != "localhost" {
		t.Errorf("expected host 'localhost', got %q", result.Host)
	}
	if result.Port != 5432 {
		t.Errorf("expected port 5432, got %d", result.Port)
	}
	if result.Name != "testdb" {
		t.Errorf("expected name 'testdb', got %q", result.Name)
	}
}

func TestBindPrefixNested(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	// Set up nested hierarchical config
	if err := cfg.Set(ctx, "app/database/host", "localhost"); err != nil {
		t.Fatalf("Set error: %v", err)
	}
	if err := cfg.Set(ctx, "app/database/port", 5432); err != nil {
		t.Fatalf("Set error: %v", err)
	}
	if err := cfg.Set(ctx, "app/cache/enabled", true); err != nil {
		t.Fatalf("Set error: %v", err)
	}

	b := New(cfg)
	bound := b.Bind()

	type AppConfig struct {
		Database struct {
			Host string `json:"host"`
			Port int    `json:"port"`
		} `json:"database"`
		Cache struct {
			Enabled bool `json:"enabled"`
		} `json:"cache"`
	}

	var result AppConfig
	if err := bound.BindPrefix(ctx, "app", &result); err != nil {
		t.Fatalf("BindPrefix error: %v", err)
	}

	if result.Database.Host != "localhost" {
		t.Errorf("expected database.host 'localhost', got %q", result.Database.Host)
	}
	if result.Database.Port != 5432 {
		t.Errorf("expected database.port 5432, got %d", result.Database.Port)
	}
	if !result.Cache.Enabled {
		t.Error("expected cache.enabled to be true")
	}
}

func TestBindPrefixEmpty(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)
	b := New(cfg)
	bound := b.Bind()

	type EmptyConfig struct {
		Value string `json:"value"`
	}

	var result EmptyConfig
	// Should not error for empty prefix results
	if err := bound.BindPrefix(ctx, "nonexistent", &result); err != nil {
		t.Fatalf("BindPrefix error for nonexistent prefix: %v", err)
	}

	// Result should be zero value
	if result.Value != "" {
		t.Errorf("expected empty value, got %q", result.Value)
	}
}

func TestValidate(t *testing.T) {
	cfg := setupTestConfig(t)
	b := New(cfg)
	bound := b.Bind()

	// Without validators, Validate should always succeed
	if err := bound.Validate("any value"); err != nil {
		t.Errorf("Validate error without validators: %v", err)
	}
}

func TestWithValidator(t *testing.T) {
	cfg := setupTestConfig(t)

	// Create a custom validator
	customValidator := ValidatorFunc(func(v any) error {
		if s, ok := v.(string); ok && s == "" {
			return &ValidationError{Err: ErrValidationFailed}
		}
		return nil
	})

	b := New(cfg, WithValidator(customValidator))
	bound := b.Bind()

	// Should fail validation for empty string
	if err := bound.Validate(""); err == nil {
		t.Error("expected validation error for empty string")
	}

	// Should pass validation for non-empty string
	if err := bound.Validate("hello"); err != nil {
		t.Errorf("unexpected validation error: %v", err)
	}
}

func TestWithValidatorOnSet(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	type Config struct {
		Name string `json:"name"`
	}

	validateCalled := false
	customValidator := ValidatorFunc(func(v any) error {
		validateCalled = true
		return nil
	})

	b := New(cfg, WithValidator(customValidator))
	bound := b.Bind()

	testValue := Config{Name: "test"}
	if err := bound.SetStruct(ctx, "config", testValue); err != nil {
		t.Fatalf("SetStruct error: %v", err)
	}

	if !validateCalled {
		t.Error("expected validator to be called on SetStruct")
	}
}

func TestHooks(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	// Set up test data
	if err := cfg.Set(ctx, "key", "value"); err != nil {
		t.Fatalf("Set error: %v", err)
	}

	beforeGetCalled := false
	afterUnmarshalCalled := false
	beforeSetCalled := false

	hooks := Hooks{
		BeforeGet: func(ctx context.Context, key string) error {
			beforeGetCalled = true
			return nil
		},
		AfterUnmarshal: func(ctx context.Context, key string, target any) error {
			afterUnmarshalCalled = true
			return nil
		},
		BeforeSet: func(ctx context.Context, key string, value any) error {
			beforeSetCalled = true
			return nil
		},
	}

	b := New(cfg, WithHooks(hooks))
	bound := b.Bind()

	// Test GetStruct hooks
	var result string
	if err := bound.GetStruct(ctx, "key", &result); err != nil {
		t.Fatalf("GetStruct error: %v", err)
	}

	if !beforeGetCalled {
		t.Error("expected BeforeGet hook to be called")
	}
	if !afterUnmarshalCalled {
		t.Error("expected AfterUnmarshal hook to be called")
	}

	// Test SetStruct hooks
	if err := bound.SetStruct(ctx, "newkey", "newvalue"); err != nil {
		t.Fatalf("SetStruct error: %v", err)
	}

	if !beforeSetCalled {
		t.Error("expected BeforeSet hook to be called")
	}
}

func TestBuildMapFromEntries(t *testing.T) {
	entries := map[string]config.Value{
		"database/host": config.NewValue("localhost"),
		"database/port": config.NewValue(5432),
		"app/name":      config.NewValue("myapp"),
	}

	result := buildMapFromEntries(entries, "")

	// Check database entry
	db, ok := result["database"].(map[string]any)
	if !ok {
		t.Fatal("expected database to be a map")
	}
	if db["host"] != "localhost" {
		t.Errorf("expected database.host 'localhost', got %v", db["host"])
	}

	// Check app entry
	app, ok := result["app"].(map[string]any)
	if !ok {
		t.Fatal("expected app to be a map")
	}
	if app["name"] != "myapp" {
		t.Errorf("expected app.name 'myapp', got %v", app["name"])
	}
}

func TestBuildMapFromEntriesWithPrefix(t *testing.T) {
	entries := map[string]config.Value{
		"prefix/host": config.NewValue("localhost"),
		"prefix/port": config.NewValue(5432),
	}

	result := buildMapFromEntries(entries, "prefix")

	if result["host"] != "localhost" {
		t.Errorf("expected host 'localhost', got %v", result["host"])
	}
	if result["port"] != float64(5432) { // JSON numbers are float64
		t.Errorf("expected port 5432, got %v", result["port"])
	}
}

func TestFieldMapperStructToMap(t *testing.T) {
	type Inner struct {
		Value string `json:"value"`
	}
	type TestStruct struct {
		Name    string  `json:"name"`
		Count   int     `json:"count"`
		Enabled bool    `json:"enabled"`
		Ignored string  `json:"-"`
		Inner   Inner   `json:"inner"`
		Tags    []string `json:"tags"`
	}

	s := TestStruct{
		Name:    "test",
		Count:   42,
		Enabled: true,
		Ignored: "should not appear",
		Inner:   Inner{Value: "nested"},
		Tags:    []string{"a", "b"},
	}

	mapper := NewFieldMapper("json")
	result, err := mapper.StructToMap(s)
	if err != nil {
		t.Fatalf("StructToMap error: %v", err)
	}

	if result["name"] != "test" {
		t.Errorf("expected name 'test', got %v", result["name"])
	}
	if result["count"] != 42 {
		t.Errorf("expected count 42, got %v", result["count"])
	}
	if result["enabled"] != true {
		t.Errorf("expected enabled true, got %v", result["enabled"])
	}
	if _, exists := result["Ignored"]; exists {
		t.Error("Ignored field should not be in result")
	}
	if inner, ok := result["inner"].(map[string]any); !ok || inner["value"] != "nested" {
		t.Errorf("expected inner.value 'nested', got %v", result["inner"])
	}
}

func TestFieldMapperStructToMapPointer(t *testing.T) {
	type TestStruct struct {
		Name string `json:"name"`
	}

	s := &TestStruct{Name: "test"}
	mapper := NewFieldMapper("json")
	result, err := mapper.StructToMap(s)
	if err != nil {
		t.Fatalf("StructToMap error: %v", err)
	}

	if result["name"] != "test" {
		t.Errorf("expected name 'test', got %v", result["name"])
	}
}

func TestFieldMapperStructToMapNonStruct(t *testing.T) {
	mapper := NewFieldMapper("json")
	_, err := mapper.StructToMap("not a struct")
	if err == nil {
		t.Error("expected error for non-struct input")
	}
}

func TestFieldMapperMapToStruct(t *testing.T) {
	type Inner struct {
		Value string `json:"value"`
	}
	type TestStruct struct {
		Name    string `json:"name"`
		Count   int    `json:"count"`
		Enabled bool   `json:"enabled"`
		Inner   Inner  `json:"inner"`
	}

	data := map[string]any{
		"name":    "test",
		"count":   float64(42), // JSON numbers are float64
		"enabled": true,
		"inner": map[string]any{
			"value": "nested",
		},
	}

	mapper := NewFieldMapper("json")
	var result TestStruct
	if err := mapper.MapToStruct(data, &result); err != nil {
		t.Fatalf("MapToStruct error: %v", err)
	}

	if result.Name != "test" {
		t.Errorf("expected name 'test', got %q", result.Name)
	}
	if result.Count != 42 {
		t.Errorf("expected count 42, got %d", result.Count)
	}
	if !result.Enabled {
		t.Error("expected enabled true")
	}
	if result.Inner.Value != "nested" {
		t.Errorf("expected inner.value 'nested', got %q", result.Inner.Value)
	}
}

func TestFieldMapperMapToStructNonPointer(t *testing.T) {
	type TestStruct struct{}
	mapper := NewFieldMapper("json")

	var result TestStruct
	err := mapper.MapToStruct(map[string]any{}, result) // Not a pointer
	if err == nil {
		t.Error("expected error for non-pointer target")
	}
}

func TestFieldMapperMapToStructNilPointer(t *testing.T) {
	mapper := NewFieldMapper("json")
	var result *struct{ Name string }
	err := mapper.MapToStruct(map[string]any{}, result) // Nil pointer
	if err == nil {
		t.Error("expected error for nil pointer target")
	}
}

func TestFieldMapperCustomTag(t *testing.T) {
	type TestStruct struct {
		Name string `yaml:"name"`
	}

	mapper := NewFieldMapper("yaml")
	result, err := mapper.StructToMap(TestStruct{Name: "test"})
	if err != nil {
		t.Fatalf("StructToMap error: %v", err)
	}

	if result["name"] != "test" {
		t.Errorf("expected name 'test', got %v", result["name"])
	}
}

func TestFieldMapperEmptyTag(t *testing.T) {
	mapper := NewFieldMapper("")
	if mapper.tagName != "json" {
		t.Errorf("expected default tagName 'json', got %q", mapper.tagName)
	}
}

func TestFieldMapperOmitempty(t *testing.T) {
	type TestStruct struct {
		Name  string `json:"name,omitempty"`
		Value int    `json:"value,omitempty"`
	}

	mapper := NewFieldMapper("json")
	result, err := mapper.StructToMap(TestStruct{}) // All zero values
	if err != nil {
		t.Fatalf("StructToMap error: %v", err)
	}

	if _, exists := result["name"]; exists {
		t.Error("empty name should be omitted")
	}
	if _, exists := result["value"]; exists {
		t.Error("zero value should be omitted")
	}
}

func TestValidationError(t *testing.T) {
	// Test with key and field
	err := &ValidationError{
		Key:    "config/key",
		Field:  "Name",
		Reason: "cannot be empty",
	}
	expected := `validation failed for "config/key" field "Name": cannot be empty`
	if err.Error() != expected {
		t.Errorf("expected %q, got %q", expected, err.Error())
	}

	// Test with just key
	err = &ValidationError{
		Key:    "config/key",
		Reason: "invalid",
	}
	expected = `validation failed for "config/key": invalid`
	if err.Error() != expected {
		t.Errorf("expected %q, got %q", expected, err.Error())
	}

	// Test with underlying error
	err = &ValidationError{
		Err: ErrValidationFailed,
	}
	if err.Error() != "validation failed: config: validation failed" {
		t.Errorf("unexpected error message: %q", err.Error())
	}

	// Test Unwrap
	if err.Unwrap() != ErrValidationFailed {
		t.Error("expected Unwrap to return ErrValidationFailed")
	}

	// Test Unwrap with nil Err
	err = &ValidationError{Reason: "test"}
	if err.Unwrap() != ErrValidationFailed {
		t.Error("expected Unwrap to return ErrValidationFailed when Err is nil")
	}
}

func TestBindError(t *testing.T) {
	underlyingErr := ErrBindingFailed
	err := &BindError{
		Key: "config/key",
		Op:  "unmarshal",
		Err: underlyingErr,
	}

	expected := `binding failed for "config/key" during unmarshal: config: binding failed`
	if err.Error() != expected {
		t.Errorf("expected %q, got %q", expected, err.Error())
	}

	if err.Unwrap() != underlyingErr {
		t.Error("expected Unwrap to return underlying error")
	}

	// Test Unwrap with nil Err
	err = &BindError{Key: "key", Op: "op"}
	if err.Unwrap() != ErrBindingFailed {
		t.Error("expected Unwrap to return ErrBindingFailed when Err is nil")
	}
}

func TestIsValidationError(t *testing.T) {
	validationErr := &ValidationError{Key: "test"}
	if !IsValidationError(validationErr) {
		t.Error("expected IsValidationError to return true")
	}

	otherErr := ErrBindingFailed
	if IsValidationError(otherErr) {
		t.Error("expected IsValidationError to return false for non-ValidationError")
	}
}

func TestIsBindError(t *testing.T) {
	bindErr := &BindError{Key: "test"}
	if !IsBindError(bindErr) {
		t.Error("expected IsBindError to return true")
	}

	otherErr := ErrValidationFailed
	if IsBindError(otherErr) {
		t.Error("expected IsBindError to return false for non-BindError")
	}
}

func TestChainValidators(t *testing.T) {
	validator1Called := false
	validator2Called := false

	v1 := ValidatorFunc(func(v any) error {
		validator1Called = true
		return nil
	})
	v2 := ValidatorFunc(func(v any) error {
		validator2Called = true
		return nil
	})

	chain := ChainValidators(v1, v2)
	if err := chain.Validate("test"); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if !validator1Called || !validator2Called {
		t.Error("expected both validators to be called")
	}
}

func TestChainValidatorsStopsOnError(t *testing.T) {
	validator2Called := false

	v1 := ValidatorFunc(func(v any) error {
		return ErrValidationFailed
	})
	v2 := ValidatorFunc(func(v any) error {
		validator2Called = true
		return nil
	})

	chain := ChainValidators(v1, v2)
	err := chain.Validate("test")
	if err != ErrValidationFailed {
		t.Errorf("expected ErrValidationFailed, got %v", err)
	}

	if validator2Called {
		t.Error("second validator should not be called after first fails")
	}
}

func TestHooksBeforeGetError(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	// Set up test data
	if err := cfg.Set(ctx, "key", "value"); err != nil {
		t.Fatalf("Set error: %v", err)
	}

	expectedErr := ErrValidationFailed
	hooks := Hooks{
		BeforeGet: func(ctx context.Context, key string) error {
			return expectedErr
		},
	}

	b := New(cfg, WithHooks(hooks))
	bound := b.Bind()

	var result string
	err := bound.GetStruct(ctx, "key", &result)
	if err != expectedErr {
		t.Errorf("expected %v, got %v", expectedErr, err)
	}
}

func TestHooksAfterUnmarshalError(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	// Set up test data
	if err := cfg.Set(ctx, "key", "value"); err != nil {
		t.Fatalf("Set error: %v", err)
	}

	expectedErr := ErrValidationFailed
	hooks := Hooks{
		AfterUnmarshal: func(ctx context.Context, key string, target any) error {
			return expectedErr
		},
	}

	b := New(cfg, WithHooks(hooks))
	bound := b.Bind()

	var result string
	err := bound.GetStruct(ctx, "key", &result)
	if err != expectedErr {
		t.Errorf("expected %v, got %v", expectedErr, err)
	}
}

func TestHooksBeforeSetError(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	expectedErr := ErrValidationFailed
	hooks := Hooks{
		BeforeSet: func(ctx context.Context, key string, value any) error {
			return expectedErr
		},
	}

	b := New(cfg, WithHooks(hooks))
	bound := b.Bind()

	err := bound.SetStruct(ctx, "key", "value")
	if err != expectedErr {
		t.Errorf("expected %v, got %v", expectedErr, err)
	}
}

func TestSetStructValidationError(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	validator := ValidatorFunc(func(v any) error {
		return ErrValidationFailed
	})

	b := New(cfg, WithValidator(validator))
	bound := b.Bind()

	err := bound.SetStruct(ctx, "key", "value")
	if !IsValidationError(err) {
		t.Errorf("expected ValidationError, got %v", err)
	}
}

func TestBindPrefixValidationError(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	// Set up test data
	if err := cfg.Set(ctx, "prefix/name", "value"); err != nil {
		t.Fatalf("Set error: %v", err)
	}

	validator := ValidatorFunc(func(v any) error {
		return ErrValidationFailed
	})

	type Config struct {
		Name string `json:"name"`
	}

	b := New(cfg, WithValidator(validator))
	bound := b.Bind()

	var result Config
	err := bound.BindPrefix(ctx, "prefix", &result)
	if !IsValidationError(err) {
		t.Errorf("expected ValidationError, got %v", err)
	}
}

func TestGetStructValidationError(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	// Set up test data
	if err := cfg.Set(ctx, "key", "value"); err != nil {
		t.Fatalf("Set error: %v", err)
	}

	validator := ValidatorFunc(func(v any) error {
		return ErrValidationFailed
	})

	b := New(cfg, WithValidator(validator))
	bound := b.Bind()

	var result string
	err := bound.GetStruct(ctx, "key", &result)
	if !IsValidationError(err) {
		t.Errorf("expected ValidationError, got %v", err)
	}
}

func TestFieldMapperSliceOfStructs(t *testing.T) {
	type Item struct {
		Name string `json:"name"`
	}
	type TestStruct struct {
		Items []Item `json:"items"`
	}

	s := TestStruct{
		Items: []Item{{Name: "first"}, {Name: "second"}},
	}

	mapper := NewFieldMapper("json")
	result, err := mapper.StructToMap(s)
	if err != nil {
		t.Fatalf("StructToMap error: %v", err)
	}

	items, ok := result["items"].([]any)
	if !ok {
		t.Fatal("expected items to be slice")
	}
	if len(items) != 2 {
		t.Errorf("expected 2 items, got %d", len(items))
	}
}

func TestFieldMapperPointerField(t *testing.T) {
	type Inner struct {
		Value string `json:"value"`
	}
	type TestStruct struct {
		Inner *Inner `json:"inner,omitempty"`
	}

	// Test with nil pointer
	mapper := NewFieldMapper("json")
	result, err := mapper.StructToMap(TestStruct{})
	if err != nil {
		t.Fatalf("StructToMap error: %v", err)
	}
	if _, exists := result["inner"]; exists {
		t.Error("nil pointer should be omitted with omitempty")
	}

	// Test with non-nil pointer
	inner := &Inner{Value: "test"}
	result, err = mapper.StructToMap(TestStruct{Inner: inner})
	if err != nil {
		t.Fatalf("StructToMap error: %v", err)
	}
	innerMap, ok := result["inner"].(map[string]any)
	if !ok {
		t.Fatal("expected inner to be a map")
	}
	if innerMap["value"] != "test" {
		t.Errorf("expected inner.value 'test', got %v", innerMap["value"])
	}
}

func TestFieldMapperMapField(t *testing.T) {
	type TestStruct struct {
		Data map[string]int `json:"data"`
	}

	// Test with nil map
	mapper := NewFieldMapper("json")
	result, err := mapper.StructToMap(TestStruct{})
	if err != nil {
		t.Fatalf("StructToMap error: %v", err)
	}
	if result["data"] != nil {
		t.Error("nil map should be nil in result")
	}

	// Test with non-nil map
	result, err = mapper.StructToMap(TestStruct{Data: map[string]int{"a": 1}})
	if err != nil {
		t.Fatalf("StructToMap error: %v", err)
	}
	data, ok := result["data"].(map[string]int)
	if !ok {
		t.Fatal("expected data to be a map")
	}
	if data["a"] != 1 {
		t.Errorf("expected data[a] = 1, got %v", data["a"])
	}
}
