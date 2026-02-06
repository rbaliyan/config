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
	mgr, err := config.New(config.WithStore(store))
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
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
	if b.mapper == nil {
		t.Error("expected mapper to be initialized")
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

	type DBConfig struct {
		Host string `json:"host"`
		Port int    `json:"port"`
	}

	// Set up hierarchical config keys
	if err := cfg.Set(ctx, "database/host", "localhost"); err != nil {
		t.Fatalf("Set error: %v", err)
	}
	if err := cfg.Set(ctx, "database/port", 5432); err != nil {
		t.Fatalf("Set error: %v", err)
	}

	b := New(cfg)
	bound := b.Bind()

	var result DBConfig
	if err := bound.GetStruct(ctx, "database", &result); err != nil {
		t.Fatalf("GetStruct error: %v", err)
	}

	if result.Host != "localhost" {
		t.Errorf("expected host 'localhost', got %q", result.Host)
	}
	if result.Port != 5432 {
		t.Errorf("expected port 5432, got %d", result.Port)
	}
}

func TestGetStructNested(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	type PoolConfig struct {
		MaxSize int `json:"max_size"`
		MinSize int `json:"min_size"`
	}
	type DBConfig struct {
		Host string     `json:"host"`
		Port int        `json:"port"`
		Pool PoolConfig `json:"pool"`
	}

	// Set up nested hierarchical config keys
	if err := cfg.Set(ctx, "database/host", "localhost"); err != nil {
		t.Fatalf("Set error: %v", err)
	}
	if err := cfg.Set(ctx, "database/port", 5432); err != nil {
		t.Fatalf("Set error: %v", err)
	}
	if err := cfg.Set(ctx, "database/pool/max_size", 100); err != nil {
		t.Fatalf("Set error: %v", err)
	}
	if err := cfg.Set(ctx, "database/pool/min_size", 10); err != nil {
		t.Fatalf("Set error: %v", err)
	}

	b := New(cfg)
	bound := b.Bind()

	var result DBConfig
	if err := bound.GetStruct(ctx, "database", &result); err != nil {
		t.Fatalf("GetStruct error: %v", err)
	}

	if result.Host != "localhost" {
		t.Errorf("expected host 'localhost', got %q", result.Host)
	}
	if result.Port != 5432 {
		t.Errorf("expected port 5432, got %d", result.Port)
	}
	if result.Pool.MaxSize != 100 {
		t.Errorf("expected pool.max_size 100, got %d", result.Pool.MaxSize)
	}
	if result.Pool.MinSize != 10 {
		t.Errorf("expected pool.min_size 10, got %d", result.Pool.MinSize)
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

	// Verify individual keys were set
	nameVal, err := cfg.Get(ctx, "app/name")
	if err != nil {
		t.Fatalf("Get error for app/name: %v", err)
	}
	var name string
	if err := nameVal.Unmarshal(&name); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if name != "myapp" {
		t.Errorf("expected name 'myapp', got %q", name)
	}

	versionVal, err := cfg.Get(ctx, "app/version")
	if err != nil {
		t.Fatalf("Get error for app/version: %v", err)
	}
	var version string
	if err := versionVal.Unmarshal(&version); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if version != "1.0.0" {
		t.Errorf("expected version '1.0.0', got %q", version)
	}
}

func TestSetStructNested(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)
	b := New(cfg)
	bound := b.Bind()

	type CacheConfig struct {
		TTL     int  `json:"ttl"`
		Enabled bool `json:"enabled"`
	}
	type AppConfig struct {
		Name  string      `json:"name"`
		Cache CacheConfig `json:"cache"`
	}
	testValue := AppConfig{
		Name: "myapp",
		Cache: CacheConfig{
			TTL:     300,
			Enabled: true,
		},
	}

	if err := bound.SetStruct(ctx, "app", testValue); err != nil {
		t.Fatalf("SetStruct error: %v", err)
	}

	// Verify nested keys were set
	ttlVal, err := cfg.Get(ctx, "app/cache/ttl")
	if err != nil {
		t.Fatalf("Get error for app/cache/ttl: %v", err)
	}
	var ttl int
	if err := ttlVal.Unmarshal(&ttl); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if ttl != 300 {
		t.Errorf("expected ttl 300, got %d", ttl)
	}

	enabledVal, err := cfg.Get(ctx, "app/cache/enabled")
	if err != nil {
		t.Fatalf("Get error for app/cache/enabled: %v", err)
	}
	var enabled bool
	if err := enabledVal.Unmarshal(&enabled); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if !enabled {
		t.Error("expected enabled true")
	}
}

func TestSetGetRoundTrip(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)
	b := New(cfg)
	bound := b.Bind()

	type DBConfig struct {
		Host string `json:"host"`
		Port int    `json:"port"`
	}

	original := DBConfig{Host: "localhost", Port: 5432}

	// Set struct
	if err := bound.SetStruct(ctx, "database", original); err != nil {
		t.Fatalf("SetStruct error: %v", err)
	}

	// Get struct back
	var result DBConfig
	if err := bound.GetStruct(ctx, "database", &result); err != nil {
		t.Fatalf("GetStruct error: %v", err)
	}

	if result.Host != original.Host {
		t.Errorf("expected host %q, got %q", original.Host, result.Host)
	}
	if result.Port != original.Port {
		t.Errorf("expected port %d, got %d", original.Port, result.Port)
	}
}

func TestNonrecursiveTag(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)
	b := New(cfg)
	bound := b.Bind()

	// Credentials should be stored as a single value, not flattened
	type Credentials struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}

	type AppConfig struct {
		Name  string      `json:"name"`
		Creds Credentials `json:"creds,nonrecursive"` //nolint:staticcheck // nonrecursive is a custom bind option
	}

	original := AppConfig{
		Name: "myapp",
		Creds: Credentials{
			Username: "admin",
			Password: "secret",
		},
	}

	// Set struct
	if err := bound.SetStruct(ctx, "app", original); err != nil {
		t.Fatalf("SetStruct error: %v", err)
	}

	// Verify that "app/name" exists (normal field)
	nameVal, err := cfg.Get(ctx, "app/name")
	if err != nil {
		t.Fatalf("Get error for app/name: %v", err)
	}
	var name string
	if err := nameVal.Unmarshal(&name); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if name != "myapp" {
		t.Errorf("expected name 'myapp', got %q", name)
	}

	// Verify that "app/creds" exists as a single value (not flattened)
	credsVal, err := cfg.Get(ctx, "app/creds")
	if err != nil {
		t.Fatalf("Get error for app/creds: %v", err)
	}
	var creds Credentials
	if err := credsVal.Unmarshal(&creds); err != nil {
		t.Fatalf("Unmarshal error for creds: %v", err)
	}
	if creds.Username != "admin" || creds.Password != "secret" {
		t.Errorf("expected creds {admin, secret}, got %+v", creds)
	}

	// Verify that "app/creds/username" does NOT exist (should not be flattened)
	_, err = cfg.Get(ctx, "app/creds/username")
	if err == nil {
		t.Error("expected app/creds/username to NOT exist (nonrecursive)")
	}

	// Get struct back - should work correctly
	var result AppConfig
	if err := bound.GetStruct(ctx, "app", &result); err != nil {
		t.Fatalf("GetStruct error: %v", err)
	}

	if result.Name != original.Name {
		t.Errorf("expected name %q, got %q", original.Name, result.Name)
	}
	if result.Creds.Username != original.Creds.Username {
		t.Errorf("expected username %q, got %q", original.Creds.Username, result.Creds.Username)
	}
	if result.Creds.Password != original.Creds.Password {
		t.Errorf("expected password %q, got %q", original.Creds.Password, result.Creds.Password)
	}
}

func TestWithTagValidation(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	type Config struct {
		Host string `json:"host" validate:"required"`
		Port int    `json:"port" validate:"required,min=1,max=65535"`
	}

	b := New(cfg, WithTagValidation())
	bound := b.Bind()

	// Valid config should pass
	validConfig := Config{Host: "localhost", Port: 8080}
	if err := bound.SetStruct(ctx, "app", validConfig); err != nil {
		t.Fatalf("SetStruct error for valid config: %v", err)
	}

	// Get it back and validate
	var result Config
	if err := bound.GetStruct(ctx, "app", &result); err != nil {
		t.Fatalf("GetStruct error: %v", err)
	}

	if result.Host != "localhost" || result.Port != 8080 {
		t.Errorf("unexpected result: %+v", result)
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

func TestTagValidationRequired(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	type Config struct {
		Name string `json:"name" validate:"required"`
	}

	b := New(cfg, WithTagValidation())
	bound := b.Bind()

	// Empty value should fail
	err := bound.SetStruct(ctx, "key", Config{Name: ""})
	if err == nil {
		t.Error("expected validation error for empty required field")
	}
}

func TestTagValidationMinMax(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	type Config struct {
		Port int `json:"port" validate:"min=1,max=65535"`
	}

	b := New(cfg, WithTagValidation())
	bound := b.Bind()

	// Below min should fail
	err := bound.SetStruct(ctx, "key", Config{Port: 0})
	if err == nil {
		t.Error("expected validation error for port below min")
	}

	// Above max should fail
	err = bound.SetStruct(ctx, "key2", Config{Port: 70000})
	if err == nil {
		t.Error("expected validation error for port above max")
	}

	// Valid value should pass
	err = bound.SetStruct(ctx, "key3", Config{Port: 8080})
	if err != nil {
		t.Errorf("unexpected error for valid port: %v", err)
	}
}

func TestSetStructValidationError(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	type Config struct {
		Value string `json:"value" validate:"required"`
	}

	b := New(cfg, WithTagValidation())
	bound := b.Bind()

	// Empty required field should return validation error
	err := bound.SetStruct(ctx, "key", Config{Value: ""})
	if err == nil {
		t.Error("expected ValidationError for empty required field")
	}
}

func TestGetStructValidationError(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	// Set up test data with empty value (which will fail validation)
	if err := cfg.Set(ctx, "key/value", ""); err != nil {
		t.Fatalf("Set error: %v", err)
	}

	type Config struct {
		Value string `json:"value" validate:"required"`
	}

	b := New(cfg, WithTagValidation())
	bound := b.Bind()

	var result Config
	err := bound.GetStruct(ctx, "key", &result)
	if err == nil {
		t.Error("expected validation error for empty required field")
	}
}

func TestBinderValidate(t *testing.T) {
	cfg := setupTestConfig(t)

	type Config struct {
		Name string `validate:"required"`
	}

	// Without tag validation, Validate should always succeed
	b := New(cfg)
	if err := b.Validate(Config{Name: ""}); err != nil {
		t.Errorf("Validate error without tag validation: %v", err)
	}

	// With tag validation enabled
	b = New(cfg, WithTagValidation())

	// Should pass for valid value
	if err := b.Validate(Config{Name: "test"}); err != nil {
		t.Errorf("unexpected validation error: %v", err)
	}

	// Should fail for empty required field
	if err := b.Validate(Config{Name: ""}); err == nil {
		t.Error("expected validation error for empty required field")
	}
}

func TestFieldMapperStructToFlatMap(t *testing.T) {
	type Inner struct {
		Value string `json:"value"`
	}
	type TestStruct struct {
		Name  string `json:"name"`
		Inner Inner  `json:"inner"`
	}

	s := TestStruct{
		Name:  "test",
		Inner: Inner{Value: "nested"},
	}

	mapper := NewFieldMapper("json")
	result, err := mapper.StructToFlatMap(s, "prefix")
	if err != nil {
		t.Fatalf("StructToFlatMap error: %v", err)
	}

	if result["prefix/name"] != "test" {
		t.Errorf("expected prefix/name 'test', got %v", result["prefix/name"])
	}
	if result["prefix/inner/value"] != "nested" {
		t.Errorf("expected prefix/inner/value 'nested', got %v", result["prefix/inner/value"])
	}
}

func TestFieldMapperFlatMapToStruct(t *testing.T) {
	type Inner struct {
		Value string `json:"value"`
	}
	type TestStruct struct {
		Name  string `json:"name"`
		Inner Inner  `json:"inner"`
	}

	data := map[string]any{
		"prefix/name":        "test",
		"prefix/inner/value": "nested",
	}

	mapper := NewFieldMapper("json")
	var result TestStruct
	if err := mapper.FlatMapToStruct(data, "prefix", &result); err != nil {
		t.Fatalf("FlatMapToStruct error: %v", err)
	}

	if result.Name != "test" {
		t.Errorf("expected name 'test', got %q", result.Name)
	}
	if result.Inner.Value != "nested" {
		t.Errorf("expected inner.value 'nested', got %q", result.Inner.Value)
	}
}

func TestFieldMapperIgnoredField(t *testing.T) {
	type TestStruct struct {
		Name     string `json:"name"`
		Password string `json:"-"`
	}

	s := TestStruct{
		Name:     "test",
		Password: "secret",
	}

	mapper := NewFieldMapper("json")
	result, err := mapper.StructToFlatMap(s, "")
	if err != nil {
		t.Fatalf("StructToFlatMap error: %v", err)
	}

	if result["name"] != "test" {
		t.Errorf("expected name 'test', got %v", result["name"])
	}
	if _, exists := result["Password"]; exists {
		t.Error("ignored field should not be in result")
	}
	if _, exists := result["-"]; exists {
		t.Error("ignored field should not be in result with '-' key")
	}
}

func TestFieldMapperOmitempty(t *testing.T) {
	type TestStruct struct {
		Name  string `json:"name,omitempty"`
		Value int    `json:"value,omitempty"`
	}

	mapper := NewFieldMapper("json")
	result, err := mapper.StructToFlatMap(TestStruct{}, "") // All zero values
	if err != nil {
		t.Fatalf("StructToFlatMap error: %v", err)
	}

	if _, exists := result["name"]; exists {
		t.Error("empty name should be omitted")
	}
	if _, exists := result["value"]; exists {
		t.Error("zero value should be omitted")
	}
}

func TestSetStructValidationFailsAtomically(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	type Config struct {
		Name  string `json:"name" validate:"required"`
		Value string `json:"value" validate:"required"`
	}

	b := New(cfg, WithTagValidation())
	bound := b.Bind()

	// One field is empty, should fail validation
	err := bound.SetStruct(ctx, "test", Config{Name: "valid", Value: ""})
	if err == nil {
		t.Error("expected validation error for empty required field")
	}

	// Verify nothing was written - the valid field should NOT be in the store
	_, err = cfg.Get(ctx, "test/name")
	if !config.IsNotFound(err) {
		t.Error("expected test/name to not exist after failed validation")
	}

	_, err = cfg.Get(ctx, "test/value")
	if !config.IsNotFound(err) {
		t.Error("expected test/value to not exist after failed validation")
	}
}

func TestTagValidationEnum(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	type Config struct {
		Level string `json:"level" validate:"enum=debug|info|warn|error"`
	}

	b := New(cfg, WithTagValidation())
	bound := b.Bind()

	// Valid enum value should pass
	err := bound.SetStruct(ctx, "log", Config{Level: "info"})
	if err != nil {
		t.Errorf("unexpected error for valid enum value: %v", err)
	}

	// Invalid enum value should fail
	err = bound.SetStruct(ctx, "log2", Config{Level: "invalid"})
	if err == nil {
		t.Error("expected validation error for invalid enum value")
	}
}

func TestTagValidationPattern(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	type Config struct {
		Email string `json:"email" validate:"pattern=^[a-z]+@[a-z]+\\.[a-z]+$"`
	}

	b := New(cfg, WithTagValidation())
	bound := b.Bind()

	// Valid pattern should pass
	err := bound.SetStruct(ctx, "user", Config{Email: "test@example.com"})
	if err != nil {
		t.Errorf("unexpected error for valid pattern: %v", err)
	}

	// Invalid pattern should fail
	err = bound.SetStruct(ctx, "user2", Config{Email: "invalid-email"})
	if err == nil {
		t.Error("expected validation error for invalid pattern")
	}
}
