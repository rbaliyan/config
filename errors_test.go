package config_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/codec"
	"github.com/rbaliyan/config/memory"
)

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
