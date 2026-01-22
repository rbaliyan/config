package config

import (
	"errors"
	"fmt"
)

// Sentinel errors for config operations.
// Use errors.Is() to check for these errors as they may be wrapped.
var (
	// ErrNotFound is returned when a config key does not exist.
	ErrNotFound = errors.New("config: key not found")

	// ErrTypeMismatch is returned when attempting to convert a value to an incompatible type.
	ErrTypeMismatch = errors.New("config: type mismatch")

	// ErrInvalidKey is returned when a config key is empty or malformed.
	ErrInvalidKey = errors.New("config: invalid key")

	// ErrInvalidNamespace is returned when a namespace is malformed.
	ErrInvalidNamespace = errors.New("config: invalid namespace")

	// ErrInvalidValue is returned when a value cannot be stored.
	ErrInvalidValue = errors.New("config: invalid value")

	// ErrStoreNotConnected is returned when operating on a disconnected store.
	ErrStoreNotConnected = errors.New("config: store not connected")

	// ErrStoreClosed is returned when operating on a closed store.
	ErrStoreClosed = errors.New("config: store closed")

	// ErrCacheDisabled is returned when cache operations are attempted with caching disabled.
	ErrCacheDisabled = errors.New("config: cache disabled")

	// ErrWatchNotSupported is returned when the store does not support watching.
	ErrWatchNotSupported = errors.New("config: watch not supported")

	// ErrManagerClosed is returned when operating on a closed manager.
	ErrManagerClosed = errors.New("config: manager closed")

	// ErrCodecNotFound is returned when a codec is not registered.
	ErrCodecNotFound = errors.New("config: codec not found")

	// ErrReadOnly is returned when attempting to write to a read-only store.
	ErrReadOnly = errors.New("config: store is read-only")
)

// KeyNotFoundError provides details about a missing key.
type KeyNotFoundError struct {
	Key       string
	Namespace string
}

func (e *KeyNotFoundError) Error() string {
	if e.Namespace != "" {
		return fmt.Sprintf("config: key %q not found in namespace %q", e.Key, e.Namespace)
	}
	return fmt.Sprintf("config: key %q not found", e.Key)
}

func (e *KeyNotFoundError) Unwrap() error {
	return ErrNotFound
}

// IsNotFound checks if an error indicates a missing key.
func IsNotFound(err error) bool {
	return errors.Is(err, ErrNotFound)
}

// TypeMismatchError provides details about a type conversion failure.
type TypeMismatchError struct {
	Key      string
	Expected Type
	Actual   Type
}

func (e *TypeMismatchError) Error() string {
	return fmt.Sprintf("config: key %q has type %s, cannot convert to %s", e.Key, e.Actual, e.Expected)
}

func (e *TypeMismatchError) Unwrap() error {
	return ErrTypeMismatch
}

// IsTypeMismatch checks if an error indicates a type mismatch.
func IsTypeMismatch(err error) bool {
	return errors.Is(err, ErrTypeMismatch)
}

// InvalidKeyError provides details about an invalid key.
type InvalidKeyError struct {
	Key    string
	Reason string
}

func (e *InvalidKeyError) Error() string {
	return fmt.Sprintf("config: invalid key %q: %s", e.Key, e.Reason)
}

func (e *InvalidKeyError) Unwrap() error {
	return ErrInvalidKey
}

// IsInvalidKey checks if an error indicates an invalid key.
func IsInvalidKey(err error) bool {
	return errors.Is(err, ErrInvalidKey)
}

// StoreError wraps backend-specific errors with domain context.
type StoreError struct {
	Op      string // Operation that failed
	Key     string // Key involved (if applicable)
	Backend string // Backend name (memory, mongodb, postgres)
	Err     error  // Underlying error
}

func (e *StoreError) Error() string {
	if e.Key != "" {
		return fmt.Sprintf("config: %s [%s] key=%q: %v", e.Op, e.Backend, e.Key, e.Err)
	}
	return fmt.Sprintf("config: %s [%s]: %v", e.Op, e.Backend, e.Err)
}

func (e *StoreError) Unwrap() error {
	return e.Err
}

// WrapStoreError creates a StoreError from a backend error.
func WrapStoreError(op, backend, key string, err error) error {
	if err == nil {
		return nil
	}
	// Don't double-wrap
	var se *StoreError
	if errors.As(err, &se) {
		return err
	}
	return &StoreError{Op: op, Backend: backend, Key: key, Err: err}
}
