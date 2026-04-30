package config

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/rbaliyan/config/codec"
)

// Value provides type-safe access to configuration values.
// This interface is focused on read operations. Write-related concerns
// like WriteMode are accessed via the WriteModer interface.
type Value interface {
	// Marshal serializes the value to bytes using the configured codec.
	Marshal(ctx context.Context) ([]byte, error)

	// Unmarshal deserializes the value into the target.
	Unmarshal(ctx context.Context, v any) error

	// Type returns the detected type of the value.
	Type() Type

	// Codec returns the codec name used for this value.
	Codec() string

	// Int64 returns the value as int64.
	Int64() (int64, error)

	// Float64 returns the value as float64.
	Float64() (float64, error)

	// String returns the value as string.
	String() (string, error)

	// Bool returns the value as bool.
	Bool() (bool, error)

	// Metadata returns associated metadata, if any.
	Metadata() Metadata
}

// WriteModer is an optional interface for values that carry write mode hints.
// Store implementations check for this interface in Set operations to determine
// conditional write behavior (create-only, update-only, or upsert).
type WriteModer interface {
	WriteMode() WriteMode
}

// Metadata provides version and timestamp information for stored values.
type Metadata interface {
	// Version returns the version number of the value.
	Version() int64

	// CreatedAt returns when the value was first created.
	CreatedAt() time.Time

	// UpdatedAt returns when the value was last modified.
	UpdatedAt() time.Time

	// IsStale returns true if this value was served from cache due to a store error.
	// When true, the value may be outdated. Applications can use this to:
	// - Log warnings about stale data
	// - Show degraded UI indicators
	// - Trigger background refresh
	IsStale() bool

	// ExpiresAt returns the absolute time at which this value expires.
	// A zero time means no TTL (the value never expires).
	ExpiresAt() time.Time
}

// storeMetadata extends Metadata with internal fields used by store implementations.
type storeMetadata interface {
	Metadata

	// EntryID returns the unique storage identifier for this entry.
	// This is the database ID (e.g., PostgreSQL BIGSERIAL, MongoDB ObjectID).
	// Used internally for pagination and store operations.
	EntryID() string
}

// EntryID returns the storage-level identifier associated with v, if any.
// The identifier is an implementation detail used by Store backends for
// pagination and update paths; it is empty for values created via NewValue
// before they have been written.
func EntryID(v Value) string {
	if v == nil {
		return ""
	}
	m := v.Metadata()
	if sm, ok := m.(storeMetadata); ok {
		return sm.EntryID()
	}
	return ""
}

// val is the concrete Value implementation.
type val struct {
	raw       any
	data      []byte
	dataType  Type
	codec     codec.Codec
	metadata  *valueMetadata
	writeMode WriteMode
}

// valueMetadata implements Metadata and StoreMetadata interfaces.
type valueMetadata struct {
	version   int64
	createdAt time.Time
	updatedAt time.Time
	expiresAt time.Time // zero = no TTL
	entryID   string    // Internal: database-level entry ID for store operations
	stale     bool      // True if served from cache due to store error
}

func (m *valueMetadata) Version() int64       { return m.version }
func (m *valueMetadata) CreatedAt() time.Time { return m.createdAt }
func (m *valueMetadata) UpdatedAt() time.Time { return m.updatedAt }
func (m *valueMetadata) ExpiresAt() time.Time { return m.expiresAt }
func (m *valueMetadata) EntryID() string      { return m.entryID }
func (m *valueMetadata) IsStale() bool        { return m.stale }

// Compile-time interface checks for valueMetadata
var (
	_ Metadata      = (*valueMetadata)(nil)
	_ storeMetadata = (*valueMetadata)(nil)
)

// ValueOption configures a value during construction.
type ValueOption func(*val)

// WithValueCodec sets the codec for the value.
func WithValueCodec(c codec.Codec) ValueOption {
	return func(v *val) {
		v.codec = c
	}
}

// WithValueType sets the type for the value.
func WithValueType(t Type) ValueOption {
	return func(v *val) {
		v.dataType = t
	}
}

// WithValueMetadata sets metadata for the value.
func WithValueMetadata(version int64, createdAt, updatedAt time.Time) ValueOption {
	return func(v *val) {
		if v.metadata == nil {
			v.metadata = &valueMetadata{}
		}
		v.metadata.version = version
		v.metadata.createdAt = createdAt
		v.metadata.updatedAt = updatedAt
	}
}

// WithValueEntryID sets the internal entry ID for the value.
// This is used by store implementations to track the database-level ID.
// Not intended for end-user code.
func WithValueEntryID(id string) ValueOption {
	return func(v *val) {
		if v.metadata == nil {
			v.metadata = &valueMetadata{}
		}
		v.metadata.entryID = id
	}
}

// WithValueExpiresAt sets the absolute expiry time on the value.
// A zero time means no TTL. Used by store backends when reading back stored values.
func WithValueExpiresAt(t time.Time) ValueOption {
	return func(v *val) {
		if v.metadata == nil {
			v.metadata = &valueMetadata{}
		}
		v.metadata.expiresAt = t
	}
}

// GetExpiresAt returns the expiry time stored in v's metadata, or zero if none.
func GetExpiresAt(v Value) time.Time {
	if v == nil {
		return time.Time{}
	}
	return v.Metadata().ExpiresAt()
}

// IsExpired reports whether v has a non-zero expiry time that is in the past.
func IsExpired(v Value) bool {
	t := GetExpiresAt(v)
	return !t.IsZero() && time.Now().After(t)
}

// WithValueStale marks the value as stale (served from cache due to store error).
// This is used internally by the Manager when falling back to cached values.
func WithValueStale(stale bool) ValueOption {
	return func(v *val) {
		if v.metadata == nil {
			v.metadata = &valueMetadata{}
		}
		v.metadata.stale = stale
	}
}

// WithValueWriteMode sets the write mode for the value.
func WithValueWriteMode(mode WriteMode) ValueOption {
	return func(v *val) {
		v.writeMode = mode
	}
}

// rawCodec is a codec placeholder for values whose encoding is managed
// externally (e.g., client-side encryption). It preserves the codec name
// but refuses to encode or decode — the server treats the bytes as opaque.
type rawCodec struct {
	name string
}

func (r *rawCodec) Name() string { return r.name }
func (r *rawCodec) Encode(_ context.Context, _ any) ([]byte, error) {
	return nil, fmt.Errorf("rawCodec %q: encode not supported", r.name)
}
func (r *rawCodec) Decode(_ context.Context, _ []byte, _ any) error {
	return fmt.Errorf("rawCodec %q: decode not supported", r.name)
}

// Compile-time interface check for rawCodec.
var _ codec.Codec = (*rawCodec)(nil)

// secretCodec is a byte-pass-through codec for TypeSecret values.
// It is registered globally so that store backends (PostgreSQL, MongoDB, Redis,
// SQLite) can look it up by name when reconstructing a TypeSecret Value on read.
// Encode accepts []byte or *Secret and returns the raw bytes unchanged.
// Decode into *any sets the target to the raw []byte (for NewValueFromBytes);
// decode into **Secret wraps the bytes in a new Secret.
type secretCodec struct{}

func (secretCodec) Name() string { return "secret" }

func (secretCodec) Encode(_ context.Context, v any) ([]byte, error) {
	switch b := v.(type) {
	case []byte:
		return b, nil
	case *Secret:
		return b.Bytes(), nil
	default:
		return nil, fmt.Errorf("secretCodec: cannot encode %T", v)
	}
}

func (secretCodec) Decode(_ context.Context, data []byte, v any) error {
	switch p := v.(type) {
	case *any:
		*p = data
		return nil
	case **Secret:
		*p = NewSecretBytes(data)
		return nil
	default:
		return fmt.Errorf("secretCodec: cannot decode into %T; use SecretFrom or **Secret", v)
	}
}

// Compile-time interface check for secretCodec.
var _ codec.Codec = secretCodec{}

func init() {
	_ = codec.Register(secretCodec{})
}

// NewRawValue creates a Value holding pre-marshaled bytes without decoding.
// The bytes are stored as-is and returned verbatim by Marshal().
// This is used when the server does not have the codec registered (e.g.,
// client-side encrypted data) and should pass the bytes through opaquely.
func NewRawValue(data []byte, codecName string, opts ...ValueOption) Value {
	v := &val{
		raw:      data, // non-nil so Marshal returns v.data
		data:     data,
		dataType: TypeCustom,
		codec:    &rawCodec{name: codecName},
	}
	for _, opt := range opts {
		opt(v)
	}
	return v
}

// CloneValue returns a new Value whose raw content is newRaw, with all metadata
// (version, timestamps, TTL, entryID, write mode) copied from src.
// The new value uses the default codec; pass WithValueCodec to override.
// The stale flag is not copied — the clone is a fresh value, not a cache fallback.
//
// Intended for Store wrappers that transform the raw bytes (e.g. encrypt/decrypt,
// compress, sign) without touching metadata. Instead of manually extracting every
// metadata field from src and re-applying them with ValueOption calls, write:
//
//	decrypted := config.CloneValue(stored, plaintext)
//	encrypted := config.CloneValue(incoming, ciphertext, config.WithValueCodec(encCodec))
func CloneValue(src Value, newRaw any, opts ...ValueOption) Value {
	v := &val{
		raw:      newRaw,
		dataType: detectType(newRaw),
		codec:    codec.Default(),
	}

	if sv, ok := src.(*val); ok {
		v.writeMode = sv.writeMode
		if sv.metadata != nil {
			cp := *sv.metadata
			cp.stale = false
			v.metadata = &cp
		}
	}

	for _, opt := range opts {
		opt(v)
	}
	return v
}

// NewValue creates a Value from any data with optional configuration.
func NewValue(data any, opts ...ValueOption) Value {
	v := &val{
		raw:      data,
		dataType: detectType(data),
		codec:    codec.Default(),
	}

	for _, opt := range opts {
		opt(v)
	}

	return v
}

// NewValueFromBytes creates a Value from encoded bytes.
// If the named codec is not registered and differs from the default codec,
// the bytes are stored as a raw pass-through value (see NewRawValue).
func NewValueFromBytes(ctx context.Context, data []byte, codecName string, opts ...ValueOption) (Value, error) {
	c := codec.Get(codecName)
	if c == nil {
		if codecName != "" && codecName != codec.Default().Name() {
			return NewRawValue(data, codecName, opts...), nil
		}
		c = codec.Default()
	}

	var raw any
	if err := c.Decode(ctx, data, &raw); err != nil {
		return nil, fmt.Errorf("decode value: %w", err)
	}

	v := &val{
		raw:      raw,
		data:     data,
		dataType: detectType(raw),
		codec:    c,
	}

	for _, opt := range opts {
		opt(v)
	}

	return v, nil
}

// MarkStale returns a copy of the value with the stale flag set.
// This is used when serving cached values due to store errors.
// The returned value's Metadata().IsStale() will return true.
func MarkStale(v Value) Value {
	if v == nil {
		return nil
	}

	// If it's our val type, we can copy it efficiently
	if src, ok := v.(*val); ok {
		cp := &val{
			raw:       src.raw,
			data:      src.data,
			dataType:  src.dataType,
			codec:     src.codec,
			writeMode: src.writeMode,
		}
		// Copy metadata and set stale flag
		if src.metadata != nil {
			cp.metadata = &valueMetadata{
				version:   src.metadata.version,
				createdAt: src.metadata.createdAt,
				updatedAt: src.metadata.updatedAt,
				expiresAt: src.metadata.expiresAt,
				entryID:   src.metadata.entryID,
				stale:     true,
			}
		} else {
			cp.metadata = &valueMetadata{stale: true}
		}
		return cp
	}

	// For other Value implementations, wrap with stale metadata
	// This shouldn't happen in practice since we control all Value creation
	return &staleValueWrapper{Value: v}
}

// staleValueWrapper wraps a Value to indicate it's stale.
type staleValueWrapper struct {
	Value
}

func (w *staleValueWrapper) Metadata() Metadata {
	return &staleMetadataWrapper{Metadata: w.Value.Metadata()}
}

// staleMetadataWrapper wraps Metadata to return stale=true.
type staleMetadataWrapper struct {
	Metadata
}

func (w *staleMetadataWrapper) IsStale() bool {
	return true
}

// Compile-time interface check
var _ Value = (*val)(nil)

// Marshal serializes the value to bytes using the configured codec.
func (v *val) Marshal(ctx context.Context) ([]byte, error) {
	if v.raw == nil {
		return nil, ErrInvalidValue
	}

	// If we already have encoded data and it matches the raw value, return it
	if v.data != nil {
		return v.data, nil
	}

	return v.codec.Encode(ctx, v.raw)
}

// Unmarshal deserializes the value into the target.
// For TypeSecret values, target must be **Secret; use SecretFrom for a safer API.
func (v *val) Unmarshal(ctx context.Context, target any) error {
	if v.raw == nil {
		return ErrInvalidValue
	}

	if v.dataType == TypeSecret {
		sp, ok := target.(**Secret)
		if !ok {
			return fmt.Errorf("%w: TypeSecret values must be unmarshaled into **Secret", ErrTypeMismatch)
		}
		*sp = NewSecretBytes(v.data)
		return nil
	}

	// If we have raw bytes, use the codec
	if v.data != nil && v.codec != nil {
		return v.codec.Decode(ctx, v.data, target)
	}

	// Otherwise use JSON round-trip
	data, err := json.Marshal(v.raw)
	if err != nil {
		return fmt.Errorf("%w: marshal failed: %w", ErrTypeMismatch, err)
	}
	if err := json.Unmarshal(data, target); err != nil {
		return fmt.Errorf("%w: unmarshal failed: %w", ErrTypeMismatch, err)
	}
	return nil
}

// Type returns the detected type of the value.
func (v *val) Type() Type {
	return v.dataType
}

// Codec returns the codec name.
func (v *val) Codec() string {
	if v.codec != nil {
		return v.codec.Name()
	}
	return "json"
}

// Metadata returns associated metadata.
func (v *val) Metadata() Metadata {
	if v.metadata == nil {
		return &valueMetadata{}
	}
	return v.metadata
}

// WriteMode returns the write mode for this value.
func (v *val) WriteMode() WriteMode {
	return v.writeMode
}

// Compile-time interface check
var _ WriteModer = (*val)(nil)

// GetWriteMode extracts the write mode from a Value.
// Returns WriteModeUpsert (default) if the value does not implement WriteModer.
// Store implementations should use this instead of calling WriteMode() directly.
func GetWriteMode(v Value) WriteMode {
	if wm, ok := v.(WriteModer); ok {
		return wm.WriteMode()
	}
	return WriteModeUpsert
}

// Int64 returns the value as int64.
// Returns an error if the value cannot be converted to int64.
func (v *val) Int64() (int64, error) {
	if v.raw == nil {
		return 0, ErrNotFound
	}
	switch val := v.raw.(type) {
	case int:
		return int64(val), nil
	case int8:
		return int64(val), nil
	case int16:
		return int64(val), nil
	case int32:
		return int64(val), nil
	case int64:
		return val, nil
	case float32:
		if float32(int64(val)) != val {
			return 0, fmt.Errorf("%w: cannot convert float32 %v to int64 without truncation", ErrTypeMismatch, val)
		}
		return int64(val), nil
	case float64:
		if val != math.Trunc(val) || math.IsInf(val, 0) || math.IsNaN(val) {
			return 0, fmt.Errorf("%w: cannot convert float64 %v to int64 without truncation", ErrTypeMismatch, val)
		}
		return int64(val), nil
	case string:
		if i, err := strconv.ParseInt(val, 10, 64); err == nil {
			return i, nil
		}
		return 0, fmt.Errorf("%w: cannot convert string %q to int64", ErrTypeMismatch, val)
	case json.Number:
		if i, err := val.Int64(); err == nil {
			return i, nil
		}
		return 0, fmt.Errorf("%w: cannot convert json.Number to int64", ErrTypeMismatch)
	}
	return 0, fmt.Errorf("%w: cannot convert %T to int64", ErrTypeMismatch, v.raw)
}

// Float64 returns the value as float64.
// Returns an error if the value cannot be converted to float64.
func (v *val) Float64() (float64, error) {
	if v.raw == nil {
		return 0, ErrNotFound
	}
	switch val := v.raw.(type) {
	case float64:
		return val, nil
	case float32:
		return float64(val), nil
	case int:
		return float64(val), nil
	case int8:
		return float64(val), nil
	case int16:
		return float64(val), nil
	case int32:
		return float64(val), nil
	case int64:
		return float64(val), nil
	case string:
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f, nil
		}
		return 0, fmt.Errorf("%w: cannot convert string %q to float64", ErrTypeMismatch, val)
	case json.Number:
		if f, err := val.Float64(); err == nil {
			return f, nil
		}
		return 0, fmt.Errorf("%w: cannot convert json.Number to float64", ErrTypeMismatch)
	}
	return 0, fmt.Errorf("%w: cannot convert %T to float64", ErrTypeMismatch, v.raw)
}

// String returns the value as string.
// Returns an error if the value is nil.
// For TypeSecret values, always returns "******" to prevent accidental leakage.
func (v *val) String() (string, error) {
	if v.raw == nil {
		return "", ErrNotFound
	}
	if v.dataType == TypeSecret {
		return secretMask, nil
	}
	switch val := v.raw.(type) {
	case string:
		return val, nil
	case []byte:
		return string(val), nil
	default:
		return fmt.Sprintf("%v", val), nil
	}
}

// Bool returns the value as bool.
// Returns an error if the value cannot be converted to bool.
func (v *val) Bool() (bool, error) {
	if v.raw == nil {
		return false, ErrNotFound
	}
	switch val := v.raw.(type) {
	case bool:
		return val, nil
	case int:
		return val != 0, nil
	case int64:
		return val != 0, nil
	case float64:
		return val != 0, nil
	case string:
		if b, err := strconv.ParseBool(val); err == nil {
			return b, nil
		}
		return false, fmt.Errorf("%w: cannot convert string %q to bool", ErrTypeMismatch, val)
	}
	return false, fmt.Errorf("%w: cannot convert %T to bool", ErrTypeMismatch, v.raw)
}

// Helper functions

// NewSecretValue creates a Value that holds a secret.
// The raw bytes are stored as-is under the "secret" codec with TypeSecret, so
// Marshal returns plaintext bytes suitable for an encrypted store or transport.
// String() always returns "******" regardless of how the value is accessed.
// Use SecretFrom to extract the *Secret from a stored Value.
func NewSecretValue(s *Secret, opts ...ValueOption) Value {
	if s == nil {
		s = &Secret{}
	}
	b := s.Bytes()
	v := &val{
		raw:      s.Clone(),
		data:     b,
		dataType: TypeSecret,
		codec:    secretCodec{},
	}
	for _, opt := range opts {
		opt(v)
	}
	return v
}

// SecretFrom extracts a *Secret from a Value of TypeSecret.
// The returned Secret holds a copy of the raw bytes; callers should call
// Wipe() when done to zero them from the heap.
// Returns ErrTypeMismatch if v is not TypeSecret.
func SecretFrom(_ context.Context, v Value) (*Secret, error) {
	if v == nil {
		return nil, ErrNotFound
	}
	if v.Type() != TypeSecret {
		return nil, fmt.Errorf("%w: want TypeSecret, got %s", ErrTypeMismatch, v.Type())
	}
	inner, ok := v.(*val)
	if !ok || inner.data == nil {
		return nil, fmt.Errorf("%w: secret data is not accessible", ErrInvalidValue)
	}
	return NewSecretBytes(inner.data), nil
}

// detectType maps a Go value to its config Type.
// It handles JSON-decoded numbers (float64 that are actually integers,
// json.Number) and unsigned integer types.
//
// Note: whole float64 values (e.g. 42.0) are classified as TypeInt because
// JSON decodes all numbers as float64. As a result, a float64 that is a whole
// number will have TypeInt, which can cause the detected type to change across
// a JSON round-trip (a TypeFloat 42.0 stored and retrieved via a JSON codec
// may come back as TypeInt). Callers that need TypeFloat should set the type
// explicitly with WithValueType(TypeFloat).
func detectType(data any) Type {
	switch val := data.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return TypeInt
	case float64:
		// JSON decodes all numbers as float64; detect integers.
		if val == math.Trunc(val) && !math.IsInf(val, 0) && !math.IsNaN(val) {
			return TypeInt
		}
		return TypeFloat
	case float32:
		return TypeFloat
	case json.Number:
		if _, err := val.Int64(); err == nil {
			return TypeInt
		}
		return TypeFloat
	case string:
		return TypeString
	case bool:
		return TypeBool
	case *Secret:
		return TypeSecret
	default:
		return TypeCustom
	}
}
