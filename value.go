package config

import (
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
	Marshal() ([]byte, error)

	// Unmarshal deserializes the value into the target.
	Unmarshal(v any) error

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
}

// StoreMetadata extends Metadata with internal fields used by store implementations.
// This interface is not intended for end-user code.
type StoreMetadata interface {
	Metadata

	// EntryID returns the unique storage identifier for this entry.
	// This is the database ID (e.g., PostgreSQL BIGSERIAL, MongoDB ObjectID).
	// Used internally for pagination and store operations.
	EntryID() string
}

// Val is the concrete Value implementation.
type Val struct {
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
	entryID   string // Internal: database-level entry ID for store operations
	stale     bool   // True if served from cache due to store error
}

func (m *valueMetadata) Version() int64       { return m.version }
func (m *valueMetadata) CreatedAt() time.Time { return m.createdAt }
func (m *valueMetadata) UpdatedAt() time.Time { return m.updatedAt }
func (m *valueMetadata) EntryID() string      { return m.entryID }
func (m *valueMetadata) IsStale() bool        { return m.stale }

// Compile-time interface checks for valueMetadata
var (
	_ Metadata      = (*valueMetadata)(nil)
	_ StoreMetadata = (*valueMetadata)(nil)
)

// ValueOption configures a Val during construction.
type ValueOption func(*Val)

// WithCodec sets the codec for the value.
func WithValueCodec(c codec.Codec) ValueOption {
	return func(v *Val) {
		v.codec = c
	}
}

// WithValueType sets the type for the value.
func WithValueType(t Type) ValueOption {
	return func(v *Val) {
		v.dataType = t
	}
}

// WithValueMetadata sets metadata for the value.
func WithValueMetadata(version int64, createdAt, updatedAt time.Time) ValueOption {
	return func(v *Val) {
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
	return func(v *Val) {
		if v.metadata == nil {
			v.metadata = &valueMetadata{}
		}
		v.metadata.entryID = id
	}
}

// WithValueStale marks the value as stale (served from cache due to store error).
// This is used internally by the Manager when falling back to cached values.
func WithValueStale(stale bool) ValueOption {
	return func(v *Val) {
		if v.metadata == nil {
			v.metadata = &valueMetadata{}
		}
		v.metadata.stale = stale
	}
}

// WithValueWriteMode sets the write mode for the value.
func WithValueWriteMode(mode WriteMode) ValueOption {
	return func(v *Val) {
		v.writeMode = mode
	}
}

// NewValue creates a Value from any data with optional configuration.
func NewValue(data any, opts ...ValueOption) Value {
	v := &Val{
		raw:      data,
		dataType: DetectType(data),
		codec:    codec.Default(),
	}

	for _, opt := range opts {
		opt(v)
	}

	return v
}

// NewValueFromBytes creates a Value from encoded bytes.
func NewValueFromBytes(data []byte, codecName string, opts ...ValueOption) (Value, error) {
	c := codec.Get(codecName)
	if c == nil {
		c = codec.Default()
	}

	var raw any
	if err := c.Decode(data, &raw); err != nil {
		return nil, fmt.Errorf("decode value: %w", err)
	}

	v := &Val{
		raw:      raw,
		data:     data,
		dataType: DetectType(raw),
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

	// If it's our Val type, we can copy it efficiently
	if val, ok := v.(*Val); ok {
		newVal := &Val{
			raw:       val.raw,
			data:      val.data,
			dataType:  val.dataType,
			codec:     val.codec,
			writeMode: val.writeMode,
		}
		// Copy metadata and set stale flag
		if val.metadata != nil {
			newVal.metadata = &valueMetadata{
				version:   val.metadata.version,
				createdAt: val.metadata.createdAt,
				updatedAt: val.metadata.updatedAt,
				entryID:   val.metadata.entryID,
				stale:     true,
			}
		} else {
			newVal.metadata = &valueMetadata{stale: true}
		}
		return newVal
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
var _ Value = (*Val)(nil)

// Marshal serializes the value to bytes using the configured codec.
func (v *Val) Marshal() ([]byte, error) {
	if v.raw == nil {
		return nil, ErrInvalidValue
	}

	// If we already have encoded data and it matches the raw value, return it
	if v.data != nil {
		return v.data, nil
	}

	return v.codec.Encode(v.raw)
}

// Unmarshal deserializes the value into the target.
func (v *Val) Unmarshal(target any) error {
	if v.raw == nil {
		return ErrNotFound
	}

	// If we have raw bytes, use the codec
	if v.data != nil && v.codec != nil {
		return v.codec.Decode(v.data, target)
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
func (v *Val) Type() Type {
	return v.dataType
}

// Codec returns the codec name.
func (v *Val) Codec() string {
	if v.codec != nil {
		return v.codec.Name()
	}
	return "json"
}

// Metadata returns associated metadata.
func (v *Val) Metadata() Metadata {
	if v.metadata == nil {
		return &valueMetadata{}
	}
	return v.metadata
}

// WriteMode returns the write mode for this value.
// Val implements WriteModer.
func (v *Val) WriteMode() WriteMode {
	return v.writeMode
}

// Compile-time interface check
var _ WriteModer = (*Val)(nil)

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
func (v *Val) Int64() (int64, error) {
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
func (v *Val) Float64() (float64, error) {
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
func (v *Val) String() (string, error) {
	if v.raw == nil {
		return "", ErrNotFound
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
func (v *Val) Bool() (bool, error) {
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

// DetectType maps a Go value to its config Type.
// It handles JSON-decoded numbers (float64 that are actually integers,
// json.Number) and unsigned integer types.
func DetectType(data any) Type {
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
	default:
		return TypeCustom
	}
}
