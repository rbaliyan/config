package config

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/rbaliyan/config/codec"
)

// Value provides type-safe access to configuration values.
type Value interface {
	// Marshal serializes the value to bytes using the configured codec.
	Marshal() ([]byte, error)

	// Unmarshal deserializes the value into the target.
	Unmarshal(v any) error

	// Raw returns the underlying raw value.
	Raw() any

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

// Metadata provides version and timestamp information for stored values.
type Metadata interface {
	// Version returns the version number of the value.
	Version() int64

	// CreatedAt returns when the value was first created.
	CreatedAt() time.Time

	// UpdatedAt returns when the value was last modified.
	UpdatedAt() time.Time

	// Tags returns the tags associated with this value, sorted by key.
	Tags() []Tag
}

// Val is the concrete Value implementation.
type Val struct {
	raw      any
	data     []byte
	dataType Type
	codec    codec.Codec
	metadata *valueMetadata
}

// valueMetadata implements Metadata interface.
type valueMetadata struct {
	version   int64
	createdAt time.Time
	updatedAt time.Time
	tags      []Tag
}

func (m *valueMetadata) Version() int64       { return m.version }
func (m *valueMetadata) CreatedAt() time.Time { return m.createdAt }
func (m *valueMetadata) UpdatedAt() time.Time { return m.updatedAt }
func (m *valueMetadata) Tags() []Tag          { return m.tags }

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

// WithValueTags sets the tags for the value.
func WithValueTags(tags []Tag) ValueOption {
	return func(v *Val) {
		if v.metadata == nil {
			v.metadata = &valueMetadata{}
		}
		v.metadata.tags = SortTags(tags)
	}
}

// NewValue creates a Val from any data with optional configuration.
func NewValue(data any, opts ...ValueOption) *Val {
	v := &Val{
		raw:      data,
		dataType: detectType(data),
		codec:    codec.Default(),
	}

	for _, opt := range opts {
		opt(v)
	}

	return v
}

// NewValueFromBytes creates a Val from encoded bytes.
func NewValueFromBytes(data []byte, codecName string, opts ...ValueOption) (*Val, error) {
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
		dataType: detectType(raw),
		codec:    c,
	}

	for _, opt := range opts {
		opt(v)
	}

	return v, nil
}

// Compile-time interface check
var _ Value = (*Val)(nil)

// Marshal serializes the value to bytes using the configured codec.
func (v *Val) Marshal() ([]byte, error) {
	if v.raw == nil {
		return nil, ErrNotFound
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
		return fmt.Errorf("%w: marshal failed: %v", ErrTypeMismatch, err)
	}
	if err := json.Unmarshal(data, target); err != nil {
		return fmt.Errorf("%w: unmarshal failed: %v", ErrTypeMismatch, err)
	}
	return nil
}

// Raw returns the underlying raw value.
func (v *Val) Raw() any {
	return v.raw
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

// IsNil returns true if the value is nil.
func (v *Val) IsNil() bool {
	return v.raw == nil
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
		return int64(val), nil
	case float64:
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

// Int returns the value as an int.
// Returns 0 if the value cannot be converted to int.
func (v *Val) Int() int {
	i, _ := v.Int64()
	return int(i)
}

// IntOr returns the value as an int, or the default if conversion fails.
func (v *Val) IntOr(defaultValue int) int {
	if i, err := v.Int64(); err == nil {
		return int(i)
	}
	return defaultValue
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

// Float returns the value as a float64.
// Returns 0 if the value cannot be converted to float64.
func (v *Val) Float() float64 {
	f, _ := v.Float64()
	return f
}

// FloatOr returns the value as a float64, or the default if conversion fails.
func (v *Val) FloatOr(defaultValue float64) float64 {
	if f, err := v.Float64(); err == nil {
		return f
	}
	return defaultValue
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

// StringValue returns the value as a string.
// Returns empty string if the value cannot be converted to string.
func (v *Val) StringValue() string {
	s, _ := v.String()
	return s
}

// StringOr returns the value as a string, or the default if conversion fails.
func (v *Val) StringOr(defaultValue string) string {
	if s, err := v.String(); err == nil {
		return s
	}
	return defaultValue
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

// BoolValue returns the value as a bool.
// Returns false if the value cannot be converted to bool.
func (v *Val) BoolValue() bool {
	b, _ := v.Bool()
	return b
}

// BoolOr returns the value as a bool, or the default if conversion fails.
func (v *Val) BoolOr(defaultValue bool) bool {
	if b, err := v.Bool(); err == nil {
		return b
	}
	return defaultValue
}

// MapStringInt returns the value as map[string]int.
// Returns nil if the value is not a map[string]int.
func (v *Val) MapStringInt() map[string]int {
	if v.raw == nil {
		return nil
	}
	if m, ok := v.raw.(map[string]int); ok {
		return m
	}
	// Try converting from map[string]interface{}
	if m, ok := v.raw.(map[string]any); ok {
		result := make(map[string]int, len(m))
		for k, val := range m {
			if i, ok := toInt(val); ok {
				result[k] = i
			}
		}
		return result
	}
	return nil
}

// MapStringFloat returns the value as map[string]float64.
// Returns nil if the value is not a map[string]float64.
func (v *Val) MapStringFloat() map[string]float64 {
	if v.raw == nil {
		return nil
	}
	if m, ok := v.raw.(map[string]float64); ok {
		return m
	}
	if m, ok := v.raw.(map[string]any); ok {
		result := make(map[string]float64, len(m))
		for k, val := range m {
			if f, ok := toFloat(val); ok {
				result[k] = f
			}
		}
		return result
	}
	return nil
}

// MapStringString returns the value as map[string]string.
// Returns nil if the value is not a map[string]string.
func (v *Val) MapStringString() map[string]string {
	if v.raw == nil {
		return nil
	}
	if m, ok := v.raw.(map[string]string); ok {
		return m
	}
	if m, ok := v.raw.(map[string]any); ok {
		result := make(map[string]string, len(m))
		for k, val := range m {
			result[k] = fmt.Sprintf("%v", val)
		}
		return result
	}
	return nil
}

// ListInt returns the value as []int.
// Returns nil if the value is not a []int.
func (v *Val) ListInt() []int {
	if v.raw == nil {
		return nil
	}
	if l, ok := v.raw.([]int); ok {
		return l
	}
	if l, ok := v.raw.([]any); ok {
		result := make([]int, 0, len(l))
		for _, val := range l {
			if i, ok := toInt(val); ok {
				result = append(result, i)
			}
		}
		return result
	}
	return nil
}

// ListFloat returns the value as []float64.
// Returns nil if the value is not a []float64.
func (v *Val) ListFloat() []float64 {
	if v.raw == nil {
		return nil
	}
	if l, ok := v.raw.([]float64); ok {
		return l
	}
	if l, ok := v.raw.([]any); ok {
		result := make([]float64, 0, len(l))
		for _, val := range l {
			if f, ok := toFloat(val); ok {
				result = append(result, f)
			}
		}
		return result
	}
	return nil
}

// ListString returns the value as []string.
// Returns nil if the value is not a []string.
func (v *Val) ListString() []string {
	if v.raw == nil {
		return nil
	}
	if l, ok := v.raw.([]string); ok {
		return l
	}
	if l, ok := v.raw.([]any); ok {
		result := make([]string, 0, len(l))
		for _, val := range l {
			result = append(result, fmt.Sprintf("%v", val))
		}
		return result
	}
	return nil
}

// Duration returns the value as time.Duration.
// Supports int (nanoseconds), float (seconds), and string (parseable duration).
// Returns 0 if the value cannot be converted to duration.
func (v *Val) Duration() time.Duration {
	return v.DurationOr(0)
}

// DurationOr returns the value as time.Duration, or the default if conversion fails.
func (v *Val) DurationOr(defaultValue time.Duration) time.Duration {
	if v.raw == nil {
		return defaultValue
	}
	switch val := v.raw.(type) {
	case time.Duration:
		return val
	case int:
		return time.Duration(val)
	case int64:
		return time.Duration(val)
	case float64:
		return time.Duration(val * float64(time.Second))
	case string:
		if d, err := time.ParseDuration(val); err == nil {
			return d
		}
	}
	return defaultValue
}

// Helper functions

func detectType(data any) Type {
	switch data.(type) {
	case int, int8, int16, int32, int64:
		return TypeInt
	case float32, float64:
		return TypeFloat
	case string:
		return TypeString
	case bool:
		return TypeBool
	case map[string]int:
		return TypeMapStringInt
	case map[string]float64:
		return TypeMapStringFloat
	case map[string]string:
		return TypeMapStringString
	case []int:
		return TypeListInt
	case []float64:
		return TypeListFloat
	case []string:
		return TypeListString
	default:
		return TypeCustom
	}
}

func toInt(val any) (int, bool) {
	switch v := val.(type) {
	case int:
		return v, true
	case int64:
		return int(v), true
	case float64:
		return int(v), true
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return int(i), true
		}
	}
	return 0, false
}

func toFloat(val any) (float64, bool) {
	switch v := val.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case json.Number:
		if f, err := v.Float64(); err == nil {
			return f, true
		}
	}
	return 0, false
}
