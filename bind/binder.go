// Package bind provides struct binding and validation for the config library.
// It wraps the Config interface without modifying it, adding struct binding capabilities.
package bind

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"maps"
	"slices"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/codec"
)

// Binder wraps Config with struct binding capabilities.
// It does NOT modify the Config interface - purely additive.
type Binder struct {
	cfg       config.Config
	codec     codec.Codec
	validator *TagValidator // nil means no validation
	fieldTag  string        // struct tag for field mapping (default: "json")
	mapper    *FieldMapper  // field mapper for struct conversion
}

// New creates a new Binder wrapping the given Config.
func New(cfg config.Config, opts ...Option) *Binder {
	b := &Binder{
		cfg:      cfg,
		codec:    codec.Default(),
		fieldTag: "json", // default tag
	}

	for _, opt := range opts {
		opt(b)
	}

	// Initialize mapper with the configured tag
	b.mapper = NewFieldMapper(b.fieldTag)

	return b
}

// Bind returns a StructConfig with struct binding capabilities.
func (b *Binder) Bind() StructConfig {
	return &boundConfig{
		Config: b.cfg,
		binder: b,
	}
}

// Config returns the underlying Config interface.
func (b *Binder) Config() config.Config {
	return b.cfg
}

// Validate validates a struct using tag-based validation.
// Returns nil if validation passes or no validator is configured.
func (b *Binder) Validate(value any) error {
	if b.validator == nil {
		return nil
	}
	return b.validator.Validate(value)
}

// StructConfig extends Config with struct binding methods.
// This interface embeds config.Config so it can be used as a drop-in replacement.
type StructConfig interface {
	config.Config // Embed original interface

	// GetStruct reads all keys with the given prefix and maps them to the target struct.
	// For example, GetStruct(ctx, "database", &cfg) reads database/host, database/port, etc.
	// and maps them to the corresponding struct fields.
	// Validates the result if validators are configured.
	GetStruct(ctx context.Context, key string, target any) error

	// GetStructDigest is like GetStruct but also returns an FNV-64a digest
	// computed from the raw config values. Two calls that return the same
	// digest populated the target with identical data. This allows callers
	// to detect changes without comparing structs via reflection.
	GetStructDigest(ctx context.Context, key string, target any) (uint64, error)

	// SetStruct flattens the struct into individual keys and stores them.
	// For example, SetStruct(ctx, "database", cfg) where cfg has Host and Port fields
	// will set database/host and database/port keys.
	// Validates before storing if validators are configured.
	SetStruct(ctx context.Context, key string, value any, opts ...config.SetOption) error
}

// boundConfig implements StructConfig
type boundConfig struct {
	config.Config
	binder *Binder
}

// GetStruct reads all keys with the given prefix and maps them to the target struct.
func (bc *boundConfig) GetStruct(ctx context.Context, key string, target any) error {
	// Find all keys with the given prefix
	page, err := bc.Find(ctx, config.NewFilter().WithPrefix(key).Build())
	if err != nil {
		return err
	}

	entries := page.Results()
	if len(entries) == 0 {
		return &config.KeyNotFoundError{Key: key}
	}

	// Convert entries to flat map
	data := make(map[string]any)
	for entryKey, val := range entries {
		var value any
		if err := val.Unmarshal(&value); err != nil {
			continue
		}
		data[entryKey] = value
	}

	// Use the field mapper to convert flat map to struct
	if err := bc.binder.mapper.FlatMapToStruct(data, key, target); err != nil {
		return &BindError{
			Key: key,
			Op:  "unmarshal",
			Err: err,
		}
	}

	// Run tag validation if configured
	if bc.binder.validator != nil {
		if err := bc.binder.validator.Validate(target); err != nil {
			return err
		}
	}

	return nil
}

// GetStructDigest is like GetStruct but also returns an FNV-64a digest
// computed from the raw config values before struct mapping.
func (bc *boundConfig) GetStructDigest(ctx context.Context, key string, target any) (uint64, error) {
	page, err := bc.Find(ctx, config.NewFilter().WithPrefix(key).Build())
	if err != nil {
		return 0, err
	}

	entries := page.Results()
	if len(entries) == 0 {
		return 0, &config.KeyNotFoundError{Key: key}
	}

	// Convert entries to flat map
	data := make(map[string]any)
	for entryKey, val := range entries {
		var value any
		if err := val.Unmarshal(&value); err != nil {
			continue
		}
		data[entryKey] = value
	}

	// Compute FNV-64a digest from sorted keys for deterministic ordering
	h := fnv.New64a()
	for _, k := range slices.Sorted(maps.Keys(data)) {
		_, _ = io.WriteString(h, k)
		_, _ = fmt.Fprint(h, data[k])
	}
	digest := h.Sum64()

	// Use the field mapper to convert flat map to struct
	if err := bc.binder.mapper.FlatMapToStruct(data, key, target); err != nil {
		return 0, &BindError{
			Key: key,
			Op:  "unmarshal",
			Err: err,
		}
	}

	// Run tag validation if configured
	if bc.binder.validator != nil {
		if err := bc.binder.validator.Validate(target); err != nil {
			return 0, err
		}
	}

	return digest, nil
}

// SetStruct flattens the struct into individual keys and stores them.
// If validation is configured, the struct is validated first.
// Only if validation passes, the keys are written to the store.
func (bc *boundConfig) SetStruct(ctx context.Context, key string, value any, opts ...config.SetOption) error {
	// Validate struct first if configured
	if bc.binder.validator != nil {
		if err := bc.binder.validator.Validate(value); err != nil {
			return err
		}
	}

	// Flatten struct to individual key/value pairs
	flatMap, err := bc.binder.mapper.StructToFlatMap(value, key)
	if err != nil {
		return &BindError{
			Key: key,
			Op:  "marshal",
			Err: err,
		}
	}

	// Set each key
	for k, v := range flatMap {
		if err := bc.Set(ctx, k, v, opts...); err != nil {
			return err
		}
	}

	return nil
}
