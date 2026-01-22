// Package bind provides struct binding and validation for the config library.
// It wraps the Config interface without modifying it, adding struct binding capabilities.
package bind

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/codec"
)

// Binder wraps Config with struct binding capabilities.
// It does NOT modify the Config interface - purely additive.
type Binder struct {
	cfg        config.Config
	codec      codec.Codec
	validators []Validator
	hooks      Hooks
	fieldTag   string       // struct tag for field mapping (default: "json")
	mapper     *FieldMapper // field mapper for struct conversion
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

// Bind returns a BoundConfig with struct binding capabilities.
func (b *Binder) Bind() BoundConfig {
	return &boundConfig{
		Config: b.cfg,
		binder: b,
	}
}

// Config returns the underlying Config interface.
func (b *Binder) Config() config.Config {
	return b.cfg
}

// BoundConfig extends Config with struct binding methods.
// This interface embeds config.Config so it can be used as a drop-in replacement.
type BoundConfig interface {
	config.Config // Embed original interface

	// GetStruct unmarshals the value at key into the target struct.
	// Validates the result if validators are configured.
	GetStruct(ctx context.Context, key string, target any) error

	// SetStruct marshals the struct and stores it at key.
	// Validates before storing if validators are configured.
	SetStruct(ctx context.Context, key string, value any, opts ...config.SetOption) error

	// MustGetStruct is like GetStruct but panics on error.
	MustGetStruct(ctx context.Context, key string, target any)

	// BindPrefix returns a struct populated from all keys with the given prefix.
	// Example: BindPrefix(ctx, "database", &dbConfig) binds database/* keys
	BindPrefix(ctx context.Context, prefix string, target any) error

	// Validate validates a value using the configured validators.
	Validate(value any) error
}

// boundConfig implements BoundConfig
type boundConfig struct {
	config.Config
	binder *Binder
}

// GetStruct unmarshals the value at key into the target struct.
func (bc *boundConfig) GetStruct(ctx context.Context, key string, target any) error {
	// Run before-get hook
	if bc.binder.hooks.BeforeGet != nil {
		if err := bc.binder.hooks.BeforeGet(ctx, key); err != nil {
			return err
		}
	}

	val, err := bc.Config.Get(ctx, key)
	if err != nil {
		return err
	}

	if err := val.Unmarshal(target); err != nil {
		return &BindError{
			Key: key,
			Op:  "unmarshal",
			Err: err,
		}
	}

	// Run validators
	for _, v := range bc.binder.validators {
		if err := v.Validate(target); err != nil {
			return &ValidationError{
				Key:   key,
				Value: target,
				Err:   err,
			}
		}
	}

	// Run after-unmarshal hook
	if bc.binder.hooks.AfterUnmarshal != nil {
		if err := bc.binder.hooks.AfterUnmarshal(ctx, key, target); err != nil {
			return err
		}
	}

	return nil
}

// SetStruct marshals the struct and stores it at key.
func (bc *boundConfig) SetStruct(ctx context.Context, key string, value any, opts ...config.SetOption) error {
	// Run before-set hook
	if bc.binder.hooks.BeforeSet != nil {
		if err := bc.binder.hooks.BeforeSet(ctx, key, value); err != nil {
			return err
		}
	}

	// Validate before setting
	for _, v := range bc.binder.validators {
		if err := v.Validate(value); err != nil {
			return &ValidationError{
				Key:   key,
				Value: value,
				Err:   err,
			}
		}
	}

	return bc.Config.Set(ctx, key, value, opts...)
}

// MustGetStruct is like GetStruct but panics on error.
func (bc *boundConfig) MustGetStruct(ctx context.Context, key string, target any) {
	if err := bc.GetStruct(ctx, key, target); err != nil {
		panic(fmt.Sprintf("config: failed to get struct for key %q: %v", key, err))
	}
}

// BindPrefix returns a struct populated from all keys with the given prefix.
func (bc *boundConfig) BindPrefix(ctx context.Context, prefix string, target any) error {
	page, err := bc.Config.Find(ctx, config.NewFilter().WithPrefix(prefix).Build())
	if err != nil {
		return err
	}

	entries := page.Results()
	if len(entries) == 0 {
		return nil
	}

	// Build a nested map from entries
	data := buildMapFromEntries(entries, prefix)

	// Use the field mapper to convert map to struct (respects custom tags and "-" ignore)
	if err := bc.binder.mapper.MapToStruct(data, target); err != nil {
		return &BindError{
			Key: prefix,
			Op:  "unmarshal",
			Err: err,
		}
	}

	// Validate
	for _, v := range bc.binder.validators {
		if err := v.Validate(target); err != nil {
			return &ValidationError{
				Key:   prefix,
				Value: target,
				Err:   err,
			}
		}
	}

	return nil
}

// Validate validates a value using the configured validators.
func (bc *boundConfig) Validate(value any) error {
	for _, v := range bc.binder.validators {
		if err := v.Validate(value); err != nil {
			return err
		}
	}
	return nil
}

// buildMapFromEntries builds a nested map from config entries
func buildMapFromEntries(entries map[string]config.Value, prefix string) map[string]any {
	result := make(map[string]any)

	for entryKey, val := range entries {
		// Remove prefix from key
		key := entryKey
		if prefix != "" {
			key = strings.TrimPrefix(key, prefix)
			key = strings.TrimPrefix(key, "/")
		}

		if key == "" {
			continue
		}

		// Parse the value
		var value any
		if err := val.Unmarshal(&value); err != nil {
			// Fallback: try to get raw bytes and unmarshal
			if data, err := val.Marshal(); err == nil {
				json.Unmarshal(data, &value)
			}
		}

		// Build nested structure
		parts := strings.Split(key, "/")
		current := result

		for i, part := range parts {
			if i == len(parts)-1 {
				// Leaf node
				current[part] = value
			} else {
				// Intermediate node
				if _, ok := current[part]; !ok {
					current[part] = make(map[string]any)
				}
				if next, ok := current[part].(map[string]any); ok {
					current = next
				} else {
					// Path conflict - overwrite
					newMap := make(map[string]any)
					current[part] = newMap
					current = newMap
				}
			}
		}
	}

	return result
}
