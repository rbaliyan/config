package bind

import (
	"context"

	"github.com/rbaliyan/config/codec"
)

// Option configures the Binder.
type Option func(*Binder)

// Hooks allow custom logic at different points in the binding process.
type Hooks struct {
	// BeforeGet is called before getting a value.
	BeforeGet func(ctx context.Context, key string) error

	// BeforeSet is called before setting a value.
	BeforeSet func(ctx context.Context, key string, value any) error

	// AfterUnmarshal is called after unmarshaling a value.
	AfterUnmarshal func(ctx context.Context, key string, value any) error
}

// WithCodec sets the codec for encoding/decoding values.
func WithCodec(c codec.Codec) Option {
	return func(b *Binder) {
		b.codec = c
	}
}

// WithValidator adds a validator to the binder.
// Multiple validators can be added and will be run in order.
func WithValidator(v Validator) Option {
	return func(b *Binder) {
		b.validators = append(b.validators, v)
	}
}

// WithTagValidation enables struct tag validation.
// Tags are specified using the "validate" tag name.
//
// Supported tags:
//   - required: field must not be zero value
//   - min=N: minimum value (for numbers) or length (for strings/slices)
//   - max=N: maximum value (for numbers) or length (for strings/slices)
//   - enum=a|b|c: value must be one of the listed options
//   - pattern=regex: value must match the regex pattern
//
// Example:
//
//	type Config struct {
//	    Host string `validate:"required"`
//	    Port int    `validate:"required,min=1,max=65535"`
//	}
func WithTagValidation() Option {
	return func(b *Binder) {
		b.validators = append(b.validators, NewTagValidator("validate"))
	}
}

// WithCustomTagValidation enables struct tag validation with a custom tag name.
func WithCustomTagValidation(tagName string) Option {
	return func(b *Binder) {
		b.validators = append(b.validators, NewTagValidator(tagName))
	}
}

// WithJSONSchema adds JSON Schema validation.
// The schema should be valid JSON Schema bytes.
func WithJSONSchema(schema []byte) Option {
	return func(b *Binder) {
		v, err := NewJSONSchemaValidator(schema)
		if err == nil {
			b.validators = append(b.validators, v)
		}
	}
}

// WithHooks sets validation hooks.
func WithHooks(h Hooks) Option {
	return func(b *Binder) {
		b.hooks = h
	}
}

// WithValidatorFunc adds a custom validation function.
func WithValidatorFunc(fn func(any) error) Option {
	return func(b *Binder) {
		b.validators = append(b.validators, ValidatorFunc(fn))
	}
}

// WithFieldTag sets the struct tag name used for field mapping.
// Default is "json". Common alternatives: "yaml", "config", "mapstructure".
//
// Fields tagged with `tagname:"-"` will be ignored during both
// marshalling (SetStruct) and unmarshalling (GetStruct, BindPrefix).
//
// Example:
//
//	binder := bind.New(cfg, bind.WithFieldTag("config"))
//
//	type DatabaseConfig struct {
//	    Host     string `config:"host"`
//	    Port     int    `config:"port"`
//	    Password string `config:"-"` // ignored - won't be stored or loaded
//	}
func WithFieldTag(tagName string) Option {
	return func(b *Binder) {
		if tagName != "" {
			b.fieldTag = tagName
		}
	}
}
