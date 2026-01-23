package bind

import (
	"github.com/rbaliyan/config/codec"
)

// Option configures the Binder.
type Option func(*Binder)

// WithCodec sets the codec for encoding/decoding values.
func WithCodec(c codec.Codec) Option {
	return func(b *Binder) {
		b.codec = c
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
		b.validator = NewTagValidator("validate")
	}
}

// WithCustomTagValidation enables struct tag validation with a custom tag name.
func WithCustomTagValidation(tagName string) Option {
	return func(b *Binder) {
		b.validator = NewTagValidator(tagName)
	}
}

// WithFieldTag sets the struct tag name used for field mapping.
// Default is "json". Common alternatives: "yaml", "config", "mapstructure".
//
// Supported tag options:
//   - "-": Field is ignored during both marshalling and unmarshalling
//   - "omitempty": Field is omitted if it has a zero value
//   - "nonrecursive": Nested struct is stored as a single value (not flattened)
//
// Example:
//
//	binder := bind.New(cfg, bind.WithFieldTag("config"))
//
//	type DatabaseConfig struct {
//	    Host     string      `config:"host"`
//	    Port     int         `config:"port"`
//	    Password string      `config:"-"`            // ignored
//	    Creds    Credentials `config:"creds,nonrecursive"` // stored as single JSON
//	}
func WithFieldTag(tagName string) Option {
	return func(b *Binder) {
		if tagName != "" {
			b.fieldTag = tagName
		}
	}
}
