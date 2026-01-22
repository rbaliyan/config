package config

import (
	"log/slog"
	"time"

	"github.com/rbaliyan/config/codec"
)

// managerOptions holds configuration for the Manager (unexported).
type managerOptions struct {
	store    Store
	codec    codec.Codec
	logger   *slog.Logger
	cacheTTL time.Duration
}

// Option configures the Manager.
type Option func(*managerOptions)

// newManagerOptions creates options with defaults.
func newManagerOptions() *managerOptions {
	return &managerOptions{
		codec:    codec.Default(),
		logger:   slog.Default(),
		cacheTTL: 5 * time.Minute,
	}
}

// WithStore sets the configuration store backend.
// This is required - the manager will fail to connect without a store.
func WithStore(store Store) Option {
	return func(o *managerOptions) {
		if store != nil {
			o.store = store
		}
	}
}

// WithCodec sets the default codec for encoding/decoding values.
// Default is JSON if not specified.
func WithCodec(c codec.Codec) Option {
	return func(o *managerOptions) {
		if c != nil {
			o.codec = c
		}
	}
}

// WithLogger sets a custom logger.
func WithLogger(logger *slog.Logger) Option {
	return func(o *managerOptions) {
		if logger != nil {
			o.logger = logger
		}
	}
}

// WithCacheTTL sets the time-to-live for cached configuration values.
// Default is 5 minutes. The cache is used for resilience - if the backend
// store becomes unavailable, cached values can still be served.
//
// Note: The cache is automatically invalidated via the store's Watch mechanism
// (e.g., MongoDB Change Streams, PostgreSQL LISTEN/NOTIFY), so in normal
// operation, values are refreshed much sooner than the TTL.
func WithCacheTTL(ttl time.Duration) Option {
	return func(o *managerOptions) {
		if ttl > 0 {
			o.cacheTTL = ttl
		}
	}
}

// SetOption configures Set operations.
type SetOption func(*setOptions)

type setOptions struct {
	tags  []Tag
	codec codec.Codec
	typ   Type
}

func newSetOptions() *setOptions {
	return &setOptions{}
}

// WithTags sets tags for the configuration entry.
// Tags are key-value pairs used for categorization and filtering.
// The combination of (namespace, key, tags) uniquely identifies an entry.
func WithTags(tags ...Tag) SetOption {
	return func(o *setOptions) {
		o.tags = SortTags(tags)
	}
}

// WithSetCodec sets the codec for encoding the value.
func WithSetCodec(c codec.Codec) SetOption {
	return func(o *setOptions) {
		if c != nil {
			o.codec = c
		}
	}
}

// WithType explicitly sets the value type.
func WithType(t Type) SetOption {
	return func(o *setOptions) {
		o.typ = t
	}
}
