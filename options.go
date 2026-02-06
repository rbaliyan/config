package config

import (
	"log/slog"
	"time"

	"github.com/rbaliyan/config/codec"
)

// watchBackoffConfig configures the exponential backoff for watch reconnection.
type watchBackoffConfig struct {
	initialBackoff time.Duration
	maxBackoff     time.Duration
	backoffFactor  float64
}

// managerOptions holds configuration for the Manager (unexported).
type managerOptions struct {
	store        Store
	codec        codec.Codec
	logger       *slog.Logger
	watchBackoff watchBackoffConfig
}

// Option configures the Manager.
type Option func(*managerOptions)

// newManagerOptions creates options with defaults.
func newManagerOptions() *managerOptions {
	return &managerOptions{
		codec:  codec.Default(),
		logger: slog.Default(),
		watchBackoff: watchBackoffConfig{
			initialBackoff: 100 * time.Millisecond,
			maxBackoff:     30 * time.Second,
			backoffFactor:  2.0,
		},
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

// WithWatchInitialBackoff sets the initial wait time between watch reconnection attempts.
// Default: 100ms.
func WithWatchInitialBackoff(d time.Duration) Option {
	return func(o *managerOptions) {
		if d > 0 {
			o.watchBackoff.initialBackoff = d
		}
	}
}

// WithWatchMaxBackoff sets the maximum wait time between watch reconnection attempts.
// Default: 30s.
func WithWatchMaxBackoff(d time.Duration) Option {
	return func(o *managerOptions) {
		if d > 0 {
			o.watchBackoff.maxBackoff = d
		}
	}
}

// WithWatchBackoffFactor sets the multiplier applied to backoff after each watch failure.
// Default: 2.0.
func WithWatchBackoffFactor(f float64) Option {
	return func(o *managerOptions) {
		if f > 0 {
			o.watchBackoff.backoffFactor = f
		}
	}
}

// SetOption configures Set operations.
type SetOption func(*setOptions)

type setOptions struct {
	codec     codec.Codec
	typ       Type
	writeMode WriteMode
}

func newSetOptions() *setOptions {
	return &setOptions{}
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

// WithIfNotExists configures Set to only create the key if it doesn't exist.
// Returns ErrKeyExists if the key already exists.
//
// This is useful for implementing "create-only" semantics where you want to
// ensure you don't accidentally overwrite an existing value.
//
// Example:
//
//	err := cfg.Set(ctx, "lock/owner", "instance-1", config.WithIfNotExists())
//	if config.IsKeyExists(err) {
//	    // Key was already taken by another instance
//	}
func WithIfNotExists() SetOption {
	return func(o *setOptions) {
		o.writeMode = WriteModeCreate
	}
}

// WithIfExists configures Set to only update the key if it already exists.
// Returns ErrNotFound if the key doesn't exist.
//
// This is useful for implementing "update-only" semantics where you want to
// ensure the key was previously created.
//
// Example:
//
//	err := cfg.Set(ctx, "app/timeout", 60, config.WithIfExists())
//	if config.IsNotFound(err) {
//	    // Key doesn't exist, need to create it first
//	}
func WithIfExists() SetOption {
	return func(o *setOptions) {
		o.writeMode = WriteModeUpdate
	}
}
