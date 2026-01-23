package config

import (
	"log/slog"
	"time"

	"github.com/rbaliyan/config/codec"
)

// WatchBackoffConfig configures the exponential backoff for watch reconnection.
// When the watch connection fails, the manager will retry with exponential backoff.
// This prevents hammering the backend while allowing quick recovery.
type WatchBackoffConfig struct {
	// InitialBackoff is the initial wait time between reconnection attempts.
	// Default: 100ms
	InitialBackoff time.Duration

	// MaxBackoff is the maximum wait time between reconnection attempts.
	// Default: 30 seconds
	MaxBackoff time.Duration

	// BackoffFactor is the multiplier applied to backoff after each failure.
	// Default: 2.0
	BackoffFactor float64
}

// DefaultWatchBackoffConfig returns the default watch backoff configuration.
func DefaultWatchBackoffConfig() WatchBackoffConfig {
	return WatchBackoffConfig{
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     30 * time.Second,
		BackoffFactor:  2.0,
	}
}

// managerOptions holds configuration for the Manager (unexported).
type managerOptions struct {
	store        Store
	codec        codec.Codec
	logger       *slog.Logger
	watchBackoff WatchBackoffConfig
}

// Option configures the Manager.
type Option func(*managerOptions)

// newManagerOptions creates options with defaults.
func newManagerOptions() *managerOptions {
	return &managerOptions{
		codec:        codec.Default(),
		logger:       slog.Default(),
		watchBackoff: DefaultWatchBackoffConfig(),
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

// WithWatchBackoff configures the exponential backoff for watch reconnection.
// When the watch connection fails, the manager retries with exponential backoff
// to prevent hammering the backend while allowing quick recovery.
//
// Example:
//
//	mgr := config.New(
//	    config.WithStore(store),
//	    config.WithWatchBackoff(config.WatchBackoffConfig{
//	        InitialBackoff: 200 * time.Millisecond,
//	        MaxBackoff:     1 * time.Minute,
//	        BackoffFactor:  1.5,
//	    }),
//	)
func WithWatchBackoff(cfg WatchBackoffConfig) Option {
	return func(o *managerOptions) {
		if cfg.InitialBackoff > 0 {
			o.watchBackoff.InitialBackoff = cfg.InitialBackoff
		}
		if cfg.MaxBackoff > 0 {
			o.watchBackoff.MaxBackoff = cfg.MaxBackoff
		}
		if cfg.BackoffFactor > 0 {
			o.watchBackoff.BackoffFactor = cfg.BackoffFactor
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
