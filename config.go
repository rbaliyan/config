package config

import (
	"context"

	"github.com/rbaliyan/config/codec"
)

// Reader provides read-only access to configuration.
// Use this interface in application code to read config values.
//
// The Reader automatically uses an internal cache for resilience. If the backend
// store is temporarily unavailable, cached values will be returned. This ensures
// your application continues working even during database outages.
type Reader interface {
	// Get retrieves a configuration value by key.
	//
	// Get uses an internal cache for resilience. If the store is unavailable,
	// it will return a cached value if one exists. This is intentional - for
	// use cases like feature flags and rate limits, having slightly stale
	// configuration is better than failing completely.
	Get(ctx context.Context, key string) (Value, error)

	// Find returns a page of keys and values matching the filter.
	// Use Page.NextCursor() to paginate through results.
	Find(ctx context.Context, filter Filter) (Page, error)
}

// Writer provides write access to configuration.
// Use this interface for gRPC/HTTP servers to manage config.
type Writer interface {
	// Set creates or updates a configuration value.
	Set(ctx context.Context, key string, value any, opts ...SetOption) error

	// Delete removes a configuration value by key.
	Delete(ctx context.Context, key string) error
}

// Config combines Reader and Writer for a specific namespace.
type Config interface {
	Reader
	Writer

	// Namespace returns the namespace name.
	Namespace() string
}

// config is the default Config implementation.
type config struct {
	namespace string
	manager   *manager
}

// Compile-time interface check
var _ Config = (*config)(nil)

// Namespace returns the namespace name.
func (c *config) Namespace() string {
	return c.namespace
}

// Get retrieves a configuration value by key.
// It first attempts to fetch from the store. If the store is unavailable,
// it falls back to cached values for resilience.
func (c *config) Get(ctx context.Context, key string) (Value, error) {
	if !c.manager.isConnected() {
		return nil, ErrManagerClosed
	}

	// Validate key
	if err := ValidateKey(key); err != nil {
		return nil, err
	}

	// Try to fetch from store first
	value, err := c.manager.store.Get(ctx, c.namespace, key)
	if err == nil {
		// Update cache with fresh value
		if cacheErr := c.manager.cache.Set(ctx, c.namespace, key, value); cacheErr != nil {
			c.manager.logger.Warn("failed to cache value", "key", key, "error", cacheErr)
		}
		return value, nil
	}

	// If store error is NOT "not found", try cache as fallback for resilience
	if !IsNotFound(err) {
		if cachedValue, cacheErr := c.manager.cache.Get(ctx, c.namespace, key); cacheErr == nil {
			c.manager.logger.Warn("serving stale cached value due to store error",
				"namespace", c.namespace, "key", key, "store_error", err)
			// Mark the value as stale so callers know it came from cache due to error
			return MarkStale(cachedValue), nil
		}
	}

	// Return original store error (either NotFound or connection error with no cache)
	return nil, err
}

// Find returns a page of keys and values matching the filter.
func (c *config) Find(ctx context.Context, filter Filter) (Page, error) {
	if !c.manager.isConnected() {
		return nil, ErrManagerClosed
	}

	return c.manager.store.Find(ctx, c.namespace, filter)
}

// Set creates or updates a configuration value.
func (c *config) Set(ctx context.Context, key string, value any, opts ...SetOption) error {
	if !c.manager.isConnected() {
		return ErrManagerClosed
	}

	// Validate key
	if err := ValidateKey(key); err != nil {
		return err
	}

	// Apply options
	setOpts := newSetOptions()
	for _, opt := range opts {
		opt(setOpts)
	}

	// Determine codec
	codecToUse := c.manager.codec
	if setOpts.codec != nil {
		codecToUse = setOpts.codec
	}
	if codecToUse == nil {
		codecToUse = codec.Default()
	}

	// Determine type
	typ := setOpts.typ
	if typ == TypeUnknown {
		typ = detectType(value)
	}

	// Create Value with write mode
	val := NewValue(value,
		WithValueCodec(codecToUse),
		WithValueType(typ),
		WithValueWriteMode(setOpts.writeMode),
	)

	// Set in store - returns the stored value with updated metadata
	newValue, err := c.manager.store.Set(ctx, c.namespace, key, val)
	if err != nil {
		return err
	}

	// Update cache with the returned value
	if newValue != nil {
		if cacheErr := c.manager.cache.Set(ctx, c.namespace, key, newValue); cacheErr != nil {
			c.manager.logger.Warn("failed to cache value", "key", key, "error", cacheErr)
		}
	}

	return nil
}

// Delete removes a configuration value by key.
func (c *config) Delete(ctx context.Context, key string) error {
	if !c.manager.isConnected() {
		return ErrManagerClosed
	}

	// Validate key
	if err := ValidateKey(key); err != nil {
		return err
	}

	if err := c.manager.store.Delete(ctx, c.namespace, key); err != nil {
		return err
	}

	// Remove from cache
	if cacheErr := c.manager.cache.Delete(ctx, c.namespace, key); cacheErr != nil {
		c.manager.logger.Warn("failed to remove from cache", "key", key, "error", cacheErr)
	}

	return nil
}
