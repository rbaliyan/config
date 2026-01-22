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
	// Get retrieves a configuration value by key and optional tags.
	// The combination of (namespace, key, tags) uniquely identifies an entry.
	//
	// Get uses an internal cache for resilience. If the store is unavailable,
	// it will return a cached value if one exists. This is intentional - for
	// use cases like feature flags and rate limits, having slightly stale
	// configuration is better than failing completely.
	Get(ctx context.Context, key string, tags ...Tag) (Value, error)

	// Find returns a page of keys and values matching the filter.
	// Use Page.NextCursor() to paginate through results.
	Find(ctx context.Context, filter Filter) (Page, error)
}

// Writer provides write access to configuration.
// Use this interface for gRPC/HTTP servers to manage config.
type Writer interface {
	// Set creates or updates a configuration value.
	// Use WithTags option to specify tags for the entry.
	Set(ctx context.Context, key string, value any, opts ...SetOption) error

	// Delete removes a configuration value by key and optional tags.
	// The combination of (namespace, key, tags) uniquely identifies an entry.
	Delete(ctx context.Context, key string, tags ...Tag) error
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

// Get retrieves a configuration value by key and optional tags.
// It first attempts to fetch from the store. If the store is unavailable,
// it falls back to cached values for resilience.
func (c *config) Get(ctx context.Context, key string, tags ...Tag) (Value, error) {
	if !c.manager.isConnected() {
		return nil, ErrManagerClosed
	}

	// Validate key
	if err := ValidateKey(key); err != nil {
		return nil, err
	}

	// Build cache key (without namespace - cache methods take it separately)
	ck := cacheKey(key, tags)

	// Try to fetch from store first
	value, err := c.manager.store.Get(ctx, c.namespace, key, tags...)
	if err == nil {
		// Update cache with fresh value
		if cacheErr := c.manager.cache.Set(ctx, c.namespace, ck, value); cacheErr != nil {
			c.manager.logger.Warn("failed to cache value", "key", key, "error", cacheErr)
		}
		return value, nil
	}

	// If store error is NOT "not found", try cache as fallback for resilience
	if !IsNotFound(err) {
		if cachedValue, cacheErr := c.manager.cache.Get(ctx, c.namespace, ck); cacheErr == nil {
			c.manager.logger.Warn("serving cached value due to store error",
				"namespace", c.namespace, "key", key, "store_error", err)
			return cachedValue, nil
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

	// Create Value with tags
	val := NewValue(value,
		WithValueCodec(codecToUse),
		WithValueType(typ),
		WithValueTags(setOpts.tags),
	)

	// Set in store
	if err := c.manager.store.Set(ctx, c.namespace, key, val); err != nil {
		return err
	}

	// Build cache key (without namespace - cache methods take it separately)
	ck := cacheKey(key, setOpts.tags)

	// Update cache with new value from store (to get updated metadata)
	// Re-fetch to get the value with updated metadata (version, timestamps)
	if newValue, err := c.manager.store.Get(ctx, c.namespace, key, setOpts.tags...); err == nil {
		if cacheErr := c.manager.cache.Set(ctx, c.namespace, ck, newValue); cacheErr != nil {
			c.manager.logger.Warn("failed to cache value", "key", key, "error", cacheErr)
		}
	}

	return nil
}

// Delete removes a configuration value.
func (c *config) Delete(ctx context.Context, key string, tags ...Tag) error {
	if !c.manager.isConnected() {
		return ErrManagerClosed
	}

	// Validate key
	if err := ValidateKey(key); err != nil {
		return err
	}

	if err := c.manager.store.Delete(ctx, c.namespace, key, tags...); err != nil {
		return err
	}

	// Build cache key (without namespace - cache methods take it separately)
	ck := cacheKey(key, tags)

	// Remove from cache
	if cacheErr := c.manager.cache.Delete(ctx, c.namespace, ck); cacheErr != nil {
		c.manager.logger.Warn("failed to remove from cache", "key", key, "error", cacheErr)
	}

	return nil
}
