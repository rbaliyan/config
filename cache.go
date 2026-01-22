package config

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// cache defines the internal interface for local configuration caching.
//
// The cache provides fast local access to configuration values and serves as
// a resilience layer. If the backend store becomes unavailable, the application
// can continue serving cached values.
//
// The cache is automatically invalidated via the store's Watch mechanism
// (e.g., MongoDB Change Streams, PostgreSQL LISTEN/NOTIFY). This provides
// eventual consistency without external dependencies like Redis.
//
// Note: The cache is NOT meant for sharing state across application instances.
// Each instance maintains its own local cache that is independently synchronized
// with the backend store.
type cache interface {
	// Get retrieves a cached value.
	// Returns ErrNotFound if not in cache.
	Get(ctx context.Context, namespace, key string) (Value, error)

	// Set stores a value in the cache.
	Set(ctx context.Context, namespace, key string, value Value) error

	// Delete removes an entry from the cache.
	Delete(ctx context.Context, namespace, key string) error

	// Flush removes all entries from the cache.
	Flush(ctx context.Context) error
}

// cacheStats provides cache statistics (internal use).
type cacheStats interface {
	// Hits returns the number of cache hits.
	Hits() int64

	// Misses returns the number of cache misses.
	Misses() int64

	// Size returns the current number of cached entries.
	Size() int64
}

// memoryCache is an in-memory cache implementation.
type memoryCache struct {
	mu       sync.RWMutex
	entries  map[string]*cacheEntry // key format: "namespace:key"
	ttl      time.Duration
	hits     atomic.Int64
	misses   atomic.Int64
	stopChan chan struct{}
}

type cacheEntry struct {
	value     Value
	expiresAt time.Time
}

// MemoryCacheOption configures the memory cache.
type MemoryCacheOption func(*memoryCacheOptions)

type memoryCacheOptions struct {
	ttl             time.Duration
	cleanupInterval time.Duration
}

func newMemoryCacheOptions() *memoryCacheOptions {
	return &memoryCacheOptions{
		ttl:             5 * time.Minute,
		cleanupInterval: time.Minute,
	}
}

// WithMemoryCacheTTL sets the cache entry TTL for the memory cache.
func WithMemoryCacheTTL(ttl time.Duration) MemoryCacheOption {
	return func(o *memoryCacheOptions) {
		if ttl > 0 {
			o.ttl = ttl
		}
	}
}

// WithCacheCleanupInterval sets the interval for cleaning up expired entries.
func WithCacheCleanupInterval(interval time.Duration) MemoryCacheOption {
	return func(o *memoryCacheOptions) {
		if interval > 0 {
			o.cleanupInterval = interval
		}
	}
}

// NewMemoryCache creates a new in-memory cache.
func NewMemoryCache(opts ...MemoryCacheOption) *memoryCache {
	options := newMemoryCacheOptions()
	for _, opt := range opts {
		opt(options)
	}

	c := &memoryCache{
		entries:  make(map[string]*cacheEntry),
		ttl:      options.ttl,
		stopChan: make(chan struct{}),
	}

	// Start cleanup goroutine
	if options.ttl > 0 && options.cleanupInterval > 0 {
		go c.cleanupLoop(options.cleanupInterval)
	}

	return c
}

// Compile-time interface checks
var (
	_ cache      = (*memoryCache)(nil)
	_ cacheStats = (*memoryCache)(nil)
)

func (c *memoryCache) cacheKey(namespace, key string) string {
	return namespace + ":" + key
}

// Get retrieves a cached value.
func (c *memoryCache) Get(ctx context.Context, namespace, key string) (Value, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, ok := c.entries[c.cacheKey(namespace, key)]
	if !ok {
		c.misses.Add(1)
		return nil, ErrNotFound
	}

	// Check expiration
	if c.ttl > 0 && time.Now().After(entry.expiresAt) {
		c.misses.Add(1)
		return nil, ErrNotFound
	}

	c.hits.Add(1)
	return entry.value, nil
}

// Set stores a value in the cache.
func (c *memoryCache) Set(ctx context.Context, namespace, key string, value Value) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries[c.cacheKey(namespace, key)] = &cacheEntry{
		value:     value,
		expiresAt: time.Now().Add(c.ttl),
	}
	return nil
}

// Delete removes an entry from the cache.
func (c *memoryCache) Delete(ctx context.Context, namespace, key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.entries, c.cacheKey(namespace, key))
	return nil
}

// Flush removes all entries from the cache.
func (c *memoryCache) Flush(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries = make(map[string]*cacheEntry)
	return nil
}

// Hits returns the number of cache hits.
func (c *memoryCache) Hits() int64 {
	return c.hits.Load()
}

// Misses returns the number of cache misses.
func (c *memoryCache) Misses() int64 {
	return c.misses.Load()
}

// Size returns the current number of cached entries.
func (c *memoryCache) Size() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return int64(len(c.entries))
}

// Close stops the cleanup goroutine.
func (c *memoryCache) Close() error {
	close(c.stopChan)
	return nil
}

// cleanupLoop removes expired entries periodically.
func (c *memoryCache) cleanupLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopChan:
			return
		case <-ticker.C:
			c.cleanup()
		}
	}
}

// cleanup removes expired entries.
func (c *memoryCache) cleanup() {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()

	for key, entry := range c.entries {
		if c.ttl > 0 && now.After(entry.expiresAt) {
			delete(c.entries, key)
		}
	}
}
