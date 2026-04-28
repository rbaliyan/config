package config

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
)

// CacheStats contains cache statistics.
type CacheStats struct {
	// Hits is the number of successful cache lookups.
	Hits int64 `json:"hits"`

	// Misses is the number of cache lookups that found no entry.
	Misses int64 `json:"misses"`

	// Size is the current number of entries in the cache.
	Size int64 `json:"size"`

	// Capacity is the maximum number of entries (0 = unbounded).
	Capacity int64 `json:"capacity"`

	// Evictions is the number of entries evicted due to capacity limits.
	Evictions int64 `json:"evictions"`
}

// HitRate returns the cache hit rate as a percentage (0.0 to 1.0).
// Returns 0 if there have been no lookups.
func (s *CacheStats) HitRate() float64 {
	total := s.Hits + s.Misses
	if total == 0 {
		return 0
	}
	return float64(s.Hits) / float64(total)
}

// Cache defines the interface for configuration value caching.
//
// The cache provides fast local access to configuration values and serves as
// a resilience layer. If the backend store becomes unavailable, the application
// can continue serving cached values.
//
// The cache is automatically invalidated via the store's Watch mechanism
// (e.g., MongoDB Change Streams, PostgreSQL LISTEN/NOTIFY).
//
// The default implementation is an in-process LRU (see newMemoryCache). For
// shared caching across multiple application instances, supply a distributed
// implementation (e.g. redis.NewCache) via config.WithCache.
type Cache interface {
	// Get retrieves a cached value.
	// Returns ErrNotFound if not in cache.
	Get(ctx context.Context, namespace, key string) (Value, error)

	// Set stores a value in the cache.
	Set(ctx context.Context, namespace, key string, value Value) error

	// Delete removes an entry from the cache.
	Delete(ctx context.Context, namespace, key string) error

	// Stats returns cache statistics.
	Stats() CacheStats
}

// defaultCacheCapacity is the default capacity when none is specified.
const defaultCacheCapacity = 10000

// cacheKeySeparator uses null byte to avoid collisions.
// Neither namespace nor key can contain null bytes (per validation rules),
// so "ns\x00key" is guaranteed unique for any (namespace, key) pair.
const cacheKeySeparator = "\x00"

// memoryCache is an in-memory LRU cache implementation using hashicorp/golang-lru/v2/expirable.
// It supports optional TTL-based expiry in addition to LRU eviction.
type memoryCache struct {
	lru      *expirable.LRU[string, Value]
	capacity int
	ttl      time.Duration

	// Statistics (atomic for lock-free reads)
	hits      atomic.Int64
	misses    atomic.Int64
	evictions atomic.Int64
}

// newMemoryCache creates a new in-memory LRU cache.
// If capacity is 0, it uses the default capacity of 10000.
// If ttl is 0, entries never expire (LRU eviction only).
func newMemoryCache(capacity int, ttl time.Duration) (Cache, error) {
	if capacity <= 0 {
		capacity = defaultCacheCapacity
	}

	c := &memoryCache{
		capacity: capacity,
		ttl:      ttl,
	}

	c.lru = expirable.NewLRU(capacity, func(key string, value Value) {
		c.evictions.Add(1)
	}, ttl)

	return c, nil
}

// Compile-time interface check
var _ Cache = (*memoryCache)(nil)

func cacheKey(namespace, key string) string {
	return namespace + cacheKeySeparator + key
}

// Get retrieves a cached value.
func (c *memoryCache) Get(ctx context.Context, namespace, key string) (Value, error) {
	value, ok := c.lru.Get(cacheKey(namespace, key))
	if !ok {
		c.misses.Add(1)
		return nil, ErrNotFound
	}

	c.hits.Add(1)
	return value, nil
}

// Set stores a value in the cache.
func (c *memoryCache) Set(ctx context.Context, namespace, key string, value Value) error {
	c.lru.Add(cacheKey(namespace, key), value)
	return nil
}

// Delete removes an entry from the cache.
func (c *memoryCache) Delete(ctx context.Context, namespace, key string) error {
	c.lru.Remove(cacheKey(namespace, key))
	return nil
}

// Stats returns cache statistics.
func (c *memoryCache) Stats() CacheStats {
	return CacheStats{
		Hits:      c.hits.Load(),
		Misses:    c.misses.Load(),
		Size:      int64(c.lru.Len()),
		Capacity:  int64(c.capacity),
		Evictions: c.evictions.Load(),
	}
}

