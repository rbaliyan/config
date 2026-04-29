package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/rbaliyan/config"
	goredis "github.com/redis/go-redis/v9"
)

// CacheOption configures a Redis-backed Cache.
type CacheOption func(*cacheConfig)

type cacheConfig struct {
	prefix string
	ttl    time.Duration
}

// WithCacheKeyPrefix sets the Redis key prefix for cache entries.
// Default is "config:cache:".
func WithCacheKeyPrefix(prefix string) CacheOption {
	return func(o *cacheConfig) {
		if prefix != "" {
			o.prefix = prefix
		}
	}
}

// WithCacheTTL sets the TTL applied to each cache entry.
// 0 (the default) means entries never expire; Redis eviction policy manages
// memory pressure instead.
func WithCacheTTL(ttl time.Duration) CacheOption {
	return func(o *cacheConfig) {
		o.ttl = ttl
	}
}

// cachePayload is the JSON envelope stored in Redis for each cached value.
// It carries enough information to reconstruct a config.Value faithfully.
type cachePayload struct {
	Data      []byte     `json:"d"`
	Codec     string     `json:"c"`
	Type      int        `json:"t"`
	Version   int64      `json:"ver"`
	CreatedAt time.Time  `json:"ca"`
	UpdatedAt time.Time  `json:"ua"`
	ExpiresAt *time.Time `json:"exp,omitempty"` // nil = no TTL on the cached value
}

// redisCache is a Redis-backed config.Cache.
type redisCache struct {
	client goredis.UniversalClient
	cfg    cacheConfig

	hits   atomic.Int64
	misses atomic.Int64
}

// Compile-time interface check.
var _ config.Cache = (*redisCache)(nil)

// NewCache returns a Redis-backed config.Cache backed by client.
//
// Each cache entry is stored as a JSON-encoded payload under a key derived
// from the namespace and config key. Multiple Manager instances sharing the
// same Redis client and prefix will share cached values, enabling distributed
// cache coherence when combined with a store backend that has Watch support.
//
// Example:
//
//	rdb := goredis.NewClient(&goredis.Options{Addr: "localhost:6379"})
//	mgr, err := config.New(
//	    config.WithStore(pgStore),
//	    config.WithCache(redis.NewCache(rdb,
//	        redis.WithCacheTTL(5*time.Minute),
//	    )),
//	)
func NewCache(client goredis.UniversalClient, opts ...CacheOption) config.Cache {
	cfg := cacheConfig{prefix: "config:cache:"}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &redisCache{client: client, cfg: cfg}
}

// redisKey returns the Redis key for a given (namespace, key) pair.
// Uses the null-byte separator consistent with the in-memory cache.
func (c *redisCache) redisKey(namespace, key string) string {
	return c.cfg.prefix + namespace + "\x00" + key
}

// Get retrieves a cached value from Redis.
// Returns config.ErrNotFound on a cache miss or if the stored payload cannot
// be decoded — a decode failure is treated as a miss so the caller falls back
// to the backend store.
func (c *redisCache) Get(ctx context.Context, namespace, key string) (config.Value, error) {
	data, err := c.client.Get(ctx, c.redisKey(namespace, key)).Bytes()
	if err == goredis.Nil {
		c.misses.Add(1)
		return nil, config.ErrNotFound
	}
	if err != nil {
		c.misses.Add(1)
		return nil, fmt.Errorf("redis cache get: %w", err)
	}

	var p cachePayload
	if err := json.Unmarshal(data, &p); err != nil {
		c.misses.Add(1)
		return nil, config.ErrNotFound
	}

	var expiresAt time.Time
	if p.ExpiresAt != nil {
		// A cache entry past its TTL must not be returned — the caller would
		// otherwise see expired data after a successful cache hit.
		if time.Now().UTC().After(*p.ExpiresAt) {
			c.misses.Add(1)
			return nil, config.ErrNotFound
		}
		expiresAt = *p.ExpiresAt
	}

	v, err := config.NewValueFromBytes(ctx, p.Data, p.Codec,
		config.WithValueType(config.Type(p.Type)),
		config.WithValueMetadata(p.Version, p.CreatedAt, p.UpdatedAt),
		config.WithValueExpiresAt(expiresAt),
	)
	if err != nil {
		c.misses.Add(1)
		return nil, config.ErrNotFound
	}

	c.hits.Add(1)
	return v, nil
}

// Set stores a value in Redis. The value is serialized via its Marshal method
// and stored alongside type and metadata so it can be reconstructed exactly.
func (c *redisCache) Set(ctx context.Context, namespace, key string, value config.Value) error {
	data, err := value.Marshal(ctx)
	if err != nil {
		return fmt.Errorf("redis cache marshal: %w", err)
	}

	meta := value.Metadata()
	p := cachePayload{
		Data:      data,
		Codec:     value.Codec(),
		Type:      int(value.Type()),
		Version:   meta.Version(),
		CreatedAt: meta.CreatedAt(),
		UpdatedAt: meta.UpdatedAt(),
	}
	if expiry := meta.ExpiresAt(); !expiry.IsZero() {
		p.ExpiresAt = &expiry
	}

	encoded, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("redis cache encode: %w", err)
	}

	// If the value has its own TTL, propagate it as the Redis key TTL so the
	// cache key is evicted at the same instant the value expires. The
	// configured cache TTL is used as a ceiling.
	ttl := c.cfg.ttl
	if !meta.ExpiresAt().IsZero() {
		valueTTL := time.Until(meta.ExpiresAt())
		if valueTTL <= 0 {
			// Already expired — do not cache.
			return nil
		}
		if ttl == 0 || valueTTL < ttl {
			ttl = valueTTL
		}
	}

	return c.client.Set(ctx, c.redisKey(namespace, key), encoded, ttl).Err()
}

// Delete removes a cache entry from Redis.
func (c *redisCache) Delete(ctx context.Context, namespace, key string) error {
	return c.client.Del(ctx, c.redisKey(namespace, key)).Err()
}

// Stats returns per-process hit/miss counters. Size, Capacity, and Evictions
// are not tracked (0) — those are managed by Redis server-side.
func (c *redisCache) Stats() config.CacheStats {
	return config.CacheStats{
		Hits:   c.hits.Load(),
		Misses: c.misses.Load(),
	}
}
