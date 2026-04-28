package redis_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/rbaliyan/config"
	configredis "github.com/rbaliyan/config/redis"
	goredis "github.com/redis/go-redis/v9"
)

// redisAddr returns the Redis address for tests, defaulting to localhost.
func redisAddr() string {
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		return addr
	}
	return "localhost:6379"
}

// newTestClient returns a Redis client and skips the test if Redis is unavailable.
func newTestClient(t *testing.T) goredis.UniversalClient {
	t.Helper()
	rdb := goredis.NewClient(&goredis.Options{Addr: redisAddr()})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("Redis unavailable at %s: %v", redisAddr(), err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// uniquePrefix returns a test-scoped key prefix to avoid cross-test pollution.
func uniquePrefix(t *testing.T) string {
	return "test:cache:" + t.Name() + ":"
}

func TestRedisCache_GetSetDelete(t *testing.T) {
	rdb := newTestClient(t)
	c := configredis.NewCache(rdb, configredis.WithCacheKeyPrefix(uniquePrefix(t)))

	ctx := context.Background()
	v := config.NewValue("hello", config.WithValueType(config.TypeString))

	// Miss before set.
	if _, err := c.Get(ctx, "ns", "key"); !config.IsNotFound(err) {
		t.Errorf("Get before Set: want ErrNotFound, got %v", err)
	}

	// Set then Get.
	if err := c.Set(ctx, "ns", "key", v); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, err := c.Get(ctx, "ns", "key")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	s, _ := got.String()
	if s != "hello" {
		t.Errorf("Get: got %q, want hello", s)
	}

	// Delete then miss again.
	if err := c.Delete(ctx, "ns", "key"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := c.Get(ctx, "ns", "key"); !config.IsNotFound(err) {
		t.Errorf("Get after Delete: want ErrNotFound, got %v", err)
	}
}

func TestRedisCache_TypePreserved(t *testing.T) {
	rdb := newTestClient(t)
	c := configredis.NewCache(rdb, configredis.WithCacheKeyPrefix(uniquePrefix(t)))
	ctx := context.Background()

	v := config.NewValue(int64(42), config.WithValueType(config.TypeInt))
	if err := c.Set(ctx, "ns", "count", v); err != nil {
		t.Fatal(err)
	}

	got, err := c.Get(ctx, "ns", "count")
	if err != nil {
		t.Fatal(err)
	}
	if got.Type() != config.TypeInt {
		t.Errorf("Type: got %v, want TypeInt", got.Type())
	}
	n, err := got.Int64()
	if err != nil {
		t.Fatalf("Int64: %v", err)
	}
	if n != 42 {
		t.Errorf("Int64: got %d, want 42", n)
	}
}

func TestRedisCache_MetadataPreserved(t *testing.T) {
	rdb := newTestClient(t)
	c := configredis.NewCache(rdb, configredis.WithCacheKeyPrefix(uniquePrefix(t)))
	ctx := context.Background()

	now := time.Now().Truncate(time.Millisecond)
	v := config.NewValue("v",
		config.WithValueType(config.TypeString),
		config.WithValueMetadata(7, now, now),
	)
	if err := c.Set(ctx, "ns", "k", v); err != nil {
		t.Fatal(err)
	}

	got, err := c.Get(ctx, "ns", "k")
	if err != nil {
		t.Fatal(err)
	}
	meta := got.Metadata()
	if meta.Version() != 7 {
		t.Errorf("Version: got %d, want 7", meta.Version())
	}
	if !meta.CreatedAt().Equal(now) {
		t.Errorf("CreatedAt: got %v, want %v", meta.CreatedAt(), now)
	}
}

func TestRedisCache_TTLExpiry(t *testing.T) {
	rdb := newTestClient(t)
	c := configredis.NewCache(rdb,
		configredis.WithCacheKeyPrefix(uniquePrefix(t)),
		configredis.WithCacheTTL(100*time.Millisecond),
	)
	ctx := context.Background()

	v := config.NewValue("ttl-val", config.WithValueType(config.TypeString))
	if err := c.Set(ctx, "ns", "ttl-key", v); err != nil {
		t.Fatal(err)
	}

	// Immediately readable.
	if _, err := c.Get(ctx, "ns", "ttl-key"); err != nil {
		t.Fatalf("Get before expiry: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Should be expired.
	if _, err := c.Get(ctx, "ns", "ttl-key"); !config.IsNotFound(err) {
		t.Errorf("Get after TTL: want ErrNotFound, got %v", err)
	}
}

func TestRedisCache_NamespaceIsolation(t *testing.T) {
	rdb := newTestClient(t)
	c := configredis.NewCache(rdb, configredis.WithCacheKeyPrefix(uniquePrefix(t)))
	ctx := context.Background()

	va := config.NewValue("a", config.WithValueType(config.TypeString))
	vb := config.NewValue("b", config.WithValueType(config.TypeString))

	_ = c.Set(ctx, "ns1", "key", va)
	_ = c.Set(ctx, "ns2", "key", vb)

	got1, _ := c.Get(ctx, "ns1", "key")
	got2, _ := c.Get(ctx, "ns2", "key")

	s1, _ := got1.String()
	s2, _ := got2.String()

	if s1 != "a" || s2 != "b" {
		t.Errorf("namespace isolation: ns1=%q, ns2=%q; want a, b", s1, s2)
	}
}

func TestRedisCache_Stats(t *testing.T) {
	rdb := newTestClient(t)
	c := configredis.NewCache(rdb, configredis.WithCacheKeyPrefix(uniquePrefix(t)))
	ctx := context.Background()

	v := config.NewValue("x", config.WithValueType(config.TypeString))
	_ = c.Set(ctx, "ns", "k", v)

	_, _ = c.Get(ctx, "ns", "k")        // hit
	_, _ = c.Get(ctx, "ns", "k")        // hit
	_, _ = c.Get(ctx, "ns", "missing")  // miss

	stats := c.Stats()
	if stats.Hits != 2 {
		t.Errorf("Hits: got %d, want 2", stats.Hits)
	}
	if stats.Misses != 1 {
		t.Errorf("Misses: got %d, want 1", stats.Misses)
	}
}

func TestRedisCache_ImplementsInterface(t *testing.T) {
	rdb := goredis.NewClient(&goredis.Options{Addr: "localhost:6379"})
	defer rdb.Close()
	var _ config.Cache = configredis.NewCache(rdb)
}
