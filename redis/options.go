// Package redis provides a config.Store implementation backed by Redis.
//
// Each config namespace is stored as a single Redis hash keyed by the
// configured key prefix (default: "cfg"), so a namespace "production" lives
// in the hash "cfg:production". Individual config keys become hash fields;
// values are JSON-encoded entry payloads (value bytes, codec, type,
// version, timestamps, entry ID).
//
// Change notifications use a single pub/sub channel per store, named
// "{keyPrefix}:changes" (default: "cfg:changes"). Set and Delete are
// published atomically with the hash mutation via Lua scripts, so watchers
// never observe a write they cannot read back.
//
// Conditional writes (config.WithIfNotExists / config.WithIfExists) are
// mapped to HSETNX and a get-then-HSET respectively.
//
// Cluster deployments are supported via WithCluster; single-node
// deployments use WithAddress (the default targets localhost:6379).
// WithKeyPrefix lets multiple independent config stores coexist in one
// Redis database (e.g. "cfg:prod", "cfg:stage").
//
// Usage:
//
//	store := redis.NewStore(
//	    redis.WithAddress("redis.internal:6379"),
//	    redis.WithKeyPrefix("cfg"),
//	)
//	if err := store.Connect(ctx); err != nil { return err }
//	defer store.Close(ctx)
//
//	mgr, _ := config.New(config.WithStore(store))
package redis

import (
	"crypto/tls"
	"time"
)

type storeOptions struct {
	addr         string
	password     string
	db           int
	keyPrefix    string
	watchBufSize int
	dialTimeout  time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration
	tlsConfig    *tls.Config
	clusterAddrs []string
}

func defaultOptions() storeOptions {
	return storeOptions{
		addr:         "localhost:6379",
		keyPrefix:    "cfg",
		watchBufSize: 100,
	}
}

// Option configures a Redis-backed Store.
type Option func(*storeOptions)

// WithAddress sets the Redis server address (host:port) for single-node
// deployments. Ignored when WithCluster is used. Default: "localhost:6379".
func WithAddress(addr string) Option {
	return func(o *storeOptions) {
		if addr != "" {
			o.addr = addr
		}
	}
}

// WithPassword sets the AUTH password used when dialing Redis.
func WithPassword(password string) Option {
	return func(o *storeOptions) {
		o.password = password
	}
}

// WithDB selects the Redis logical database index. Ignored when
// WithCluster is used (Redis Cluster does not support logical databases).
// Default: 0.
func WithDB(db int) Option {
	return func(o *storeOptions) {
		o.db = db
	}
}

// WithKeyPrefix sets the Redis key prefix used for namespace hashes and
// the pub/sub channel. The namespace "N" is stored in the hash
// "{prefix}:N", and change events are published on "{prefix}:changes".
// Use distinct prefixes to run multiple independent stores in one Redis
// database. Default: "cfg".
func WithKeyPrefix(prefix string) Option {
	return func(o *storeOptions) {
		if prefix != "" {
			o.keyPrefix = prefix
		}
	}
}

// WithTLS enables TLS when dialing Redis with the provided configuration.
// Pass a *tls.Config with the CA roots and client certificate the
// deployment requires.
func WithTLS(cfg *tls.Config) Option {
	return func(o *storeOptions) {
		o.tlsConfig = cfg
	}
}

// WithCluster dials a Redis Cluster using the provided seed node
// addresses (host:port). When set, WithAddress and WithDB are ignored.
func WithCluster(addrs ...string) Option {
	return func(o *storeOptions) {
		o.clusterAddrs = addrs
	}
}

// WithWatchBufferSize sets the per-watcher channel buffer for change
// events. When the buffer is full, events are dropped rather than
// blocking the pub/sub relay. Default: 100.
func WithWatchBufferSize(n int) Option {
	return func(o *storeOptions) {
		if n > 0 {
			o.watchBufSize = n
		}
	}
}

// WithDialTimeout sets the timeout for establishing new Redis
// connections. Zero means use the go-redis default.
func WithDialTimeout(d time.Duration) Option {
	return func(o *storeOptions) {
		if d > 0 {
			o.dialTimeout = d
		}
	}
}

// WithReadTimeout sets the socket read timeout for Redis commands.
// Zero means use the go-redis default.
func WithReadTimeout(d time.Duration) Option {
	return func(o *storeOptions) {
		if d > 0 {
			o.readTimeout = d
		}
	}
}

// WithWriteTimeout sets the socket write timeout for Redis commands.
// Zero means use the go-redis default.
func WithWriteTimeout(d time.Duration) Option {
	return func(o *storeOptions) {
		if d > 0 {
			o.writeTimeout = d
		}
	}
}
