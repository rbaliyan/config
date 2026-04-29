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
	store              Store
	codec              codec.Codec
	logger             *slog.Logger
	watchBackoff       watchBackoffConfig
	maxKeysPerNS       int // 0 = unlimited
	aliases            map[string]string // alias key → target key
	cache              Cache             // nil = use default in-memory LRU
	memoryCacheTTL     time.Duration     // 0 = no expiry (LRU eviction only)
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

// WithMaxKeysPerNamespace sets the maximum number of keys allowed per namespace.
// Set operations that would exceed this limit return ErrNamespaceFull.
// Only enforced on creates (not updates to existing keys). 0 means unlimited (default).
func WithMaxKeysPerNamespace(n int) Option {
	return func(o *managerOptions) {
		if n >= 0 {
			o.maxKeysPerNS = n
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

// WithAlias registers a key alias that maps alias to target.
// When any operation (Get, Set, Delete) uses the alias key, it is transparently
// resolved to the target key. This enables key migration without changing
// application code that references old key names.
//
// Aliases are global (not per-namespace) and single-hop.
// Returns an error from New() if the alias is invalid.
//
// Example:
//
//	mgr, err := config.New(
//	    config.WithStore(store),
//	    config.WithAlias("db.host", "database/host"),
//	)
func WithAlias(alias, target string) Option {
	return func(o *managerOptions) {
		if o.aliases == nil {
			o.aliases = make(map[string]string)
		}
		o.aliases[alias] = target
	}
}

// WithAliases maps one or more alias keys to a single canonical target key.
// This is convenient when migrating multiple old key names to one new key.
// See WithAlias for details.
//
// Example:
//
//	mgr, err := config.New(
//	    config.WithStore(store),
//	    config.WithAliases("database/host", "db.host", "db-host", "legacy/db/host"),
//	)
func WithAliases(target string, aliases ...string) Option {
	return func(o *managerOptions) {
		if o.aliases == nil {
			o.aliases = make(map[string]string, len(aliases))
		}
		for _, alias := range aliases {
			o.aliases[alias] = target
		}
	}
}

// WithMemoryCacheTTL sets the TTL for entries in the default in-memory LRU cache.
// Entries older than d are automatically evicted. Use 0 (the default) to disable
// expiry and rely solely on LRU eviction. Has no effect when WithCache is also set.
func WithMemoryCacheTTL(d time.Duration) Option {
	return func(o *managerOptions) {
		if d > 0 {
			o.memoryCacheTTL = d
		}
	}
}

// WithCache sets a custom cache implementation for the Manager.
// When set, the provided cache is used instead of the default in-memory LRU.
// Use this to share cache state across instances with a distributed backend
// such as Redis:
//
//	mgr, err := config.New(
//	    config.WithStore(pgStore),
//	    config.WithCache(redis.NewCache(redisClient,
//	        redis.WithCacheTTL(5*time.Minute),
//	    )),
//	)
func WithCache(c Cache) Option {
	return func(o *managerOptions) {
		if c != nil {
			o.cache = c
		}
	}
}

// SetOption configures Set operations.
type SetOption func(*setOptions)

type setOptions struct {
	codec     codec.Codec
	typ       Type
	writeMode WriteMode
	expiresAt time.Time // zero = no TTL
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

// WithTTL sets a time-to-live on the stored value. After d has elapsed the
// value is treated as if it does not exist: Get returns ErrNotFound, Find/
// GetMany skip the entry, and the cache layer reports a miss.
//
// Physical removal of the row/document is backend-specific:
//
//   - memory   — a background sweeper deletes expired entries every
//     WithCleanupInterval (default 1m) and emits a ChangeTypeDelete watch event.
//   - mongodb  — MongoDB's native TTL index reaps expired documents
//     (typically within ~60s of expiry); change-stream events are emitted
//     by the server.
//   - postgres — read paths filter on expires_at; physical rows are removed
//     when the next write touches them. Run a periodic DELETE job to reclaim
//     storage and trigger NOTIFY events on demand.
//   - sqlite   — same as postgres; no native TTL.
//   - redis    — entries carry an embedded expiry; reads filter past-expiry
//     payloads. The Redis-level TTL on the hash key is not used because hash
//     fields cannot have individual TTLs.
//
// Passing 0 or a negative duration is ignored (no TTL is set). On update,
// passing 0 preserves any existing TTL on the entry.
//
// Example:
//
//	err := cfg.Set(ctx, "session/token", tok, config.WithTTL(24*time.Hour))
func WithTTL(d time.Duration) SetOption {
	return func(o *setOptions) {
		if d > 0 {
			o.expiresAt = time.Now().UTC().Add(d)
		}
	}
}
