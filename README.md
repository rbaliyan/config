# Config

[![CI](https://github.com/rbaliyan/config/actions/workflows/ci.yml/badge.svg)](https://github.com/rbaliyan/config/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/rbaliyan/config.svg)](https://pkg.go.dev/github.com/rbaliyan/config)
[![Go Report Card](https://goreportcard.com/badge/github.com/rbaliyan/config)](https://goreportcard.com/report/github.com/rbaliyan/config)
[![Release](https://img.shields.io/github/v/release/rbaliyan/config)](https://github.com/rbaliyan/config/releases/latest)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/rbaliyan/config/badge)](https://scorecard.dev/viewer/?uri=github.com/rbaliyan/config)

A type-safe, namespace-aware configuration library for Go with support for multiple storage backends, built-in resilience, and OpenTelemetry instrumentation.

## Features

- **Multiple Storage Backends**: Memory, PostgreSQL, MongoDB, SQLite, File, Redis, etcd, Kubernetes (ConfigMaps/Secrets)
- **Version History**: Retrieve and paginate historical versions of any config key
- **Multi-Store**: Combine stores for caching, fallback, or replication patterns
- **Namespace Isolation**: Organize configuration by environment, tenant, or service
- **Built-in Resilience**: Internal cache ensures app works during backend outages
- **Type-safe Values**: Strongly typed access with automatic conversion
- **Codecs**: JSON, YAML, TOML encoding support
- **OpenTelemetry**: Tracing and metrics instrumentation
- **Struct Binding**: Bind configuration to Go structs with validation
- **Live Binding**: Auto-reload structs on config changes via polling

## Design Philosophy

This library is designed for use cases like feature flags, rate limits, and dynamic configuration where **eventual consistency is acceptable**. The key principle is: having some configuration (even slightly stale) is better than having no configuration at all.

The library maintains an internal in-memory cache that serves as a resilience layer:
- If the backend store becomes temporarily unavailable, cached values continue to be served
- Each application instance maintains its own local cache
- Cache is automatically invalidated via the store's native change stream (MongoDB Change Streams, PostgreSQL LISTEN/NOTIFY)
- No external dependencies like Redis required for caching

## Installation

```bash
go get github.com/rbaliyan/config
```

## Quick Start

```go
package main

import (
    "context"
    "log"

    "github.com/rbaliyan/config"
    "github.com/rbaliyan/config/memory"
)

func main() {
    ctx := context.Background()

    // Create manager with memory store
    mgr, err := config.New(
        config.WithStore(memory.NewStore()),
    )
    if err != nil {
        log.Fatal(err)
    }

    // Connect to backend
    if err := mgr.Connect(ctx); err != nil {
        log.Fatal(err)
    }
    defer mgr.Close(ctx)

    // Get configuration for a namespace (use "" for default)
    cfg := mgr.Namespace("production")

    // Set a value
    if err := cfg.Set(ctx, "app/timeout", 30); err != nil {
        log.Fatal(err)
    }

    // Get a value
    val, err := cfg.Get(ctx, "app/timeout")
    if err != nil {
        log.Fatal(err)
    }

    // Unmarshal into a typed variable
    var timeout int
    if err := val.Unmarshal(ctx, &timeout); err != nil {
        log.Fatal(err)
    }

    log.Printf("Timeout: %d", timeout)
}
```

## Storage Backends

### Memory Store

In-memory storage for testing and single-instance deployments.

```go
import "github.com/rbaliyan/config/memory"

store := memory.NewStore()
```

### PostgreSQL Store

Persistent storage with LISTEN/NOTIFY for real-time updates.

```go
import (
    "database/sql"
    "github.com/lib/pq"
    "github.com/rbaliyan/config/postgres"
)

db, _ := sql.Open("postgres", "postgres://localhost/mydb")
listener := pq.NewListener(dsn, 10*time.Second, time.Minute, nil)

store := postgres.NewStore(db, listener,
    postgres.WithTable("config_entries"),
    postgres.WithNotifyChannel("config_changes"),
)
```

### MongoDB Store

Persistent storage with change streams for real-time updates.

```go
import (
    "go.mongodb.org/mongo-driver/v2/mongo"
    "go.mongodb.org/mongo-driver/v2/mongo/options"
    "github.com/rbaliyan/config/mongodb"
)

client, _ := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017"))

store := mongodb.NewStore(client,
    mongodb.WithDatabase("config"),
    mongodb.WithCollection("entries"),
)
```

### SQLite Store

Lightweight persistent storage with no external dependencies.

The store does not manage the database connection lifecycle: open the `*sql.DB`
yourself and pass it in.

```go
import (
    "database/sql"

    "github.com/rbaliyan/config/sqlite"
)

db, err := sql.Open("sqlite3", "config.db")
if err != nil {
    log.Fatal(err)
}

store := sqlite.NewStore(db,
    sqlite.WithTable("config_entries"),
)
```

### File Store

Load configuration from YAML, TOML, or JSON files on disk.

By default the file store is read-only: `Set` and `Delete` return `config.ErrReadOnly` and `Watch` returns `config.ErrWatchNotSupported`. Pass `file.WithWritable()` to enable writes, in which case writes are persisted to a sidecar file (default: `{path}.writes.yaml`) that overlays the base file, and `Watch` is served by polling both files at `WithWatchInterval` (default 2s).

```go
import "github.com/rbaliyan/config/file"

// Read-only (default).
store := file.NewStore("config.yaml")

// Writable: Set/Delete persist to config.yaml.writes.yaml, Watch is polling-based.
writable := file.NewStore("config.yaml",
    file.WithWritable(),
    file.WithWatchInterval(5*time.Second),
)
```

Top-level keys in the file become namespaces; nested keys are flattened with `/` (configurable via `WithKeySeparator`). Top-level scalar values go into the namespace set by `WithDefaultNamespace` (default: `"default"`).

Pass `WithExpansion` or `WithAngleBracketExpander` to substitute `${VAR}` or `<VAR>` placeholders at load time — see [Variable Expansion](#variable-expansion).

### Redis Store

Persistent storage backed by Redis. Each namespace is stored as a single Redis hash (`{keyPrefix}:{namespace}`); change events are published on a single pub/sub channel (`{keyPrefix}:changes`). Writes and notifications are performed atomically via Lua scripts, so watchers never observe a write they cannot read back.

```go
import "github.com/rbaliyan/config/redis"

store := redis.NewStore(
    redis.WithAddress("redis.internal:6379"),
    redis.WithKeyPrefix("cfg"),     // hash names become "cfg:{namespace}"
    redis.WithPassword("s3cret"),
)

// Or connect to a Redis Cluster.
cluster := redis.NewStore(
    redis.WithCluster("redis-0:6379", "redis-1:6379", "redis-2:6379"),
)
```

### etcd Store

Persistent storage backed by etcd v3. Values are stored as JSON envelopes that
preserve the codec name and metadata across round-trips, and the namespace is
used as the etcd key prefix (`/<namespace>/<key>`, or `/<key>` for the default
empty namespace). Real-time `Watch` is served by etcd's native watch API.

```go
import (
    clientv3 "go.etcd.io/etcd/client/v3"

    "github.com/rbaliyan/config/etcd"
)

client, err := clientv3.New(clientv3.Config{
    Endpoints: []string{"etcd.internal:2379"},
})
if err != nil {
    log.Fatal(err)
}

store, err := etcd.New(client, "prod",     // namespace = etcd key prefix
    etcd.WithWatchBufferSize(128),         // Watch channel buffer (default 64)
)
if err != nil {
    log.Fatal(err)
}
```

### Kubernetes Store

Persistent storage backed by Kubernetes ConfigMaps and Secrets, with real-time `Watch`. Config namespaces map to Kubernetes namespaces (or a single fixed namespace via `WithK8sNamespace`). Each config namespace is backed by one ConfigMap named `config-{namespace}`; keys prefixed with `secret/` (configurable via `WithSecretKeyPrefix`) are routed to a Secret named `config-secrets-{namespace}` instead.

The k8s store ships in the main module and imports no `k8s.io/*` packages. Instead of a `kubernetes.Clientset`, `NewStore` takes a narrow `Client` interface (Get/Upsert/Watch/Health) that the caller supplies. A reference adapter built on `kubernetes.Interface` lives in a separate module at `k8s/example` (so `client-go` stays out of the core module); import it directly or copy it into your project.

```bash
go get github.com/rbaliyan/config/k8s/example
```

```go
import (
    "github.com/rbaliyan/config/k8s"
    kubeclient "github.com/rbaliyan/config/k8s/example"
    "k8s.io/client-go/kubernetes"
    "k8s.io/client-go/tools/clientcmd"
)

clientConfig, _ := clientcmd.BuildConfigFromFlags("", "/path/to/kubeconfig")
clientset, _ := kubernetes.NewForConfig(clientConfig)

adapter := kubeclient.New(clientset) // kubernetes.Interface -> k8s.Client
store := k8s.NewStore(adapter,
    k8s.WithK8sNamespace("my-app"),       // restrict to one k8s namespace
    k8s.WithSecretKeyPrefix("secret/"),   // keys with this prefix -> Secrets
)
if err := store.Connect(ctx); err != nil { // establishes the Client and starts the watch (no cache sync)
    return err
}
defer store.Close(ctx)
```

Config key `/` characters are replaced with `.` to form valid Kubernetes data keys (e.g. `app/timeout` becomes `app.timeout`).

## Working with Values

### Reading Values

```go
val, err := cfg.Get(ctx, "database/port")
if err != nil {
    if config.IsNotFound(err) {
        // Key doesn't exist
    }
    return err
}

// Type-safe access via Value interface
port, err := val.Int64()
str, err := val.String()
flag, err := val.Bool()
num, err := val.Float64()

// Unmarshal into any type
var dbConfig DatabaseConfig
if err := val.Unmarshal(ctx, &dbConfig); err != nil {
    return err
}

// Access metadata
meta := val.Metadata()
version := meta.Version()
created := meta.CreatedAt()
updated := meta.UpdatedAt()
```

### Writing Values

```go
// Simple values
cfg.Set(ctx, "app/timeout", 30)
cfg.Set(ctx, "app/name", "myservice")
cfg.Set(ctx, "app/enabled", true)

// Complex values
cfg.Set(ctx, "app/servers", []string{"host1", "host2"})
cfg.Set(ctx, "app/limits", map[string]int{"max": 100, "min": 1})

// With options
cfg.Set(ctx, "app/config", value,
    config.WithType(config.TypeCustom),
    config.WithSetCodec(yamlCodec),
)
```

### Conditional Writes

Control create/update behavior with conditional write options:

```go
// Create only - fails if key already exists
err := cfg.Set(ctx, "feature/flag", true, config.WithIfNotExists())
if config.IsKeyExists(err) {
    // Key already existed, value not changed
}

// Update only - fails if key doesn't exist
err = cfg.Set(ctx, "feature/flag", false, config.WithIfExists())
if config.IsNotFound(err) {
    // Key didn't exist, nothing updated
}

// Default (upsert) - creates or updates
cfg.Set(ctx, "feature/flag", true) // Always succeeds
```

These options leverage atomic database operations:
- **PostgreSQL**: Uses `ON CONFLICT DO NOTHING` / `UPDATE` with row count checks
- **MongoDB**: Uses `InsertOne` / `FindOneAndUpdate` with upsert control

### Listing Values

```go
// List all keys with prefix using the Filter builder
limit := 100
page, err := cfg.Find(ctx, config.NewFilter().
    WithPrefix("app/database").
    WithLimit(limit).
    Build())

for key, val := range page.Results() {
    str, _ := val.String()
    fmt.Printf("%s = %s\n", key, str)
}

// Pagination: check if len(results) < limit to determine if more pages exist
if len(page.Results()) == page.Limit() {
    nextPage, _ := cfg.Find(ctx, config.NewFilter().
        WithPrefix("app/database").
        WithLimit(limit).
        WithCursor(page.NextCursor()).
        Build())
    // Process nextPage...
}
```

### Listing Namespaces

Stores that implement `NamespaceLister` expose efficient, server-side
paginated enumeration of namespace names. Memory, SQLite, PostgreSQL, and
MongoDB ship with native implementations; Redis is intentionally excluded
and falls back to `StatsProvider`.

```go
// Type-assert against the underlying store (the Manager does not expose
// NamespaceLister directly — type-assert the store you pass into the Manager).
if nl, ok := store.(config.NamespaceLister); ok {
    cursor := ""
    for {
        names, next, err := nl.ListNamespaces(ctx, "prod-", 50, cursor)
        if err != nil {
            if config.IsInvalidCursor(err) {
                // Cursor was malformed, expired, or from a different store.
                // Retry from the first page.
                cursor = ""
                continue
            }
            return err
        }
        for _, ns := range names {
            handle(ns)
        }
        if next == "" {
            break
        }
        cursor = next
    }
}
```

Contract highlights (see `config.NamespaceLister` godoc for the full
contract): namespaces are returned in ascending byte-wise UTF-8 order;
cursors are opaque and **not portable across backends** (a cursor from
the postgres store passed to the mongodb store returns
`ErrInvalidCursor`); `limit <= 0` is a caller error on every backend
shipped today (no silent default-fallback).

> **Wrapper note:** the `otel`, `multi`, `transform`, and `expand`
> wrapper stores do not currently forward `NamespaceLister`. A type
> assertion against a wrapped store returns `ok == false`; callers that
> need direct enumeration must unwrap to the underlying backend.

### Version History

Stores that implement `VersionedStore` retain historical versions of config entries. Use `GetVersions` to retrieve them:

```go
// Check if versioning is available
if vr, ok := cfg.(config.VersionedReader); ok {
    // Get a specific version
    page, err := vr.GetVersions(ctx, "app/timeout",
        config.NewVersionFilter().WithVersion(2).Build())

    // List all versions (newest first) with pagination
    page, err := vr.GetVersions(ctx, "app/timeout",
        config.NewVersionFilter().WithLimit(10).Build())

    for _, v := range page.Versions() {
        fmt.Printf("v%d: %v (updated %s)\n",
            v.Metadata().Version(), v, v.Metadata().UpdatedAt())
    }

    // Paginate through all versions
    if len(page.Versions()) == page.Limit() {
        nextPage, _ := vr.GetVersions(ctx, "app/timeout",
            config.NewVersionFilter().
                WithLimit(10).
                WithCursor(page.NextCursor()).
                Build())
        // ...
    }
}
```

The memory store supports versioning with an optional history cap:

```go
store := memory.NewStore(memory.WithMaxHistory(100)) // Keep up to 100 versions per key
```

## Namespaces

Namespaces provide isolation between different environments or tenants.

```go
// Get configs for different namespaces
prodCfg := mgr.Namespace("production")
devCfg := mgr.Namespace("development")

// Same key, different values per namespace
prodCfg.Set(ctx, "timeout", 60)
devCfg.Set(ctx, "timeout", 5)

// Use "" for the default namespace
defaultCfg := mgr.Namespace("")
```

Namespace names may contain alphanumeric characters, underscores, dashes, dots, and colons (e.g. `org.example:env.prod`).

### System Namespaces

The `internal:` prefix is reserved for system namespaces used by infrastructure components such as key rotation and service discovery. Use `IsSystemNamespace()` to check whether a namespace is reserved:

```go
config.IsSystemNamespace("internal:config:crypto") // true
config.IsSystemNamespace("production")              // false
```

Server-side authorizers can use this to block client writes to system namespaces while allowing infrastructure components to manage them.

## Context Helpers

Access configuration from context without explicit dependency injection.

```go
// Add manager to context
ctx = config.ContextWithManager(ctx, mgr)
ctx = config.ContextWithNamespace(ctx, "production")

// Use anywhere in your application
val, err := config.Get(ctx, "app/setting")
err = config.Set(ctx, "app/setting", "value")
```

## Multi-Store (Fallback Pattern)

Combine multiple stores for caching, fallback, or replication:

```go
import "github.com/rbaliyan/config/multi"

// Cache + Backend pattern
cacheStore := memory.NewStore()
backendStore := postgres.NewStore(db, listener)
store := multi.NewStore(
    []config.Store{cacheStore, backendStore},
    multi.WithStrategy(multi.StrategyReadThrough),
)

// Primary + Backup pattern
primaryStore := postgres.NewStore(primaryDB, primaryListener)
backupStore := postgres.NewStore(backupDB, backupListener)
store := multi.NewStore(
    []config.Store{primaryStore, backupStore},
    multi.WithStrategy(multi.StrategyFallback),
)

mgr, _ := config.New(config.WithStore(store))
```

Strategies:
- `StrategyFallback`: Read from first available store, write to all stores
- `StrategyReadThrough`: Read through stores (cache miss populates earlier stores), write to all stores
- `StrategyWriteThrough`: Write to all stores, read from first available store

All strategies write to all stores to maintain consistency. The difference is in read behavior:
- Fallback/WriteThrough: Return first successful read
- ReadThrough: Try each store in order, populate earlier stores on cache miss

### Multi-Store Diagnostics

Multi-Store supports health checks and statistics when the underlying stores implement the optional interfaces:

```go
// Health check (if underlying stores implement HealthChecker)
if err := store.Health(ctx); err != nil {
    log.Printf("Store unhealthy: %v", err)
}

// Statistics (if underlying stores implement StatsProvider)
stats, err := store.Stats(ctx)
if err == nil {
    log.Printf("Total entries: %d", stats.TotalEntries())
}
```

Because a write only needs one store to succeed, replicas can diverge when some
stores are temporarily unavailable. Such partial failures are never silently
discarded:

```go
store := multi.NewStore(
    []config.Store{primaryStore, backupStore},
    // Surface partial write failures via a callback (logging/metrics).
    multi.WithOnWriteError(func(e *multi.PartialWriteError) {
        log.Printf("partial write %s %s/%s: %d of %d replicas failed",
            e.Op, e.Namespace, e.Key, e.Failed, e.Stores)
    }),
    // Optionally fail the write outright when any replica fails,
    // trading availability for an all-replicas-or-fail contract.
    multi.WithStrictWrites(),
)

// Count partial writes observed since the store was created.
log.Printf("partial writes: %d", store.PartialWrites())
```

For primary/secondary replication with synchronous or asynchronous propagation and configurable read preferences, see the dedicated `replica` package (`replica.NewStore(primary, secondaries, ...)`).

## OpenTelemetry Instrumentation

Wrap stores with tracing and metrics. Both are disabled by default and must be explicitly enabled.

```go
import "github.com/rbaliyan/config/otel"

// Enable tracing and metrics explicitly
instrumentedStore, _ := otel.WrapStore(store,
    otel.WithServiceName("my-service"),
    otel.WithBackendName("postgres"),
    otel.WithTracesEnabled(true),   // Opt-in, disabled by default
    otel.WithMetricsEnabled(true),  // Opt-in, disabled by default
)

mgr, _ := config.New(config.WithStore(instrumentedStore))
```

Metrics exported:
- `config.operations.total` - Counter of all operations
- `config.errors.total` - Counter of errors by type
- `config.operation.duration` - Histogram of operation latency

## Transform Store

`transform.WrapStore` decorates any `config.Store` with a `codec.Transformer` — a
reversible byte-level transformation such as encryption or compression. The
forward transform is applied on `Set` (before writing) and the reverse on `Get`
(after reading), so values are stored transformed and returned in their original
form. The wrapper transparently forwards the optional `HealthChecker`,
`StatsProvider`, `BulkStore`, and `VersionedStore` interfaces to the inner store,
and exposes `Unwrap()` to retrieve it.

```go
import (
    "github.com/rbaliyan/config/codec"
    "github.com/rbaliyan/config/transform"
)

// transformer implements codec.Transformer (Name/Transform/Reverse).
store, err := transform.WrapStore(backendStore, transformer,
    transform.WithWatchBufferSize(128), // Watch output channel buffer (default 100)
)
if err != nil {
    log.Fatal(err)
}

mgr, _ := config.New(config.WithStore(store))
```

## Struct Binding

Bind configuration to Go structs with validation. Struct fields are automatically mapped to hierarchical keys using the configured struct tag (default: `json`).

```go
import "github.com/rbaliyan/config/bind"

type DatabaseConfig struct {
    Host string `json:"host" validate:"required"`
    Port int    `json:"port" validate:"required,min=1,max=65535"`
}

binder := bind.New(cfg, bind.WithTagValidation())
bound := binder.Bind()

// Store a struct - creates keys: database/host, database/port
err := bound.SetStruct(ctx, "database", DatabaseConfig{Host: "localhost", Port: 5432})

// Retrieve a struct - reads keys: database/host, database/port
var dbConfig DatabaseConfig
err = bound.GetStruct(ctx, "database", &dbConfig)
```

Nested structs are also supported:

```go
type AppConfig struct {
    Name  string      `json:"name"`
    Cache CacheConfig `json:"cache"`
}

type CacheConfig struct {
    TTL     int  `json:"ttl"`
    Enabled bool `json:"enabled"`
}

// SetStruct creates: app/name, app/cache/ttl, app/cache/enabled
err := bound.SetStruct(ctx, "app", AppConfig{
    Name: "myapp",
    Cache: CacheConfig{TTL: 300, Enabled: true},
})
```

Use `nonrecursive` to store a nested struct as a single JSON value instead of flattening:

```go
type Credentials struct {
    Username string `json:"username"`
    Password string `json:"password"`
}

type AppConfig struct {
    Name  string      `json:"name"`
    Creds Credentials `json:"creds,nonrecursive"` // Store as single JSON value
}

// SetStruct creates: app/name, app/creds (not app/creds/username, app/creds/password)
// Useful when fields are tightly coupled and should be updated atomically
```

## Live Config (Auto-Reload)

Keep a typed struct automatically synchronized with configuration using polling and atomic swap:

```go
import "github.com/rbaliyan/config/live"

type DatabaseConfig struct {
    Host string `json:"host"`
    Port int    `json:"port"`
}

ref, err := live.New[DatabaseConfig](ctx, cfg, "database",
    live.WithRefPollInterval[DatabaseConfig](10*time.Second),
    live.OnChange(func(old, new DatabaseConfig) {
        log.Printf("config changed: %s -> %s", old.Host, new.Host)
    }),
    live.OnError(func(err error) {
        log.Printf("reload error: %v", err)
    }),
)
if err != nil {
    return err
}
defer ref.Close()

// Hot path: single atomic load, zero contention
http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
    snap := ref.Load()
    fmt.Fprintf(w, "Host: %s, Port: %d", snap.Host, snap.Port)
})
```

Options:
- `WithRefPollInterval(d)` - Set polling interval (default: 30s)
- `OnChange(fn)` - Callback with old and new values on change
- `OnError(fn)` - Callback on reload error

Methods:
- `Load()` - Get current snapshot (atomic, zero-cost)
- `Close()` - Stop background polling
- `ReloadNow(ctx)` - Force immediate reload
- `LastReload()` - Get last reload timestamp
- `LastError()` - Get last error (nil if successful)
- `ReloadCount()` - Get total successful reload count

## Codecs

Multiple encoding formats are supported.

```go
import "github.com/rbaliyan/config/codec"

// Available codecs
jsonCodec := codec.Get("json")
yamlCodec := codec.Get("yaml")
tomlCodec := codec.Get("toml")

// Use with manager
mgr, _ := config.New(
    config.WithStore(store),
    config.WithCodec(yamlCodec),
)
```

## Configuration Types

The library tracks value types for better type safety.

```go
const (
    TypeInt             // int, int64
    TypeFloat           // float64
    TypeString          // string
    TypeBool            // bool
    TypeMapStringInt    // map[string]int
    TypeMapStringFloat  // map[string]float64
    TypeMapStringString // map[string]string
    TypeListInt         // []int
    TypeListFloat       // []float64
    TypeListString      // []string
    TypeCustom          // any other type
)
```

## Error Handling

```go
val, err := cfg.Get(ctx, "key")
if err != nil {
    switch {
    case config.IsNotFound(err):
        // Key doesn't exist
    case config.IsTypeMismatch(err):
        // Type conversion failed
    case config.IsKeyExists(err):
        // Key already exists (from WithIfNotExists)
    default:
        // Other error
    }
}
```

## Variable Expansion

Placeholder tokens in string values can be substituted at parse time (file
store only) or at query time (any backend via `expand.Store`).

### Parse-time expansion (file store)

Placeholders are resolved once when the file is loaded. Changes to the
underlying source (env vars, etc.) are not reflected until the store reloads.

```go
// ${VAR} and ${VAR:-default} from environment variables.
store := file.NewStore("config.yaml",
    file.WithExpansion(file.EnvExpander()),
)

// <secret-name> from a custom secrets provider.
store := file.NewStore("config.yaml",
    file.WithAngleBracketExpander(func(name string) (string, bool) {
        return vault.Get(name) // your lookup
    }),
)

// Chain multiple sources: custom overrides first, env fallback.
store := file.NewStore("config.yaml",
    file.WithExpansion(overridesFn),
    file.WithExpansion(file.EnvExpander()),
)
```

Config file syntax:

```yaml
database:
  host: ${DB_HOST}               # replaced with env var
  port: ${DB_PORT:-5432}         # fallback to 5432 if unset
  password: <db_password>        # replaced via angle-bracket expander
  literal: \${NOT_EXPANDED}      # backslash disables substitution
```

### Query-time expansion (any backend)

`expand.NewStore` wraps any `config.Store` and expands placeholders on every
`Get` and `Find`, so changes to the source are reflected immediately.

```go
import "github.com/rbaliyan/config/expand"

inner := memory.NewStore()
if err := inner.Connect(ctx); err != nil {
    log.Fatal(err)
}

s, err := expand.NewStore(inner,
    expand.WithDollarExpander(expand.EnvExpander()),  // ${VAR}
    expand.WithAngleExpander(secretsFn),              // <VAR>
)

// Reads expand at call time; writes pass through to the inner store unchanged.
val, _ := s.Get(ctx, "app", "host")
```

## Sensitive Values

`config.Secret` wraps a sensitive string and masks it in all output so secrets
are never accidentally leaked into logs, error messages, or HTTP responses.

```go
type AppConfig struct {
    DBPassword config.Secret `yaml:"db_password"`
    APIKey     config.Secret `json:"api_key"`
}

var cfg AppConfig
// Decode from YAML/JSON/TOML: Bytes() holds the real plaintext.
yaml.Unmarshal(data, &cfg)

fmt.Println(cfg.DBPassword)                  // ******
log.Info("config loaded", "cfg", cfg)        // *** no leak ***
realPwd := string(cfg.DBPassword.Bytes())    // "actual-password"

// Marshal to JSON always masks: a Secret marshals as "******" whether zero or not.
out, _ := json.Marshal(cfg)
// Marshal to text (YAML/TOML) is zero-aware: a non-zero Secret writes "******",
// while a zero Secret writes "" so round-tripped files are not polluted.
yamlOut, _ := yaml.Marshal(cfg)

// IsZero reports whether no value has been set.
if cfg.APIKey.IsZero() {
    log.Warn("API key not configured")
}
```

`UnmarshalText("******")` produces a zero `Secret` (empty value), preventing a
masked token from being treated as a real credential on round-trip.

## Manager Options

```go
mgr, err := config.New(
    config.WithStore(store),           // Required: storage backend
    config.WithCodec(yamlCodec),       // Optional: default codec (default: JSON)
    config.WithLogger(slogLogger),     // Optional: custom logger
)
```

## License

MIT License
