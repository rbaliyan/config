# Config

A type-safe, namespace-aware configuration library for Go with support for multiple storage backends, built-in resilience, and OpenTelemetry instrumentation.

## Features

- **Multiple Storage Backends**: Memory, PostgreSQL, MongoDB
- **Multi-Store**: Combine stores for caching, fallback, or replication patterns
- **Namespace Isolation**: Organize configuration by environment, tenant, or service
- **Tags**: Multi-dimensional entry organization with tag-based filtering
- **Built-in Resilience**: Internal cache ensures app works during backend outages
- **Type-safe Values**: Strongly typed access with automatic conversion
- **Codecs**: JSON, YAML, TOML encoding support
- **OpenTelemetry**: Tracing and metrics instrumentation
- **Struct Binding**: Bind configuration to Go structs with validation

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
    mgr := config.New(
        config.WithStore(memory.NewStore()),
    )

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
    if err := val.Unmarshal(&timeout); err != nil {
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
    "go.mongodb.org/mongo-driver/mongo"
    "github.com/rbaliyan/config/mongodb"
)

client, _ := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))

store := mongodb.NewStore(client,
    mongodb.WithDatabase("config"),
    mongodb.WithCollection("entries"),
)
```

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
if err := val.Unmarshal(&dbConfig); err != nil {
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

### Listing Values

```go
// List all keys with prefix using the Filter builder
limit := 100
page, err := cfg.Find(ctx, config.NewFilter().
    WithPrefix("app/database").
    WithLimit(limit).
    Build())

for key, val := range page.Results() {
    fmt.Printf("%s = %v\n", key, val.Raw())
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
store := multi.NewStoreWithOptions(
    []config.Store{cacheStore, backendStore},
    []multi.Option{multi.WithStrategy(multi.StrategyReadThrough)},
)

// Primary + Backup pattern
primaryStore := postgres.NewStore(primaryDB, primaryListener)
backupStore := postgres.NewStore(backupDB, backupListener)
store := multi.NewStoreWithOptions(
    []config.Store{primaryStore, backupStore},
    []multi.Option{multi.WithStrategy(multi.StrategyFallback)},
)

mgr := config.New(config.WithStore(store))
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
    log.Printf("Total entries: %d", stats.TotalEntries)
}
```

## Tags

Tag entries for multi-dimensional organization. Tags are part of an entry's identity - the same key with different tags creates separate entries.

```go
// Set with tags
cfg.Set(ctx, "database/host", "prod-db.example.com",
    config.WithTags(
        config.MustTag("env", "production"),
        config.MustTag("region", "us-west"),
    ),
)

// Same key, different tags = different entry
cfg.Set(ctx, "database/host", "dev-db.example.com",
    config.WithTags(config.MustTag("env", "development")),
)

// Get with tags (must match exactly)
val, err := cfg.Get(ctx, "database/host",
    config.MustTag("env", "production"),
    config.MustTag("region", "us-west"),
)

// Delete with tags
cfg.Delete(ctx, "database/host", config.MustTag("env", "development"))

// Find by tags
page, _ := cfg.Find(ctx, config.NewFilter().
    WithPrefix("database/").
    WithTags(config.MustTag("env", "production")).
    Build())

// Access tags from value
tags := val.Metadata().Tags()
for _, t := range tags {
    fmt.Printf("%s=%s\n", t.Key(), t.Value())
}
```

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

mgr := config.New(config.WithStore(instrumentedStore))
```

Metrics exported:
- `config.operations.total` - Counter of all operations
- `config.errors.total` - Counter of errors by type
- `config.operation.duration` - Histogram of operation latency

## Struct Binding

Bind configuration to Go structs with validation.

```go
import "github.com/rbaliyan/config/bind"

type DatabaseConfig struct {
    Host string `json:"host" validate:"required"`
    Port int    `json:"port" validate:"required,min=1,max=65535"`
}

binder := bind.New(cfg, bind.WithTagValidation())
bound := binder.Bind()

var dbConfig DatabaseConfig
err := bound.GetStruct(ctx, "database", &dbConfig)
```

## Codecs

Multiple encoding formats are supported.

```go
import "github.com/rbaliyan/config/codec"

// Available codecs
jsonCodec := codec.Get("json")
yamlCodec := codec.Get("yaml")
tomlCodec := codec.Get("toml")

// Use with manager
mgr := config.New(
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
    default:
        // Other error
    }
}
```

## Manager Options

```go
mgr := config.New(
    config.WithStore(store),           // Required: storage backend
    config.WithCodec(yamlCodec),       // Optional: default codec (default: JSON)
    config.WithLogger(slogLogger),     // Optional: custom logger
    config.WithCacheTTL(5*time.Minute),// Optional: cache TTL (default: 5 min)
)
```

## License

MIT License
