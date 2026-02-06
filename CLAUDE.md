# CLAUDE.md

This file provides guidance for AI assistants working on this codebase.

## Project Overview

This is a Go configuration library that provides type-safe, namespace-aware configuration management with multiple storage backends, real-time watching, and caching.

## Architecture

### Core Interfaces

- **`Store`** (`store.go`): Storage backend interface with CRUD + Watch operations
- **`Value`** (`value.go`): Type-safe value wrapper with serialization
- **`Manager`** (`manager.go`): Top-level manager with caching and namespace access
- **`Config`** (`config.go`): Namespace-scoped Reader + Writer interface
- **`Cache`** (`cache.go`): Caching layer interface

### Package Structure

```
config/
├── store.go          # Store interface, ChangeEvent, filters
├── value.go          # Value interface, Val implementation
├── types.go          # Type enum (TypeInt, TypeString, etc.)
├── config.go         # Config interface (Reader + Writer)
├── manager.go        # Manager implementation with caching
├── options.go        # Manager and Set options
├── context.go        # Context helpers (Get/Set from context)
├── cache.go          # Cache interface and memory implementation
├── errors.go         # Error types and helpers
│
├── codec/            # Encoding/decoding
│   ├── codec.go      # Codec interface and registry
│   ├── json.go       # JSON codec
│   ├── yaml.go       # YAML codec
│   └── toml.go       # TOML codec
│
├── memory/           # In-memory store
│   └── store.go
│
├── postgres/         # PostgreSQL store (LISTEN/NOTIFY)
│   └── store.go
│
├── mongodb/          # MongoDB store (change streams)
│   └── store.go
│
├── file/             # File-based store (fsnotify)
│   ├── store.go
│   └── options.go
│
├── otel/             # OpenTelemetry instrumentation
│   ├── store.go      # Instrumented store wrapper
│   ├── metrics.go    # Metric definitions
│   └── options.go
│
├── bind/             # Struct binding with validation
│   ├── binder.go
│   ├── validate.go
│   ├── validate_tags.go
│   └── validate_schema.go
│
├── multi/            # Multi-store composition
│   ├── store.go      # Fallback, ReadThrough, WriteThrough strategies
│   └── options.go
│
└── live/             # Live config binding
    ├── ref.go        # Atomic live reference (Ref[T])
    └── binding.go    # Mutex-based live binding
```

## Key Design Decisions

### Value vs Entry

- `Value` is the public interface for configuration values
- Each store has its own internal `entry` struct (unexported)
- `Store.Get()` returns `Value`, not `Entry`
- `Store.Set()` accepts `Value`, not `Entry`

### Namespace Handling

- Use `config.DefaultNamespace` (empty string) when you don't need namespace separation
- Namespaces are user-defined (e.g., `prod`, `qa`, `prod/us/west1`, `tenant-123`)
- Namespace is required for all store operations

### Change Events

```go
type ChangeEvent struct {
    Type      ChangeType  // ChangeTypeSet or ChangeTypeDelete
    Namespace string
    Key       string
    Value     Value       // nil for Delete events
    Timestamp time.Time
}
```

No `PreviousVersion` - only current state is available (MongoDB change streams limitation).

### Store Interface

```go
type Store interface {
    Connect(ctx context.Context) error
    Close(ctx context.Context) error
    Get(ctx context.Context, namespace, key string) (Value, error)
    Set(ctx context.Context, namespace, key string, value Value) (Value, error)  // Returns stored value with metadata
    Delete(ctx context.Context, namespace, key string) error
    Find(ctx context.Context, namespace string, filter Filter) (Page, error)
    Watch(ctx context.Context, filter WatchFilter) (<-chan ChangeEvent, error)
}
```

### Filter and Page Interfaces

```go
// Filter interface for List queries (use NewFilter() builder)
type Filter interface {
    Keys() []string   // Specific keys (mutually exclusive with Prefix)
    Prefix() string   // Prefix match (mutually exclusive with Keys)
    Limit() int       // Max results (0 = no limit)
    Cursor() string   // Pagination cursor
}

// Page interface for cursor-based pagination
type Page interface {
    Results() map[string]Value
    NextCursor() string
    Limit() int  // Actual limit used; check len(Results()) < Limit() for more
}

// Filter builder pattern
filter := config.NewFilter().
    WithPrefix("app/").
    WithLimit(100).
    WithCursor(lastCursor).
    Build()
```

### Optional Store Interfaces

- `HealthChecker`: `Health(ctx) error`
- `StatsProvider`: `Stats(ctx) (*StoreStats, error)`
- `BulkStore`: `GetMany`, `SetMany`, `DeleteMany` (implemented by memory, postgres, mongodb stores)

### Multi-Store

Combines multiple stores for caching, fallback, or replication:

```go
// Strategies (all write to ALL stores for consistency)
multi.StrategyFallback     // Read from first available
multi.StrategyReadThrough  // Read through stores, populate earlier stores on miss
multi.StrategyWriteThrough // Write to all, read from first

// Multi-store implements HealthChecker and StatsProvider
store.Health(ctx)  // Check all underlying stores
store.Stats(ctx)   // Get stats from primary store
```

**Operation Behavior (intentional asymmetry):**

| Operation | Behavior | Reason |
|-----------|----------|--------|
| Get | Tries all stores in order | Fallback for cache misses |
| Set/Delete | Writes to all stores | Consistency across replicas |
| Find | Primary store only | Avoid duplicates, consistent pagination |
| Watch | Primary store only | Avoid duplicate events |

For cache + backend scenarios, Find/Watch on primary (cache) may not return all entries.
Use Get for individual keys when fallback behavior is needed.

### OpenTelemetry

Tracing and metrics are **disabled by default** (opt-in):

```go
store, _ := otel.WrapStore(baseStore,
    otel.WithTracesEnabled(true),   // Must explicitly enable
    otel.WithMetricsEnabled(true),  // Must explicitly enable
    otel.WithBackendName("postgres"),
)
```

## Common Tasks

### Adding a New Store Backend

1. Create package under `config/{backend}/`
2. Implement `Store` interface
3. Create internal entry struct for storage
4. Implement `Connect()` for initialization
5. Implement `Watch()` with backend's change mechanism
6. Add compile-time interface checks:
   ```go
   var _ config.Store = (*Store)(nil)
   var _ config.HealthChecker = (*Store)(nil)
   ```

### Adding a New Codec

1. Add file to `codec/` package
2. Implement `Codec` interface:
   ```go
   type Codec interface {
       Name() string
       Encode(v any) ([]byte, error)
       Decode(data []byte, v any) error
   }
   ```
3. Register in `init()`: `Register(&myCodec{})`

### Working with Values

```go
// Create from raw data
val := config.NewValue(42, config.WithValueType(config.TypeInt))

// Create from bytes
val, err := config.NewValueFromBytes(data, "json",
    config.WithValueType(config.TypeInt),
    config.WithValueMetadata(version, createdAt, updatedAt),
)

// Access methods (Value interface) - return errors
data, err := val.Marshal()
err = val.Unmarshal(&target)
i, err := val.Int64()   // Error if not convertible
s, err := val.String()  // Error if not convertible
meta := val.Metadata()
```

### Value Method Patterns

The `Value` interface provides error-returning methods for type-safe access:

| Method | Returns | Description |
|--------|---------|-------------|
| `Int64()` | `(int64, error)` | Error if not convertible |
| `Float64()` | `(float64, error)` | Error if not convertible |
| `String()` | `(string, error)` | Error if not convertible |
| `Bool()` | `(bool, error)` | Error if not convertible |

The concrete `Val` type also provides convenience methods (zero on error):

| Method | Returns | Description |
|--------|---------|-------------|
| `Int()` | `int` | Returns 0 on error |
| `Float()` | `float64` | Returns 0.0 on error |
| `BoolValue()` | `bool` | Returns false on error |
| `IntOr(default)` | `int` | Returns default on error |
| `FloatOr(default)` | `float64` | Returns default on error |

**Best Practice**: Use error-returning methods (`Int64()`, `String()`, etc.) when you need to handle errors. Use convenience methods (`Int()`, `IntOr()`) for cases where a default value is acceptable.

## Testing

```bash
# Run all tests
go test ./...

# Run specific package tests
go test ./memory/...
go test ./postgres/...  # Requires PostgreSQL
go test ./mongodb/...   # Requires MongoDB

# Environment variables for integration tests
POSTGRES_DSN=postgres://localhost:5432/config_test?sslmode=disable
MONGO_URI=mongodb://localhost:27017
```

## Code Style

- Use `ctx context.Context` as first parameter
- Return `(Value, error)` not `(*Value, error)`
- Use sentinel errors with `errors.Is()` checks
- Wrap backend errors with `WrapStoreError()`
- Use functional options pattern for configuration
- No default namespace - always explicit

## Error Handling

```go
// Sentinel errors
config.ErrNotFound
config.ErrTypeMismatch
config.ErrStoreClosed
config.ErrWatchNotSupported

// Check with errors.Is()
if config.IsNotFound(err) { ... }

// Wrap store errors
return config.WrapStoreError("get", "postgres", key, err)
```

## Dependencies

Core:
- `github.com/rbaliyan/config/codec` (internal)

Optional (for specific backends):
- `go.mongodb.org/mongo-driver` - MongoDB store
- `github.com/lib/pq` - PostgreSQL store
- `github.com/fsnotify/fsnotify` - File store
- `gopkg.in/yaml.v3` - YAML codec
- `github.com/BurntSushi/toml` - TOML codec
- `go.opentelemetry.io/otel` - Instrumentation (opt-in)

## Recent Changes

- **Store.Set returns Value**: `Set()` now returns `(Value, error)` with updated metadata, eliminating the need for extra Get calls
- **BulkStore for all stores**: `GetMany`, `SetMany`, `DeleteMany` implemented for memory, postgres, and mongodb stores
- **Configurable circuit breaker**: Use `WithCircuitBreaker(CircuitBreakerConfig{...})` to configure watch reconnection behavior
- **Cache metrics**: `Manager.CacheStats()` returns hit/miss/eviction statistics via `CacheStats` struct
- **DefaultNamespace constant**: `config.DefaultNamespace` for the empty string namespace
- **Conditional writes**: `WithIfNotExists()` and `WithIfExists()` options for Set operations
- **Live config binding**: `live.Ref[T]` provides atomic, lock-free access to typed config structs with background polling
- **OpenTelemetry opt-in**: Tracing and metrics disabled by default, must use `WithTracesEnabled(true)` and/or `WithMetricsEnabled(true)`
- **Multi-store consistency**: All strategies now write to all stores (not just primary)
- **Tags removed**: Tag support has been removed for simplicity; use key prefixes for categorization instead
