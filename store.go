package config

import (
	"context"
	"regexp"
	"slices"
	"strings"
	"time"
)

// DefaultNamespace is the default namespace (empty string).
// Use this when you don't need namespace separation.
const DefaultNamespace = ""

// validNamespace matches valid namespace names: alphanumeric, underscore, dash, dot, colon.
// Empty namespace is allowed (represents default namespace).
// Non-empty namespaces must start with an alphanumeric character.
var validNamespace = regexp.MustCompile(`^([a-zA-Z0-9][a-zA-Z0-9_.\-:]*)?$`)

// validKey matches valid key characters: alphanumeric, underscore, dash, dot, slash.
// Keys must not be empty and must not contain path traversal sequences.
var validKey = regexp.MustCompile(`^[a-zA-Z0-9_.\-/]+$`)

// ValidateNamespace validates a namespace name.
// Empty namespaces are allowed (represents the default namespace).
// Returns ErrInvalidNamespace if the namespace contains invalid characters.
func ValidateNamespace(namespace string) error {
	if !validNamespace.MatchString(namespace) {
		return ErrInvalidNamespace
	}
	return nil
}

// ValidateKey validates a configuration key.
// Keys must:
//   - Not be empty
//   - Contain only alphanumeric characters, underscores, dashes, dots, and slashes
//   - Not contain path traversal sequences (..)
//   - Not start or end with a slash
//
// Returns an InvalidKeyError if the key is invalid.
func ValidateKey(key string) error {
	if key == "" {
		return &InvalidKeyError{Key: key, Reason: "key cannot be empty"}
	}
	if strings.Contains(key, "..") {
		return &InvalidKeyError{Key: key, Reason: "key cannot contain path traversal (..)"}
	}
	if strings.HasPrefix(key, "/") || strings.HasSuffix(key, "/") {
		return &InvalidKeyError{Key: key, Reason: "key cannot start or end with slash"}
	}
	if !validKey.MatchString(key) {
		return &InvalidKeyError{Key: key, Reason: "key contains invalid characters"}
	}
	return nil
}

// Store defines the interface for configuration storage backends.
//
// Implementations must be safe for concurrent use by multiple goroutines.
// The store is responsible for persistence and versioning.
//
// # Design Philosophy
//
// This library is designed for use cases like feature flags, rate limits, and
// dynamic configuration where eventual consistency is acceptable. The key principle
// is: having some configuration (even slightly stale) is better than having no
// configuration at all.
//
// The library maintains an internal in-memory cache that serves as a resilience
// layer. If the backend store becomes temporarily unavailable, the application
// can continue operating with cached values. This cache is NOT meant for sharing
// state across multiple application instances - each instance maintains its own
// cache that is kept in sync with the backend via the store's Watch mechanism.
//
// For multi-instance deployments, each instance independently watches the backend
// (e.g., MongoDB Change Streams, PostgreSQL LISTEN/NOTIFY) to invalidate its local
// cache. This provides eventual consistency without requiring external dependencies
// like Redis.
//
// Implementations:
//   - memory.Store: For testing and single-instance deployments
//   - mongodb.Store: For MongoDB databases (uses Change Streams internally)
//   - postgres.Store: For PostgreSQL databases (uses LISTEN/NOTIFY internally)
type Store interface {
	// Connect establishes connection to the storage backend.
	// Must be called before any other operations.
	Connect(ctx context.Context) error

	// Close releases resources and closes the connection.
	Close(ctx context.Context) error

	// Get retrieves a configuration value by namespace and key.
	// Returns ErrNotFound if the entry does not exist.
	Get(ctx context.Context, namespace, key string) (Value, error)

	// Set creates or updates a configuration value.
	// The version is auto-incremented on each update.
	// Returns the stored Value with updated metadata (version, timestamps).
	Set(ctx context.Context, namespace, key string, value Value) (Value, error)

	// Delete removes a configuration value by namespace and key.
	// Returns ErrNotFound if the entry does not exist.
	Delete(ctx context.Context, namespace, key string) error

	// Find returns a page of keys and values matching the filter within a namespace.
	// Use Page.NextCursor() to paginate through results.
	Find(ctx context.Context, namespace string, filter Filter) (Page, error)

	// Watch returns a channel that receives change events for cache invalidation.
	// The channel is closed when the context is cancelled.
	// This is used by the Manager for automatic cache synchronization.
	// For stores that don't support real-time watching (e.g., file-based),
	// return ErrWatchNotSupported.
	Watch(ctx context.Context, filter WatchFilter) (<-chan ChangeEvent, error)
}

// HealthChecker is an optional interface for stores that support health checks.
type HealthChecker interface {
	// Health performs a health check on the store.
	// Returns nil if healthy, or an error describing the issue.
	Health(ctx context.Context) error
}

// StatsProvider is an optional interface for stores that provide statistics.
type StatsProvider interface {
	// Stats returns store statistics.
	Stats(ctx context.Context) (*StoreStats, error)
}

// StoreStats contains storage statistics.
type StoreStats struct {
	TotalEntries       int64            `json:"total_entries"`
	EntriesByType      map[Type]int64   `json:"entries_by_type"`
	EntriesByNamespace map[string]int64 `json:"entries_by_namespace"`
}

// CodecValidator is an optional interface for stores that restrict supported codecs.
// Stores that do not implement this accept all codecs (backward compatible).
type CodecValidator interface {
	SupportsCodec(codecName string) bool
}

// VersionedStore is an optional interface for stores that support version history.
// Stores that implement this interface retain all historical versions of config entries.
// Use type assertion to check if a store supports versioning:
//
//	if vs, ok := store.(config.VersionedStore); ok {
//	    // Get a specific version
//	    page, _ := vs.GetVersions(ctx, ns, key, config.NewVersionFilter().WithVersion(3).Build())
//	    // List all versions with pagination
//	    page, _ := vs.GetVersions(ctx, ns, key, config.NewVersionFilter().WithLimit(10).Build())
//	}
type VersionedStore interface {
	// GetVersions retrieves version history for a configuration key.
	//
	// When VersionFilter.Version() is set (> 0), returns a page containing only
	// that specific version. Returns ErrVersionNotFound if the version does not exist.
	//
	// When VersionFilter.Version() is 0, returns a paginated list of all versions
	// ordered by version descending (newest first). Use Limit() and Cursor() for pagination.
	//
	// Returns ErrNotFound if the key has never existed or was deleted.
	// Note: deleting a key may also remove its version history depending on the
	// store implementation. Database-backed stores (postgres, mongodb) retain
	// history in a separate table/collection; the memory store discards it.
	GetVersions(ctx context.Context, namespace, key string, filter VersionFilter) (VersionPage, error)
}

// AliasStore is an optional interface for stores that support persistent key aliases.
//
// Aliases provide transparent key migration by mapping old key names to new
// canonical keys. They occupy the same key space as configuration entries: a
// key that exists as a config entry cannot be used as an alias, and vice versa.
//
// Alias values reuse the [Value] interface, where the underlying data is the
// target key name (a string). The returned Value carries metadata such as
// version and timestamps.
//
// Stores that implement AliasStore emit [ChangeTypeAliasSet] and
// [ChangeTypeAliasDelete] events through the Watch channel so that all
// connected Manager instances stay in sync.
//
// Use type assertion to check if a store supports aliases:
//
//	if as, ok := store.(config.AliasStore); ok {
//	    val, err := as.SetAlias(ctx, "db.host", "database/host")
//	}
type AliasStore interface {
	// SetAlias creates a new alias mapping from alias to target.
	// Returns the stored Value (target key name with metadata).
	// Returns [ErrAliasExists] if the alias key is already registered as an
	// alias or already exists as a configuration entry in any namespace.
	SetAlias(ctx context.Context, alias, target string) (Value, error)

	// DeleteAlias removes an alias.
	// Returns [ErrNotFound] if the alias does not exist.
	DeleteAlias(ctx context.Context, alias string) error

	// GetAlias retrieves the target for a specific alias.
	// Returns [ErrNotFound] if the alias does not exist.
	GetAlias(ctx context.Context, alias string) (Value, error)

	// ListAliases returns all registered aliases as a map of alias → Value.
	// The Value for each entry contains the target key name as a string.
	ListAliases(ctx context.Context) (map[string]Value, error)
}

// BulkStore is an optional interface for stores that support batch operations.
// Implementing this interface allows efficient bulk reads and writes.
type BulkStore interface {
	// GetMany retrieves multiple values in a single operation.
	// Returns a map of key -> Value. Missing keys are not included in the result.
	GetMany(ctx context.Context, namespace string, keys []string) (map[string]Value, error)

	// SetMany creates or updates multiple values in a single operation.
	// SetMany always uses upsert semantics regardless of any WriteMode set on
	// individual values. Use individual Set calls for conditional (create-only
	// or update-only) writes.
	SetMany(ctx context.Context, namespace string, values map[string]Value) error

	// DeleteMany removes multiple values in a single operation.
	// Returns the number of entries actually deleted.
	DeleteMany(ctx context.Context, namespace string, keys []string) (int64, error)
}

// Filter defines criteria for listing configuration entries.
// Use NewFilter() to create a FilterBuilder and construct filters.
//
// Filters support two mutually exclusive modes:
//   - Keys mode: retrieve specific keys by exact match
//   - Prefix mode: retrieve all keys matching a prefix
//
// Example:
//
//	// Get specific keys
//	filter := config.NewFilter().WithKeys("db/host", "db/port").Build()
//
//	// Get all keys with prefix
//	filter := config.NewFilter().WithPrefix("db/").WithLimit(100).Build()
//
//	// Paginate with cursor
//	page, _ := cfg.Find(ctx, config.NewFilter().WithPrefix("").WithLimit(50).Build())
//	nextPage, _ := cfg.Find(ctx, config.NewFilter().WithPrefix("").WithLimit(50).WithCursor(page.NextCursor()).Build())
type Filter interface {
	// Keys returns specific keys to retrieve (mutually exclusive with Prefix).
	Keys() []string

	// Prefix returns the prefix to match (mutually exclusive with Keys).
	Prefix() string

	// Limit returns the maximum number of results (0 = no limit).
	Limit() int

	// Cursor returns the pagination cursor (entry ID) for continuing from a previous result.
	Cursor() string
}

// Page represents a page of results from a Find operation.
// It provides access to the results and pagination information.
//
// Example usage for pagination:
//
//	limit := 100
//	filter := config.NewFilter().WithPrefix("app/").WithLimit(limit).Build()
//	for {
//	    page, err := cfg.Find(ctx, filter)
//	    if err != nil {
//	        return err
//	    }
//	    for key, val := range page.Results() {
//	        // Process each entry
//	    }
//	    // No more results if returned count < limit
//	    if len(page.Results()) < page.Limit() {
//	        break
//	    }
//	    filter = config.NewFilter().WithPrefix("app/").WithLimit(limit).WithCursor(page.NextCursor()).Build()
//	}
type Page interface {
	// Results returns the values in this page as a map of key -> Value.
	Results() map[string]Value

	// NextCursor returns the cursor for fetching the next page.
	// This is typically the last key in the results.
	NextCursor() string

	// Limit returns the actual limit used by the server.
	// The server may adjust the requested limit. Clients should check
	// len(Results()) < Limit() to determine if there are more results.
	Limit() int
}

// page is the default Page implementation.
type page struct {
	results    map[string]Value
	nextCursor string
	limit      int
}

func (p *page) Results() map[string]Value { return p.results }
func (p *page) NextCursor() string        { return p.nextCursor }
func (p *page) Limit() int                { return p.limit }

// NewPage creates a new Page with the given results and pagination info.
// This is used by Store implementations to create Page results.
func NewPage(results map[string]Value, nextCursor string, limit int) Page {
	return &page{
		results:    results,
		nextCursor: nextCursor,
		limit:      limit,
	}
}

// FilterBuilder builds Filter instances using a fluent API.
type FilterBuilder struct {
	keys   []string
	prefix string
	limit  int
	cursor string
}

// NewFilter creates a new FilterBuilder.
func NewFilter() *FilterBuilder {
	return &FilterBuilder{}
}

// WithKeys sets specific keys to retrieve.
// Cannot be used with WithPrefix - calling this clears any prefix.
func (b *FilterBuilder) WithKeys(keys ...string) *FilterBuilder {
	b.keys = keys
	b.prefix = "" // Clear prefix - mutually exclusive
	return b
}

// WithPrefix sets a prefix to match keys.
// Cannot be used with WithKeys - calling this clears any keys.
func (b *FilterBuilder) WithPrefix(prefix string) *FilterBuilder {
	b.prefix = prefix
	b.keys = nil // Clear keys - mutually exclusive
	return b
}

// WithLimit sets the maximum number of results.
func (b *FilterBuilder) WithLimit(limit int) *FilterBuilder {
	b.limit = limit
	return b
}

// WithCursor sets the pagination cursor for continuing from a previous result.
func (b *FilterBuilder) WithCursor(cursor string) *FilterBuilder {
	b.cursor = cursor
	return b
}

// Build creates the Filter.
func (b *FilterBuilder) Build() Filter {
	return &filter{
		keys:   b.keys,
		prefix: b.prefix,
		limit:  b.limit,
		cursor: b.cursor,
	}
}

// filter implements Filter.
type filter struct {
	keys   []string
	prefix string
	limit  int
	cursor string
}

func (f *filter) Keys() []string { return f.keys }
func (f *filter) Prefix() string { return f.prefix }
func (f *filter) Limit() int     { return f.limit }
func (f *filter) Cursor() string { return f.cursor }

// VersionFilter defines criteria for retrieving version history.
// Use NewVersionFilter() to create a VersionFilterBuilder.
//
// Two modes of operation:
//   - Specific version: set Version() > 0 to retrieve a single version
//   - List all versions: leave Version() as 0 and use Limit()/Cursor() for pagination
type VersionFilter interface {
	// Version returns the specific version to retrieve (0 = all versions).
	Version() int64

	// Limit returns the maximum number of versions to return (0 = no limit).
	Limit() int

	// Cursor returns the pagination cursor for continuing from a previous result.
	Cursor() string
}

// VersionPage represents a page of version history results.
type VersionPage interface {
	// Versions returns the values in this page, ordered by version descending.
	Versions() []Value

	// NextCursor returns the cursor for fetching the next page.
	NextCursor() string

	// Limit returns the actual limit used.
	Limit() int
}

// versionFilter implements VersionFilter.
type versionFilter struct {
	version int64
	limit   int
	cursor  string
}

func (f *versionFilter) Version() int64 { return f.version }
func (f *versionFilter) Limit() int     { return f.limit }
func (f *versionFilter) Cursor() string { return f.cursor }

// versionPage implements VersionPage.
type versionPage struct {
	versions   []Value
	nextCursor string
	limit      int
}

func (p *versionPage) Versions() []Value  { return p.versions }
func (p *versionPage) NextCursor() string { return p.nextCursor }
func (p *versionPage) Limit() int         { return p.limit }

// NewVersionPage creates a new VersionPage with the given versions and pagination info.
// This is used by Store implementations to create VersionPage results.
func NewVersionPage(versions []Value, nextCursor string, limit int) VersionPage {
	return &versionPage{
		versions:   versions,
		nextCursor: nextCursor,
		limit:      limit,
	}
}

// VersionFilterBuilder builds VersionFilter instances using a fluent API.
type VersionFilterBuilder struct {
	version int64
	limit   int
	cursor  string
}

// NewVersionFilter creates a new VersionFilterBuilder.
func NewVersionFilter() *VersionFilterBuilder {
	return &VersionFilterBuilder{}
}

// WithVersion sets a specific version to retrieve.
// When set (> 0), GetVersions returns only that version.
// When 0 (default), GetVersions returns all versions with pagination.
func (b *VersionFilterBuilder) WithVersion(version int64) *VersionFilterBuilder {
	b.version = version
	return b
}

// WithLimit sets the maximum number of versions to return.
func (b *VersionFilterBuilder) WithLimit(limit int) *VersionFilterBuilder {
	b.limit = limit
	return b
}

// WithCursor sets the pagination cursor for continuing from a previous result.
func (b *VersionFilterBuilder) WithCursor(cursor string) *VersionFilterBuilder {
	b.cursor = cursor
	return b
}

// Build creates the VersionFilter.
func (b *VersionFilterBuilder) Build() VersionFilter {
	return &versionFilter{
		version: b.version,
		limit:   b.limit,
		cursor:  b.cursor,
	}
}

// WatchFilter specifies criteria for watching changes.
type WatchFilter struct {
	// Namespaces to watch (empty = all namespaces).
	Namespaces []string

	// Prefixes to watch within namespaces (empty = all keys).
	Prefixes []string
}

// ChangeEvent represents a configuration change notification.
type ChangeEvent struct {
	// Type indicates the kind of change.
	Type ChangeType

	// Namespace is the namespace of the changed key.
	Namespace string

	// Key is the key that changed.
	Key string

	// Value is the new value (nil for Delete events).
	Value Value

	// Timestamp is when the change occurred.
	Timestamp time.Time
}

// ChangeType represents the type of configuration change.
type ChangeType int

const (
	// ChangeTypeSet indicates a create or update operation.
	ChangeTypeSet ChangeType = iota

	// ChangeTypeDelete indicates a delete operation.
	ChangeTypeDelete

	// ChangeTypeAliasSet indicates an alias was created.
	ChangeTypeAliasSet

	// ChangeTypeAliasDelete indicates an alias was removed.
	ChangeTypeAliasDelete
)

// String returns the string representation of the change type.
func (c ChangeType) String() string {
	switch c {
	case ChangeTypeSet:
		return "set"
	case ChangeTypeDelete:
		return "delete"
	case ChangeTypeAliasSet:
		return "alias_set"
	case ChangeTypeAliasDelete:
		return "alias_delete"
	default:
		return "unknown"
	}
}

// IsAliasChange reports whether the change type is an alias event.
func (c ChangeType) IsAliasChange() bool {
	return c == ChangeTypeAliasSet || c == ChangeTypeAliasDelete
}

// MatchesWatchFilter reports whether event satisfies filter.
// Store implementations use this to decide which watchers receive an event.
// Alias change events always match (they are global and affect all namespaces).
func MatchesWatchFilter(event ChangeEvent, filter WatchFilter) bool {
	// Alias events are global — always deliver them.
	if event.Type.IsAliasChange() {
		return true
	}

	if len(filter.Namespaces) > 0 && !slices.Contains(filter.Namespaces, event.Namespace) {
		return false
	}
	if len(filter.Prefixes) > 0 {
		found := false
		for _, prefix := range filter.Prefixes {
			if strings.HasPrefix(event.Key, prefix) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
