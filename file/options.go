// Package file provides file-based configuration loading and a
// config.Store backed by YAML, TOML, or JSON files.
//
// It provides two main types:
//   - Loader: register named config structs and unmarshal file sections into them (replaces go-conf/viper)
//   - Store: implements config.Store with file contents, mapping top-level keys to namespaces.
//     Read-only by default; enable writes via WithWritable().
//
// # Read mode (default)
//
// When constructed without WithWritable, the Store loads the file once on
// Connect and serves Get and Find from memory. Set and Delete return
// config.ErrReadOnly; Watch returns config.ErrWatchNotSupported.
//
// # Writable mode
//
// WithWritable enables Set, Delete, and Watch. Writes are persisted to a
// companion "sidecar" file (default: "{path}.writes.yaml") so the base file
// stays pristine and can continue to be edited by hand. On Connect the
// sidecar is layered on top of the base file; tombstones in the sidecar
// hide deleted keys. A background goroutine polls both files at
// WithWatchInterval (default 2s) to detect external edits and emit Watch
// events.
//
// # Variable expansion (parse-time)
//
// WithExpansion and WithAngleBracketExpander enable placeholder substitution
// in string values at file-load time. Placeholders are resolved once when the
// file is read; subsequent env-var changes are not reflected until the store
// reconnects or the file is reloaded.
//
// For query-time expansion that is re-evaluated on every Get (and works with
// any backend), wrap the store with expand.NewStore instead.
//
// Usage:
//
//	// Read-only — fail on any Set attempt.
//	store := file.NewStore("config.yaml")
//
//	// Writable — Set persists to config.yaml.writes.yaml, Watch works.
//	store := file.NewStore("config.yaml",
//	    file.WithWritable(),
//	    file.WithWatchInterval(5*time.Second),
//	)
//
//	// Parse-time ${VAR} substitution from environment.
//	store := file.NewStore("config.yaml",
//	    file.WithExpansion(file.EnvExpander()),
//	)
//
//	// Angle-bracket <secret-name> substitution from a secrets provider.
//	store := file.NewStore("config.yaml",
//	    file.WithAngleBracketExpander(vaultLookup),
//	)
package file

import (
	"log/slog"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/rbaliyan/config"
)

// LoaderOption configures a Loader.
type LoaderOption func(*loaderOptions)

type loaderOptions struct {
	format     string // explicit format override (yaml, toml, json)
	strict     bool   // fail on unknown fields
	tagName    string // struct tag for mapping (default: mapstructure)
	decoderFns []mapstructure.DecodeHookFunc
	logger     *slog.Logger
}

// WithFormat explicitly sets the config file format.
// Overrides auto-detection from file extension.
// Supported: "yaml", "yml", "toml", "json".
func WithFormat(format string) LoaderOption {
	return func(o *loaderOptions) {
		o.format = format
	}
}

// WithStrictMode enables strict unmarshaling.
// Unknown fields in the config file that don't map to struct fields will cause an error.
func WithStrictMode() LoaderOption {
	return func(o *loaderOptions) {
		o.strict = true
	}
}

// WithTagName sets the struct tag used for field mapping.
// Default is "mapstructure". Other common values: "yaml", "json", "toml".
func WithTagName(tag string) LoaderOption {
	return func(o *loaderOptions) {
		if tag != "" {
			o.tagName = tag
		}
	}
}

// WithDecodeHook adds a custom decode hook function for mapstructure.
// This allows custom type conversions during unmarshaling.
func WithDecodeHook(fn mapstructure.DecodeHookFunc) LoaderOption {
	return func(o *loaderOptions) {
		o.decoderFns = append(o.decoderFns, fn)
	}
}

// WithLogger sets an optional logger for observability.
// When set, the Loader and Store log warnings for recoverable issues
// such as values that cannot be encoded.
func WithLogger(logger *slog.Logger) LoaderOption {
	return func(o *loaderOptions) {
		o.logger = logger
	}
}

// StoreOption configures a file-backed Store.
type StoreOption func(*storeOptions)

type storeOptions struct {
	keySeparator     string // separator for flattened keys (default: "/")
	defaultNamespace string // namespace for top-level scalars (default: "default")
	loaderOpts       []LoaderOption
	writable         bool
	sidecarSuffix    string        // default ".writes.yaml"
	watchInterval    time.Duration // default 2s
	watchBufSize     int           // default 100
	expander         ExpanderFunc  // nil means no ${VAR} expansion
	angleExpander    ExpanderFunc  // nil means no <VAR> expansion
	onDropped        func(event config.ChangeEvent) // optional callback when a watch event is dropped
}

// WithKeySeparator sets the separator used for flattened nested keys.
// Default is "/". For example, with "/", the YAML key path db.pool.max_size
// becomes the config key "pool/max_size" under namespace "db".
func WithKeySeparator(sep string) StoreOption {
	return func(o *storeOptions) {
		if sep != "" {
			o.keySeparator = sep
		}
	}
}

// WithDefaultNamespace sets the namespace for top-level scalar values
// that are not under a section. Default is "default".
func WithDefaultNamespace(ns string) StoreOption {
	return func(o *storeOptions) {
		o.defaultNamespace = ns
	}
}

// WithLoaderOptions passes options through to the underlying Loader.
func WithLoaderOptions(opts ...LoaderOption) StoreOption {
	return func(o *storeOptions) {
		o.loaderOpts = append(o.loaderOpts, opts...)
	}
}

// WithWritable enables sidecar-based write support and Watch.
// Writes are persisted to a companion file (default: "{path}.writes.yaml").
func WithWritable() StoreOption {
	return func(o *storeOptions) {
		o.writable = true
	}
}

// WithSidecarSuffix overrides the sidecar file suffix.
// Default is ".writes.yaml".
func WithSidecarSuffix(s string) StoreOption {
	return func(o *storeOptions) {
		if s != "" {
			o.sidecarSuffix = s
		}
	}
}

// WithWatchInterval sets the polling interval for file change detection.
// Default is 2s.
func WithWatchInterval(d time.Duration) StoreOption {
	return func(o *storeOptions) {
		if d > 0 {
			o.watchInterval = d
		}
	}
}

// WithWatchBufferSize sets the buffer size for watch event channels.
// Default is 100.
func WithWatchBufferSize(n int) StoreOption {
	return func(o *storeOptions) {
		if n > 0 {
			o.watchBufSize = n
		}
	}
}

// WithOnDropped sets a callback invoked when a watch event is dropped because
// a subscriber's channel buffer is full. Use it for logging or metrics. The
// callback runs synchronously on the notify path, so it must be fast and must
// not block. The dropped-event count is also available via Store.DroppedEvents.
func WithOnDropped(fn func(event config.ChangeEvent)) StoreOption {
	return func(o *storeOptions) {
		o.onDropped = fn
	}
}

// WithExpansion enables ${VAR} and ${VAR:-default} substitution in string
// values at file-load time using fn. Multiple calls chain the expanders: each
// is tried in order and the first match wins.
//
// Expansion is single-pass (results are not re-scanned) and only applies to
// scalar string values; nested strings inside lists or sub-maps are not
// expanded. For query-time expansion that works with any backend, use
// expand.NewStore instead.
//
// Example with environment variables:
//
//	file.NewStore("config.yaml", file.WithExpansion(file.EnvExpander()))
func WithExpansion(fn ExpanderFunc) StoreOption {
	return func(o *storeOptions) {
		if o.expander == nil {
			o.expander = fn
		} else {
			o.expander = ChainExpanders(o.expander, fn)
		}
	}
}

// WithAngleBracketExpander enables <VAR> placeholder substitution in string
// values at file-load time using fn. Multiple calls chain the expanders.
//
// Only identifiers are matched (letters, digits, underscores, hyphens), so
// HTML tags and comparison operators are not affected. Use \<VAR> to write a
// literal <VAR> without substitution.
//
// Example:
//
//	file.NewStore("config.yaml", file.WithAngleBracketExpander(secretsFn))
func WithAngleBracketExpander(fn ExpanderFunc) StoreOption {
	return func(o *storeOptions) {
		if o.angleExpander == nil {
			o.angleExpander = fn
		} else {
			o.angleExpander = ChainExpanders(o.angleExpander, fn)
		}
	}
}
