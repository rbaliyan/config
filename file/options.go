// Package file provides file-based configuration loading and a read-only
// config.Store backed by YAML, TOML, or JSON files.
//
// It provides two main types:
//   - Loader: register named config structs and unmarshal file sections into them (replaces go-conf/viper)
//   - Store: implements config.Store with file contents, mapping top-level keys to namespaces
package file

import (
	"log/slog"

	"github.com/go-viper/mapstructure/v2"
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
