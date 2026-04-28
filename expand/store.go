package expand

import (
	"context"
	"fmt"

	"github.com/rbaliyan/config"
)

// Option configures an expand Store.
type Option func(*options)

type options struct {
	dollar ExpanderFunc
	angle  ExpanderFunc
}

// WithDollarExpander enables ${VAR} and ${VAR:-default} substitution using fn.
// Multiple calls chain the expanders: each is tried in order, first match wins.
func WithDollarExpander(fn ExpanderFunc) Option {
	return func(o *options) {
		if o.dollar == nil {
			o.dollar = fn
		} else {
			o.dollar = ChainExpanders(o.dollar, fn)
		}
	}
}

// WithAngleExpander enables <VAR> substitution using fn.
// Multiple calls chain the expanders: each is tried in order, first match wins.
func WithAngleExpander(fn ExpanderFunc) Option {
	return func(o *options) {
		if o.angle == nil {
			o.angle = fn
		} else {
			o.angle = ChainExpanders(o.angle, fn)
		}
	}
}

// Store wraps a config.Store and expands placeholder tokens in string values
// at query time. Expansion is applied on Get and Find (read path); Set,
// Delete, Watch, Connect, and Close delegate to the inner store unchanged.
//
// Because expansion runs on every read, changes to the underlying expander
// source (env vars, a secrets manager, etc.) are reflected immediately
// without reloading the store.
//
// Only TypeString values are expanded. Other types (int, bool, maps, lists)
// are returned as-is.
type Store struct {
	inner config.Store
	opts  options
}

// Compile-time interface checks.
var (
	_ config.Store         = (*Store)(nil)
	_ config.HealthChecker = (*Store)(nil)
	_ config.StatsProvider = (*Store)(nil)
	_ config.BulkStore     = (*Store)(nil)
	_ config.AliasStore    = (*Store)(nil)
)

// NewStore wraps inner with query-time placeholder expansion.
// Returns an error if inner is nil or no expander option is provided.
func NewStore(inner config.Store, opts ...Option) (*Store, error) {
	if inner == nil {
		return nil, fmt.Errorf("expand: inner store must not be nil")
	}
	o := options{}
	for _, opt := range opts {
		opt(&o)
	}
	if o.dollar == nil && o.angle == nil {
		return nil, fmt.Errorf("expand: at least one of WithDollarExpander or WithAngleExpander must be set")
	}
	return &Store{inner: inner, opts: o}, nil
}

// Unwrap returns the underlying store.
func (s *Store) Unwrap() config.Store { return s.inner }

// Connect delegates to the inner store.
func (s *Store) Connect(ctx context.Context) error { return s.inner.Connect(ctx) }

// Close delegates to the inner store.
func (s *Store) Close(ctx context.Context) error { return s.inner.Close(ctx) }

// Get retrieves a value and expands any placeholder tokens in string values.
func (s *Store) Get(ctx context.Context, namespace, key string) (config.Value, error) {
	v, err := s.inner.Get(ctx, namespace, key)
	if err != nil {
		return nil, err
	}
	return s.expandValue(v), nil
}

// Set delegates to the inner store unchanged. Values are stored as-is;
// placeholder tokens in the raw string are not expanded on write.
func (s *Store) Set(ctx context.Context, namespace, key string, value config.Value) (config.Value, error) {
	return s.inner.Set(ctx, namespace, key, value)
}

// Delete delegates to the inner store.
func (s *Store) Delete(ctx context.Context, namespace, key string) error {
	return s.inner.Delete(ctx, namespace, key)
}

// Find returns entries matching the filter, with string values expanded.
func (s *Store) Find(ctx context.Context, namespace string, filter config.Filter) (config.Page, error) {
	page, err := s.inner.Find(ctx, namespace, filter)
	if err != nil {
		return nil, err
	}
	results := page.Results()
	expanded := make(map[string]config.Value, len(results))
	for k, v := range results {
		expanded[k] = s.expandValue(v)
	}
	return config.NewPage(expanded, page.NextCursor(), page.Limit()), nil
}

// Watch delegates to the inner store. Events carry raw (unexpanded) values;
// callers should call Get to obtain the expanded value after receiving an event.
func (s *Store) Watch(ctx context.Context, filter config.WatchFilter) (<-chan config.ChangeEvent, error) {
	return s.inner.Watch(ctx, filter)
}

// Health delegates to the inner store.
func (s *Store) Health(ctx context.Context) error {
	if hc, ok := s.inner.(config.HealthChecker); ok {
		return hc.Health(ctx)
	}
	return nil
}

// Stats delegates to the inner store.
func (s *Store) Stats(ctx context.Context) (*config.StoreStats, error) {
	if sp, ok := s.inner.(config.StatsProvider); ok {
		return sp.Stats(ctx)
	}
	return &config.StoreStats{}, nil
}

// expandValue applies configured expansions to v if it is a TypeString value.
// Non-string values are returned unchanged. The stale flag is preserved so
// callers can still detect degraded-cache reads after expansion.
func (s *Store) expandValue(v config.Value) config.Value {
	if v == nil || v.Type() != config.TypeString {
		return v
	}
	str, err := v.String()
	if err != nil {
		return v
	}
	expanded := s.apply(str)
	if expanded == str {
		return v
	}
	meta := v.Metadata()
	result := config.NewValue(expanded,
		config.WithValueType(config.TypeString),
		config.WithValueMetadata(meta.Version(), meta.CreatedAt(), meta.UpdatedAt()),
	)
	if meta.IsStale() {
		return config.MarkStale(result)
	}
	return result
}

// apply runs all configured expansions on s.
func (s *Store) apply(str string) string {
	if s.opts.dollar != nil {
		str = Dollar(str, s.opts.dollar)
	}
	if s.opts.angle != nil {
		str = Angle(str, s.opts.angle)
	}
	return str
}

// GetMany retrieves multiple values and expands string values. Delegates to the
// inner BulkStore when available; otherwise falls back to individual Gets.
func (s *Store) GetMany(ctx context.Context, namespace string, keys []string) (map[string]config.Value, error) {
	if bulk, ok := s.inner.(config.BulkStore); ok {
		results, err := bulk.GetMany(ctx, namespace, keys)
		if err != nil {
			return nil, err
		}
		expanded := make(map[string]config.Value, len(results))
		for k, v := range results {
			expanded[k] = s.expandValue(v)
		}
		return expanded, nil
	}
	results := make(map[string]config.Value, len(keys))
	for _, key := range keys {
		v, err := s.Get(ctx, namespace, key)
		if err == nil {
			results[key] = v
		} else if !config.IsNotFound(err) {
			return nil, err
		}
	}
	return results, nil
}

// SetMany delegates to the inner store unchanged. Values are not expanded on write.
// The fallback path strips WriteMode hints to preserve the upsert-only contract
// documented on config.BulkStore.
func (s *Store) SetMany(ctx context.Context, namespace string, values map[string]config.Value) error {
	if bulk, ok := s.inner.(config.BulkStore); ok {
		return bulk.SetMany(ctx, namespace, values)
	}
	for key, val := range values {
		// BulkStore.SetMany is always upsert; strip any conditional write hint.
		if config.GetWriteMode(val) != config.WriteModeUpsert {
			val = upsertValue{val}
		}
		if _, err := s.inner.Set(ctx, namespace, key, val); err != nil {
			return err
		}
	}
	return nil
}

// upsertValue wraps a Value to override its WriteMode to Upsert, satisfying
// the BulkStore.SetMany contract in the non-bulk fallback path.
type upsertValue struct{ config.Value }

func (upsertValue) WriteMode() config.WriteMode { return config.WriteModeUpsert }

// DeleteMany delegates to the inner store.
func (s *Store) DeleteMany(ctx context.Context, namespace string, keys []string) (int64, error) {
	if bulk, ok := s.inner.(config.BulkStore); ok {
		return bulk.DeleteMany(ctx, namespace, keys)
	}
	var deleted int64
	for _, key := range keys {
		if err := s.inner.Delete(ctx, namespace, key); err == nil {
			deleted++
		} else if !config.IsNotFound(err) {
			return deleted, err
		}
	}
	return deleted, nil
}

// SetAlias delegates to the inner AliasStore, if supported.
func (s *Store) SetAlias(ctx context.Context, alias, target string) (config.Value, error) {
	if as, ok := s.inner.(config.AliasStore); ok {
		return as.SetAlias(ctx, alias, target)
	}
	return nil, fmt.Errorf("expand: inner store does not support aliases")
}

// DeleteAlias delegates to the inner AliasStore, if supported.
func (s *Store) DeleteAlias(ctx context.Context, alias string) error {
	if as, ok := s.inner.(config.AliasStore); ok {
		return as.DeleteAlias(ctx, alias)
	}
	return fmt.Errorf("expand: inner store does not support aliases")
}

// GetAlias retrieves the alias target from the inner AliasStore, if supported.
func (s *Store) GetAlias(ctx context.Context, alias string) (config.Value, error) {
	if as, ok := s.inner.(config.AliasStore); ok {
		return as.GetAlias(ctx, alias)
	}
	return nil, fmt.Errorf("expand: inner store does not support aliases")
}

// ListAliases returns all aliases from the inner AliasStore, if supported.
func (s *Store) ListAliases(ctx context.Context) (map[string]config.Value, error) {
	if as, ok := s.inner.(config.AliasStore); ok {
		return as.ListAliases(ctx)
	}
	return nil, fmt.Errorf("expand: inner store does not support aliases")
}
