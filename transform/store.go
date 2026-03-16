package transform

import (
	"context"
	"fmt"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/codec"
)

// Store is the interface returned by WrapStore. It extends config.Store with
// Unwrap to allow callers to retrieve the underlying store in a decorator chain.
type Store interface {
	config.Store
	Unwrap() config.Store
}

// transformStore wraps a config.Store and applies a codec.Transformer to all
// values at the Set/Get boundary.
type transformStore struct {
	store       config.Store
	transformer codec.Transformer
	opts        options
}

// Compile-time interface checks.
var (
	_ config.Store         = (*transformStore)(nil)
	_ config.HealthChecker = (*transformStore)(nil)
	_ config.StatsProvider = (*transformStore)(nil)
	_ config.CodecValidator = (*transformStore)(nil)
)

// WrapStore creates a store decorator that applies transformer to all stored values.
// Transform is applied on Set (before writing) and Reverse on Get (after reading).
// The returned Store includes an Unwrap method for accessing the underlying store.
func WrapStore(store config.Store, transformer codec.Transformer, opts ...Option) (Store, error) {
	if store == nil {
		return nil, fmt.Errorf("transform: store must not be nil")
	}
	if transformer == nil {
		return nil, fmt.Errorf("transform: transformer must not be nil")
	}

	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}

	return &transformStore{
		store:       store,
		transformer: transformer,
		opts:        o,
	}, nil
}

// Unwrap returns the underlying store.
func (s *transformStore) Unwrap() config.Store {
	return s.store
}

// Connect delegates to the inner store.
func (s *transformStore) Connect(ctx context.Context) error {
	return s.store.Connect(ctx)
}

// Close delegates to the inner store.
func (s *transformStore) Close(ctx context.Context) error {
	return s.store.Close(ctx)
}

// Get retrieves a value and reverses the transformation.
func (s *transformStore) Get(ctx context.Context, namespace, key string) (config.Value, error) {
	v, err := s.store.Get(ctx, namespace, key)
	if err != nil {
		return nil, err
	}
	return s.reverseValue(v)
}

// Set transforms the value before storing, then reverses the returned result.
func (s *transformStore) Set(ctx context.Context, namespace, key string, value config.Value) (config.Value, error) {
	tv, err := s.transformValue(value)
	if err != nil {
		return nil, fmt.Errorf("transform: set transform: %w", err)
	}
	result, err := s.store.Set(ctx, namespace, key, tv)
	if err != nil {
		return nil, err
	}
	return s.reverseValue(result)
}

// Delete delegates to the inner store.
func (s *transformStore) Delete(ctx context.Context, namespace, key string) error {
	return s.store.Delete(ctx, namespace, key)
}

// Find retrieves a page and reverses each value.
func (s *transformStore) Find(ctx context.Context, namespace string, filter config.Filter) (config.Page, error) {
	page, err := s.store.Find(ctx, namespace, filter)
	if err != nil {
		return nil, err
	}

	results := page.Results()
	reversed := make(map[string]config.Value, len(results))
	for k, v := range results {
		rv, err := s.reverseValue(v)
		if err != nil {
			return nil, fmt.Errorf("transform: find reverse key %q: %w", k, err)
		}
		reversed[k] = rv
	}

	return config.NewPage(reversed, page.NextCursor(), page.Limit()), nil
}

// Watch returns a channel that receives change events with reversed values.
func (s *transformStore) Watch(ctx context.Context, filter config.WatchFilter) (<-chan config.ChangeEvent, error) {
	innerCh, err := s.store.Watch(ctx, filter)
	if err != nil {
		return nil, err
	}
	if innerCh == nil {
		return nil, nil
	}

	outCh := make(chan config.ChangeEvent, s.opts.watchBufferSize)
	go func() {
		defer close(outCh)
		for {
			select {
			case <-ctx.Done():
				return
			case evt, ok := <-innerCh:
				if !ok {
					return
				}
				if evt.Value != nil {
					rv, err := s.reverseValue(evt.Value)
					if err != nil {
						// Skip events that fail to reverse — watch is for
						// cache invalidation so we don't crash the loop.
						continue
					}
					evt.Value = rv
				}
				select {
				case outCh <- evt:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return outCh, nil
}

// Health delegates to the inner store if it implements HealthChecker.
func (s *transformStore) Health(ctx context.Context) error {
	if checker, ok := s.store.(config.HealthChecker); ok {
		return checker.Health(ctx)
	}
	return nil
}

// Stats delegates to the inner store if it implements StatsProvider.
func (s *transformStore) Stats(ctx context.Context) (*config.StoreStats, error) {
	if provider, ok := s.store.(config.StatsProvider); ok {
		return provider.Stats(ctx)
	}
	return nil, nil
}

// SupportsCodec delegates to the inner store if it implements CodecValidator.
func (s *transformStore) SupportsCodec(codecName string) bool {
	if cv, ok := s.store.(config.CodecValidator); ok {
		return cv.SupportsCodec(codecName)
	}
	return true
}

// GetMany retrieves multiple values and reverses each one.
func (s *transformStore) GetMany(ctx context.Context, namespace string, keys []string) (map[string]config.Value, error) {
	if bulk, ok := s.store.(config.BulkStore); ok {
		results, err := bulk.GetMany(ctx, namespace, keys)
		if err != nil {
			return nil, err
		}
		reversed := make(map[string]config.Value, len(results))
		for k, v := range results {
			rv, err := s.reverseValue(v)
			if err != nil {
				return nil, fmt.Errorf("transform: get_many reverse key %q: %w", k, err)
			}
			reversed[k] = rv
		}
		return reversed, nil
	}

	// Fallback to individual gets; non-NotFound errors are returned immediately.
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

// SetMany transforms each value before delegating to the inner store.
func (s *transformStore) SetMany(ctx context.Context, namespace string, values map[string]config.Value) error {
	transformed := make(map[string]config.Value, len(values))
	for k, v := range values {
		tv, err := s.transformValue(v)
		if err != nil {
			return fmt.Errorf("transform: set_many transform key %q: %w", k, err)
		}
		transformed[k] = tv
	}

	if bulk, ok := s.store.(config.BulkStore); ok {
		return bulk.SetMany(ctx, namespace, transformed)
	}

	// Fallback to individual sets.
	for key, val := range transformed {
		if _, err := s.store.Set(ctx, namespace, key, val); err != nil {
			return err
		}
	}
	return nil
}

// DeleteMany delegates to the inner store.
func (s *transformStore) DeleteMany(ctx context.Context, namespace string, keys []string) (int64, error) {
	if bulk, ok := s.store.(config.BulkStore); ok {
		return bulk.DeleteMany(ctx, namespace, keys)
	}

	// Fallback to individual deletes; non-NotFound errors are returned immediately.
	var deleted int64
	for _, key := range keys {
		if err := s.store.Delete(ctx, namespace, key); err == nil {
			deleted++
		} else if !config.IsNotFound(err) {
			return deleted, err
		}
	}
	return deleted, nil
}

// transformValue applies the forward transformation for Set operations.
func (s *transformStore) transformValue(value config.Value) (config.Value, error) {
	data, err := value.Marshal()
	if err != nil {
		return nil, fmt.Errorf("marshal: %w", err)
	}

	transformed, err := s.transformer.Transform(data)
	if err != nil {
		return nil, fmt.Errorf("transform: %w", err)
	}

	opts := valueOptions(value)
	return config.NewRawValue(transformed, value.Codec(), opts...), nil
}

// reverseValue applies the reverse transformation for Get operations.
func (s *transformStore) reverseValue(value config.Value) (config.Value, error) {
	data, err := value.Marshal()
	if err != nil {
		return nil, fmt.Errorf("marshal: %w", err)
	}

	reversed, err := s.transformer.Reverse(data)
	if err != nil {
		return nil, fmt.Errorf("reverse: %w", err)
	}

	opts := valueOptions(value)
	rv, err := config.NewValueFromBytes(reversed, value.Codec(), opts...)
	if err != nil {
		return nil, fmt.Errorf("new value from bytes: %w", err)
	}
	return rv, nil
}

// valueOptions extracts all metadata from a value to preserve across transformations.
func valueOptions(v config.Value) []config.ValueOption {
	var opts []config.ValueOption

	if m := v.Metadata(); m != nil {
		opts = append(opts, config.WithValueMetadata(m.Version(), m.CreatedAt(), m.UpdatedAt()))
		if m.IsStale() {
			opts = append(opts, config.WithValueStale(true))
		}
		// EntryID is on storeMetadata (unexported interface) but valueMetadata
		// exports the method, so we can reach it via a structural type assertion.
		if sm, ok := m.(interface{ EntryID() string }); ok {
			if id := sm.EntryID(); id != "" {
				opts = append(opts, config.WithValueEntryID(id))
			}
		}
	}

	if t := v.Type(); t != 0 {
		opts = append(opts, config.WithValueType(t))
	}

	if wm, ok := v.(config.WriteModer); ok {
		if mode := wm.WriteMode(); mode != 0 {
			opts = append(opts, config.WithValueWriteMode(mode))
		}
	}

	return opts
}
