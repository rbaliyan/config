// Package multi provides a multi-store wrapper that supports fallback patterns.
// This allows combining multiple stores where if one fails, the next is tried.
//
// # Consistency Model
//
// All strategies write to ALL stores to maintain consistency across replicas.
// A write operation succeeds if at least one store accepts it, allowing the
// system to remain available even when some stores are temporarily unavailable.
//
// For Delete operations, NotFound errors from individual stores are ignored
// since the key may not exist in all stores (eventual consistency).
//
// # Observability
//
// Multi-store implements HealthChecker and StatsProvider interfaces when the
// underlying stores support them. Health returns success if at least one store
// is healthy. Stats aggregates metrics from all stores.
package multi

import (
	"context"
	"errors"

	"github.com/rbaliyan/config"
)

// Strategy defines how the multi-store handles reads and writes.
//
// Consistency Guarantee: All strategies write to ALL stores to maintain consistency.
// At least one store must succeed for a write to be considered successful.
// Individual store failures during writes are tolerated as long as one succeeds.
type Strategy int

const (
	// StrategyFallback reads from the first available store, writes to all stores.
	// Read: Returns first successful result.
	// Write: Writes to all stores (at least one must succeed).
	// Best for: Primary + backup scenarios.
	StrategyFallback Strategy = iota

	// StrategyReadThrough reads from stores in order, populating earlier stores on cache miss.
	// Read: Tries each store in order; on hit from store N, populates stores 0..N-1.
	// Write: Writes to all stores (at least one must succeed).
	// Best for: Cache + backend scenarios where cache is first.
	StrategyReadThrough

	// StrategyWriteThrough writes to all stores, reads from first available.
	// Read: Returns first successful result.
	// Write: Writes to all stores (at least one must succeed).
	// Best for: Keeping multiple stores in sync.
	StrategyWriteThrough
)

// Store wraps multiple stores with configurable fallback behavior.
// The stores slice is immutable after construction; each underlying store
// provides its own concurrency protection.
type Store struct {
	stores   []config.Store
	strategy Strategy
}

// Option configures the multi-store.
type Option func(*Store)

// WithStrategy sets the read/write strategy.
func WithStrategy(s Strategy) Option {
	return func(ms *Store) {
		ms.strategy = s
	}
}

// NewStore creates a multi-store from the given stores with optional configuration.
// The first store is considered the primary.
//
// Example:
//
//	// Cache + backend pattern
//	cacheStore := memory.NewStore()
//	backendStore := mongodb.NewStore(client, "config", "entries")
//	store := multi.NewStore(
//	    []config.Store{cacheStore, backendStore},
//	    multi.WithStrategy(multi.StrategyReadThrough),
//	)
//
//	// Primary + backup pattern
//	primaryStore := postgres.NewStore(primaryDB)
//	backupStore := postgres.NewStore(backupDB)
//	store := multi.NewStore(
//	    []config.Store{primaryStore, backupStore},
//	    multi.WithStrategy(multi.StrategyFallback),
//	)
func NewStore(stores []config.Store, opts ...Option) *Store {
	ms := &Store{
		stores:   stores,
		strategy: StrategyFallback,
	}
	for _, opt := range opts {
		opt(ms)
	}
	return ms
}

// Compile-time interface check.
var _ config.Store = (*Store)(nil)

// Connect connects all underlying stores.
func (ms *Store) Connect(ctx context.Context) error {
	var errs []error
	for _, s := range ms.stores {
		if err := s.Connect(ctx); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) == len(ms.stores) {
		return errors.Join(errs...)
	}
	return nil
}

// Close closes all underlying stores.
func (ms *Store) Close(ctx context.Context) error {
	var errs []error
	for _, s := range ms.stores {
		if err := s.Close(ctx); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// Get retrieves a value, trying stores in order until one succeeds.
func (ms *Store) Get(ctx context.Context, namespace, key string) (config.Value, error) {
	var lastErr error
	for i, s := range ms.stores {
		val, err := s.Get(ctx, namespace, key)
		if err == nil {
			// For read-through strategy, populate earlier stores with the found value
			if ms.strategy == StrategyReadThrough && i > 0 {
				// Write back to earlier stores (cache population)
				for j := range i {
					_, _ = ms.stores[j].Set(ctx, namespace, key, val)
				}
			}
			return val, nil
		}
		lastErr = err

		// Only continue to next store on NotFound or store errors
		if !config.IsNotFound(err) && err != config.ErrStoreNotConnected {
			return nil, err
		}
	}

	return nil, lastErr
}

// Set creates or updates a value based on the strategy.
// Returns the stored Value with updated metadata from the first successful store.
func (ms *Store) Set(ctx context.Context, namespace, key string, value config.Value) (config.Value, error) {
	if len(ms.stores) == 0 {
		return nil, config.ErrStoreNotConnected
	}

	// All strategies write to all stores to maintain consistency.
	// For ReadThrough: ensures both cache and backend are updated.
	// For Fallback/WriteThrough: ensures all replicas are in sync.
	var errs []error
	var result config.Value
	for _, s := range ms.stores {
		if val, err := s.Set(ctx, namespace, key, value); err != nil {
			errs = append(errs, err)
		} else if result == nil {
			// Return value from first successful store
			result = val
		}
	}

	// At least one store must succeed
	if result == nil && len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return result, nil
}

// Delete removes a value from all stores.
func (ms *Store) Delete(ctx context.Context, namespace, key string) error {
	if len(ms.stores) == 0 {
		return config.ErrStoreNotConnected
	}

	// Delete from all stores to maintain consistency.
	// NotFound errors from individual stores are tracked separately since the key
	// may not exist in all stores (eventual consistency).
	var errs []error
	var succeeded bool
	var allNotFound = true
	for _, s := range ms.stores {
		if err := s.Delete(ctx, namespace, key); err != nil {
			if !config.IsNotFound(err) {
				errs = append(errs, err)
				allNotFound = false
			}
			// NotFound from this store is ok, key may not exist in all stores
		} else {
			succeeded = true
			allNotFound = false
		}
	}

	// If all stores returned NotFound, the key doesn't exist anywhere
	if allNotFound {
		return &config.KeyNotFoundError{Key: key, Namespace: namespace}
	}

	// At least one store must succeed or we return the accumulated errors
	if !succeeded && len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// Find searches for values in the primary store only.
//
// Note: Unlike Get which tries all stores, Find only queries the primary store.
// This is intentional to avoid duplicate results and ensure consistent pagination.
// For cache scenarios, the primary (cache) store may not have all entries;
// use Get for individual keys if you need fallback behavior.
func (ms *Store) Find(ctx context.Context, namespace string, filter config.Filter) (config.Page, error) {
	if len(ms.stores) == 0 {
		return nil, config.ErrStoreNotConnected
	}

	return ms.stores[0].Find(ctx, namespace, filter)
}

// Watch watches for changes in the first store that supports watching.
// This is an internal method used by the Manager for cache invalidation.
//
// Note: Unlike Get which tries all stores, Watch only monitors one store.
// This is intentional to avoid duplicate events. For cache + backend scenarios,
// you typically want to watch the backend (source of truth).
func (ms *Store) Watch(ctx context.Context, filter config.WatchFilter) (<-chan config.ChangeEvent, error) {
	if len(ms.stores) == 0 {
		return nil, config.ErrStoreNotConnected
	}

	// Try each store; skip those that don't support watching
	for _, s := range ms.stores {
		ch, err := s.Watch(ctx, filter)
		if err == nil {
			return ch, nil
		}
		if !errors.Is(err, config.ErrWatchNotSupported) {
			return nil, err
		}
	}

	return nil, config.ErrWatchNotSupported
}

// Primary returns the primary (first) store.
func (ms *Store) Primary() config.Store {
	if len(ms.stores) == 0 {
		return nil
	}
	return ms.stores[0]
}

// Stores returns all underlying stores.
func (ms *Store) Stores() []config.Store {
	stores := make([]config.Store, len(ms.stores))
	copy(stores, ms.stores)
	return stores
}

// Health checks the health of all underlying stores.
// Returns nil if at least one store is healthy (following fallback pattern).
// Returns an error only if all stores are unhealthy.
func (ms *Store) Health(ctx context.Context) error {
	if len(ms.stores) == 0 {
		return config.ErrStoreNotConnected
	}

	var errs []error
	for _, s := range ms.stores {
		if hc, ok := s.(config.HealthChecker); ok {
			if err := hc.Health(ctx); err != nil {
				errs = append(errs, err)
			} else {
				// At least one store is healthy
				return nil
			}
		}
	}

	// If no stores implement HealthChecker, assume healthy
	if len(errs) == 0 {
		return nil
	}

	// All stores that implement HealthChecker are unhealthy
	return errors.Join(errs...)
}

// Stats aggregates statistics from all underlying stores.
// Returns combined stats from all stores that implement StatsProvider.
func (ms *Store) Stats(ctx context.Context) (*config.StoreStats, error) {
	if len(ms.stores) == 0 {
		return nil, config.ErrStoreNotConnected
	}

	combined := &config.StoreStats{
		EntriesByType:      make(map[config.Type]int64),
		EntriesByNamespace: make(map[string]int64),
	}

	var hasStats bool
	for _, s := range ms.stores {
		if sp, ok := s.(config.StatsProvider); ok {
			stats, err := sp.Stats(ctx)
			if err != nil {
				continue
			}
			hasStats = true

			// Aggregate stats (use max of total entries since stores may have same data)
			if stats.TotalEntries > combined.TotalEntries {
				combined.TotalEntries = stats.TotalEntries
			}

			// Merge type counts (use max since stores may overlap)
			for t, count := range stats.EntriesByType {
				if count > combined.EntriesByType[t] {
					combined.EntriesByType[t] = count
				}
			}

			// Merge namespace counts (use max since stores may overlap)
			for ns, count := range stats.EntriesByNamespace {
				if count > combined.EntriesByNamespace[ns] {
					combined.EntriesByNamespace[ns] = count
				}
			}
		}
	}

	if !hasStats {
		return nil, nil
	}

	return combined, nil
}

// GetMany retrieves multiple values in a single operation.
// Tries stores in order, returning from the first that succeeds (same as Get).
func (ms *Store) GetMany(ctx context.Context, namespace string, keys []string) (map[string]config.Value, error) {
	if len(ms.stores) == 0 {
		return nil, config.ErrStoreNotConnected
	}

	var lastErr error
	for i, s := range ms.stores {
		var results map[string]config.Value
		if bulk, ok := s.(config.BulkStore); ok {
			var err error
			results, err = bulk.GetMany(ctx, namespace, keys)
			if err != nil {
				lastErr = err
				continue
			}
		} else {
			// Fallback to individual gets
			results = make(map[string]config.Value, len(keys))
			for _, key := range keys {
				val, err := s.Get(ctx, namespace, key)
				if err == nil {
					results[key] = val
				}
			}
		}

		// For read-through, populate earlier stores
		if ms.strategy == StrategyReadThrough && i > 0 && len(results) > 0 {
			for j := range i {
				if bulk, ok := ms.stores[j].(config.BulkStore); ok {
					_ = bulk.SetMany(ctx, namespace, results)
				} else {
					for key, val := range results {
						_, _ = ms.stores[j].Set(ctx, namespace, key, val)
					}
				}
			}
		}
		return results, nil
	}

	return nil, lastErr
}

// SetMany creates or updates multiple values across all stores.
func (ms *Store) SetMany(ctx context.Context, namespace string, values map[string]config.Value) error {
	if len(ms.stores) == 0 {
		return config.ErrStoreNotConnected
	}

	var errs []error
	var succeeded bool
	for _, s := range ms.stores {
		if bulk, ok := s.(config.BulkStore); ok {
			if err := bulk.SetMany(ctx, namespace, values); err != nil {
				errs = append(errs, err)
			} else {
				succeeded = true
			}
		} else {
			// Fallback to individual sets
			var storeErr error
			for key, val := range values {
				if _, err := s.Set(ctx, namespace, key, val); err != nil {
					storeErr = err
				}
			}
			if storeErr != nil {
				errs = append(errs, storeErr)
			} else {
				succeeded = true
			}
		}
	}

	if !succeeded && len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// DeleteMany removes multiple values from all stores.
func (ms *Store) DeleteMany(ctx context.Context, namespace string, keys []string) (int64, error) {
	if len(ms.stores) == 0 {
		return 0, config.ErrStoreNotConnected
	}

	var errs []error
	var maxDeleted int64
	var succeeded bool
	for _, s := range ms.stores {
		if bulk, ok := s.(config.BulkStore); ok {
			deleted, err := bulk.DeleteMany(ctx, namespace, keys)
			if err != nil {
				errs = append(errs, err)
			} else {
				succeeded = true
				if deleted > maxDeleted {
					maxDeleted = deleted
				}
			}
		} else {
			// Fallback to individual deletes
			var deleted int64
			for _, key := range keys {
				if err := s.Delete(ctx, namespace, key); err == nil {
					deleted++
				}
			}
			succeeded = true
			if deleted > maxDeleted {
				maxDeleted = deleted
			}
		}
	}

	if !succeeded && len(errs) > 0 {
		return 0, errors.Join(errs...)
	}
	return maxDeleted, nil
}

// Compile-time interface checks for optional interfaces.
var (
	_ config.HealthChecker = (*Store)(nil)
	_ config.StatsProvider = (*Store)(nil)
	_ config.BulkStore     = (*Store)(nil)
)
