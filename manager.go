package config

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rbaliyan/config/codec"
)

// Manager manages configuration across namespaces.
//
// The Manager wraps a Store with caching and automatic cache invalidation.
// It provides access to namespaced configuration via the Namespace method.
//
// Example:
//
//	// Create manager
//	mgr := config.New(
//	    config.WithStore(memory.NewStore()),
//	)
//
//	// Connect to backend
//	if err := mgr.Connect(ctx); err != nil {
//	    return err
//	}
//	defer mgr.Close(ctx)
//
//	// Get configuration for a namespace (use "" for default)
//	prodConfig := mgr.Namespace("production")
//
//	// Use Reader interface in application code
//	val, err := prodConfig.Get(ctx, "app/database/timeout")
//	if err != nil {
//	    return err
//	}
//	var timeout int
//	if err := val.Unmarshal(&timeout); err != nil {
//	    return err
//	}
//
//	// Use Writer interface for management
//	if err := prodConfig.Set(ctx, "app/database/timeout", 30); err != nil {
//	    return err
//	}
// CacheStats contains cache statistics.
type CacheStats struct {
	Hits   int64 // Number of cache hits
	Misses int64 // Number of cache misses
	Size   int64 // Number of cached entries
}

type Manager interface {
	// Connect establishes connection to the backend and starts watching.
	// Must be called before any other operations.
	Connect(ctx context.Context) error

	// Close stops watching and releases resources.
	Close(ctx context.Context) error

	// Namespace returns a Config for the specified namespace.
	// Use "" for the default namespace.
	Namespace(name string) Config

	// Refresh forces a cache refresh for a specific key.
	Refresh(ctx context.Context, namespace, key string) error

	// RefreshAll forces a refresh of all cached entries.
	RefreshAll(ctx context.Context) error

	// Health performs a health check on the manager and underlying store.
	// Returns nil if healthy, or an error describing the issue.
	Health(ctx context.Context) error

	// CacheStats returns cache statistics for observability.
	CacheStats() CacheStats
}

// manager is the default Manager implementation.
type manager struct {
	status int32 // 0=created, 1=connected, 2=closed
	store  Store
	cache  cache // internal cache for resilience
	codec  codec.Codec
	logger *slog.Logger

	watchCancel context.CancelFunc
	watchWg     sync.WaitGroup

	// Circuit breaker for watch failures
	watchFailures   atomic.Int32  // consecutive failures
	circuitOpenTime atomic.Int64  // unix timestamp when circuit opened (0 = closed)

	// Config cache
	configMu sync.RWMutex
	configs  map[string]*config
}

// Compile-time interface check
var _ Manager = (*manager)(nil)

// New creates a new configuration Manager.
//
// The manager is created but not connected. Call Connect() before use.
// This follows the New/Connect split pattern for better error handling.
//
// The manager always maintains an internal cache for resilience. If the backend
// store becomes temporarily unavailable, cached values will continue to be served.
// This ensures your application keeps working during database outages.
func New(opts ...Option) *manager {
	o := newManagerOptions()
	for _, opt := range opts {
		opt(o)
	}

	m := &manager{
		status:  0,
		store:   o.store,
		codec:   o.codec,
		logger:  o.logger.With("component", "config"),
		configs: make(map[string]*config),
		// Always create internal cache for resilience
		cache: NewMemoryCache(WithMemoryCacheTTL(o.cacheTTL)),
	}

	return m
}

// Connect establishes connection to the backend and starts watching.
func (m *manager) Connect(ctx context.Context) error {
	if m.store == nil {
		return ErrStoreNotConnected
	}

	if !atomic.CompareAndSwapInt32(&m.status, 0, 1) {
		return ErrManagerClosed
	}

	// Connect to store
	if err := m.store.Connect(ctx); err != nil {
		atomic.StoreInt32(&m.status, 0)
		return err
	}

	// Start watching for changes if store supports it (for internal cache invalidation)
	// This uses the store's native change stream (MongoDB Change Streams, PostgreSQL LISTEN/NOTIFY)
	if ws, ok := m.store.(watchableStore); ok {
		watchCtx, cancel := context.WithCancel(context.Background())
		m.watchCancel = cancel

		m.watchWg.Add(1)
		go m.watchChanges(watchCtx, ws)
	}

	m.logger.Info("config manager connected")
	return nil
}

// Close stops watching and releases resources.
func (m *manager) Close(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&m.status, 1, 2) {
		return nil // Already closed or not connected
	}

	// Stop watching
	if m.watchCancel != nil {
		m.watchCancel()
	}
	m.watchWg.Wait()

	// Close store
	if m.store != nil {
		if err := m.store.Close(ctx); err != nil {
			m.logger.Error("failed to close store", "error", err)
		}
	}

	// Flush and close cache
	if m.cache != nil {
		if err := m.cache.Flush(ctx); err != nil {
			m.logger.Error("failed to flush cache", "error", err)
		}
		// Close cache if it implements io.Closer (to stop cleanup goroutine)
		if closer, ok := m.cache.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil {
				m.logger.Error("failed to close cache", "error", err)
			}
		}
	}

	m.logger.Info("config manager closed")
	return nil
}

func (m *manager) isConnected() bool {
	return atomic.LoadInt32(&m.status) == 1
}

// Namespace returns a Config for the specified namespace.
// Use "" for the default namespace.
func (m *manager) Namespace(name string) Config {
	m.configMu.RLock()
	cfg, ok := m.configs[name]
	m.configMu.RUnlock()

	if ok {
		return cfg
	}

	// Create new config for namespace
	m.configMu.Lock()
	defer m.configMu.Unlock()

	// Double-check after acquiring write lock
	if cfg, ok = m.configs[name]; ok {
		return cfg
	}

	cfg = &config{
		namespace: name,
		manager:   m,
	}
	m.configs[name] = cfg

	return cfg
}

// Refresh forces a cache refresh for a specific key.
// It fetches the latest value from the store and updates the cache.
func (m *manager) Refresh(ctx context.Context, namespace, key string) error {
	if !m.isConnected() {
		return ErrManagerClosed
	}

	// Fetch fresh data from store
	value, err := m.store.Get(ctx, namespace, key)
	if err != nil {
		// If key not found, remove from cache
		if IsNotFound(err) && m.cache != nil {
			_ = m.cache.Delete(ctx, namespace, key)
		}
		return err
	}

	// Update cache with fresh data
	if m.cache != nil {
		if err := m.cache.Set(ctx, namespace, key, value); err != nil {
			m.logger.Warn("failed to update cache during refresh", "key", key, "error", err)
		}
	}

	return nil
}

// RefreshAll forces a refresh of all cached entries.
func (m *manager) RefreshAll(ctx context.Context) error {
	if !m.isConnected() {
		return ErrManagerClosed
	}

	if m.cache != nil {
		return m.cache.Flush(ctx)
	}
	return nil
}

// Health performs a health check on the manager and underlying store.
func (m *manager) Health(ctx context.Context) error {
	if !m.isConnected() {
		return ErrManagerClosed
	}

	// Check if store supports health checks
	if hc, ok := m.store.(HealthChecker); ok {
		return hc.Health(ctx)
	}

	// No health checker available, assume healthy if connected
	return nil
}

// CacheStats returns cache statistics for observability.
func (m *manager) CacheStats() CacheStats {
	if cs, ok := m.cache.(cacheStats); ok {
		return CacheStats{
			Hits:   cs.Hits(),
			Misses: cs.Misses(),
			Size:   cs.Size(),
		}
	}
	return CacheStats{}
}

// Circuit breaker constants for watch failures
const (
	circuitBreakerThreshold = 5               // consecutive failures before opening circuit
	circuitBreakerTimeout   = 5 * time.Minute // how long circuit stays open
)

// watchChanges handles cache invalidation based on store's change stream.
// The store provides the change stream (e.g., MongoDB change streams, PostgreSQL LISTEN/NOTIFY).
// It implements retry with exponential backoff and circuit breaker for resilience.
func (m *manager) watchChanges(ctx context.Context, ws watchableStore) {
	defer m.watchWg.Done()

	const (
		initialBackoff = 100 * time.Millisecond
		maxBackoff     = 30 * time.Second
		backoffFactor  = 2
	)

	backoff := initialBackoff

	for {
		// Check if context is cancelled before attempting to watch
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Check circuit breaker state
		if m.isCircuitOpen() {
			m.logger.Warn("watch circuit breaker is open, waiting before retry",
				"timeout", circuitBreakerTimeout)

			select {
			case <-ctx.Done():
				return
			case <-time.After(circuitBreakerTimeout):
				// Reset circuit breaker after timeout
				m.resetCircuitBreaker()
				m.logger.Info("watch circuit breaker reset, attempting reconnect")
			}
		}

		changes, err := ws.Watch(ctx, WatchFilter{})
		if err != nil {
			if err == ErrWatchNotSupported {
				// Store doesn't support watching, exit without retry
				return
			}

			failures := m.watchFailures.Add(1)
			m.logger.Error("failed to start watching",
				"error", err,
				"backoff", backoff,
				"consecutive_failures", failures)

			// Open circuit breaker if too many failures
			if failures >= circuitBreakerThreshold {
				m.openCircuitBreaker()
				m.logger.Error("watch circuit breaker opened due to consecutive failures",
					"failures", failures,
					"threshold", circuitBreakerThreshold)
			}

			// Wait with backoff before retrying
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}

			// Increase backoff for next retry (with cap)
			backoff = min(time.Duration(float64(backoff)*backoffFactor), maxBackoff)
			continue
		}

		// Successfully connected, reset failures and backoff
		m.watchFailures.Store(0)
		backoff = initialBackoff
		m.logger.Debug("watch started successfully")

		// Process changes until channel closes or context cancelled
		channelClosed := m.processWatchEvents(ctx, changes)

		if !channelClosed {
			// Context was cancelled, exit
			return
		}

		// Channel closed unexpectedly, log and retry
		m.logger.Warn("watch channel closed, will reconnect", "backoff", backoff)

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}

		backoff = min(time.Duration(float64(backoff)*backoffFactor), maxBackoff)
	}
}

// isCircuitOpen returns true if the circuit breaker is currently open.
func (m *manager) isCircuitOpen() bool {
	openTime := m.circuitOpenTime.Load()
	if openTime == 0 {
		return false
	}
	// Circuit is open, check if timeout has elapsed
	elapsed := time.Since(time.Unix(openTime, 0))
	return elapsed < circuitBreakerTimeout
}

// openCircuitBreaker opens the circuit breaker.
func (m *manager) openCircuitBreaker() {
	m.circuitOpenTime.Store(time.Now().Unix())
}

// resetCircuitBreaker resets the circuit breaker to closed state.
func (m *manager) resetCircuitBreaker() {
	m.circuitOpenTime.Store(0)
	m.watchFailures.Store(0)
}

// processWatchEvents handles incoming change events.
// Returns true if the channel was closed, false if context was cancelled.
func (m *manager) processWatchEvents(ctx context.Context, changes <-chan ChangeEvent) bool {
	for {
		select {
		case <-ctx.Done():
			return false
		case change, ok := <-changes:
			if !ok {
				return true // Channel closed
			}
			m.handleChange(ctx, change)
		}
	}
}

// handleChange processes a change event and updates the cache.
func (m *manager) handleChange(ctx context.Context, change ChangeEvent) {
	// Check if manager is still connected (avoid race with Close())
	if !m.isConnected() {
		return
	}

	if m.cache == nil {
		return
	}

	// Build cache key with tags (without namespace - cache methods take it separately)
	ck := cacheKey(change.Key, change.Tags)

	switch change.Type {
	case ChangeTypeSet:
		if change.Value != nil {
			if err := m.cache.Set(ctx, change.Namespace, ck, change.Value); err != nil {
				// Only log if still connected (avoid spurious errors during shutdown)
				if m.isConnected() {
					m.logger.Warn("failed to update cache", "namespace", change.Namespace, "key", change.Key, "error", err)
				}
			}
		}
	case ChangeTypeDelete:
		if err := m.cache.Delete(ctx, change.Namespace, ck); err != nil {
			// Only log if still connected (avoid spurious errors during shutdown)
			if m.isConnected() {
				m.logger.Warn("failed to invalidate cache", "namespace", change.Namespace, "key", change.Key, "error", err)
			}
		}
	}
}
