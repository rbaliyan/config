package config

import (
	"context"
	"fmt"
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

	// Health performs a health check on the manager and underlying store.
	// Returns nil if healthy, or an error describing the issue.
	// Includes watch status - returns an error if watch has consecutive failures.
	Health(ctx context.Context) error

	// CacheStats returns statistics about the internal cache.
	// This can be used for monitoring cache effectiveness.
	CacheStats() CacheStats

	// WatchStatus returns the current status of the watch connection.
	// Use this for observability and monitoring.
	WatchStatus() WatchStatus
}

// WatchStatus provides observability into the watch connection state.
// Applications can use this to monitor watch health and implement
// their own alerting or circuit breaking logic if needed.
type WatchStatus struct {
	// Connected indicates if the manager is connected to the store.
	Connected bool `json:"connected"`

	// ConsecutiveFailures is the number of consecutive watch failures.
	// Resets to 0 when watch successfully connects.
	ConsecutiveFailures int32 `json:"consecutive_failures"`

	// LastError is the most recent watch error message (empty if no error).
	LastError string `json:"last_error,omitempty"`

	// LastAttempt is when the last watch connection was attempted.
	LastAttempt time.Time `json:"last_attempt,omitempty"`

	// Cache contains cache statistics for correlation with watch health.
	Cache CacheStats `json:"cache"`
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

	// Watch backoff configuration and status
	watchBackoff  WatchBackoffConfig
	watchFailures atomic.Int32          // consecutive failures for observability
	lastWatchErr  atomic.Pointer[string] // last watch error message (nil = no error)
	lastWatchTime atomic.Int64          // unix timestamp of last watch attempt

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
//
// Returns an error if cache initialization fails.
func New(opts ...Option) (Manager, error) {
	o := newManagerOptions()
	for _, opt := range opts {
		opt(o)
	}

	// Create internal cache for resilience (bounded, no expiration)
	cache, err := newMemoryCache(0) // Use default capacity
	if err != nil {
		return nil, fmt.Errorf("failed to initialize cache: %w", err)
	}

	m := &manager{
		status:       0,
		store:        o.store,
		codec:        o.codec,
		logger:       o.logger.With("component", "config"),
		watchBackoff: o.watchBackoff,
		configs:      make(map[string]*config),
		cache:        cache,
	}

	return m, nil
}

// Connect establishes connection to the backend and starts watching.
//
// The provided context is used only for the initial store connection.
// The watch goroutine runs independently with its own context until Close() is called.
// This is intentional: the watch should continue running even if the Connect context
// times out, as the watch is a long-running background operation.
func (m *manager) Connect(ctx context.Context) error {
	if m.store == nil {
		return ErrStoreNotConnected
	}

	if !atomic.CompareAndSwapInt32(&m.status, 0, 1) {
		return ErrManagerClosed
	}

	// Connect to store (uses caller's context for connection timeout)
	if err := m.store.Connect(ctx); err != nil {
		atomic.StoreInt32(&m.status, 0)
		return err
	}

	// Start watching for changes (for internal cache invalidation)
	// Uses independent context - watch should run until Close(), not until Connect's context expires
	// This uses the store's native change stream (MongoDB Change Streams, PostgreSQL LISTEN/NOTIFY)
	watchCtx, cancel := context.WithCancel(context.Background())
	m.watchCancel = cancel

	m.watchWg.Add(1)
	go m.watchChanges(watchCtx)

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

// Health performs a health check on the manager and underlying store.
// Returns an error if the manager is closed, the store is unhealthy,
// or if watch has experienced multiple consecutive failures.
func (m *manager) Health(ctx context.Context) error {
	if !m.isConnected() {
		return ErrManagerClosed
	}

	// Check watch status - report unhealthy if multiple consecutive failures
	failures := m.watchFailures.Load()
	if failures >= 3 {
		lastErr := ""
		if p := m.lastWatchErr.Load(); p != nil {
			lastErr = *p
		}
		return &WatchHealthError{ConsecutiveFailures: failures, LastError: lastErr}
	}

	// Check if store supports health checks
	if hc, ok := m.store.(HealthChecker); ok {
		return hc.Health(ctx)
	}

	// No health checker available, assume healthy if connected
	return nil
}

// CacheStats returns statistics about the internal cache.
func (m *manager) CacheStats() CacheStats {
	if m.cache != nil {
		return m.cache.Stats()
	}
	return CacheStats{}
}

// WatchStatus returns the current status of the watch connection.
func (m *manager) WatchStatus() WatchStatus {
	status := WatchStatus{
		Connected:           m.isConnected(),
		ConsecutiveFailures: m.watchFailures.Load(),
		Cache:               m.CacheStats(),
	}

	if p := m.lastWatchErr.Load(); p != nil {
		status.LastError = *p
	}

	if ts := m.lastWatchTime.Load(); ts > 0 {
		status.LastAttempt = time.Unix(ts, 0)
	}

	return status
}

// watchChanges handles cache invalidation based on store's change stream.
// The store provides the change stream (e.g., MongoDB change streams, PostgreSQL LISTEN/NOTIFY).
// Uses exponential backoff for reconnection - the internal cache provides resilience
// during backend unavailability, so aggressive circuit breaking is not needed.
func (m *manager) watchChanges(ctx context.Context) {
	defer m.watchWg.Done()

	cfg := m.watchBackoff
	backoff := cfg.InitialBackoff

	for {
		// Check if context is cancelled before attempting to watch
		select {
		case <-ctx.Done():
			return
		default:
		}

		m.lastWatchTime.Store(time.Now().Unix())
		changes, err := m.store.Watch(ctx, WatchFilter{})
		if err != nil {
			if err == ErrWatchNotSupported {
				// Store doesn't support watching, exit without retry
				m.logger.Debug("store does not support watching")
				return
			}

			failures := m.watchFailures.Add(1)
			errMsg := err.Error()
			m.lastWatchErr.Store(&errMsg)
			m.logger.Warn("failed to start watching",
				"error", err,
				"backoff", backoff,
				"consecutive_failures", failures)

			// Wait with backoff before retrying
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}

			// Increase backoff for next retry (with cap)
			backoff = min(time.Duration(float64(backoff)*cfg.BackoffFactor), cfg.MaxBackoff)
			continue
		}

		// Successfully connected, reset failures and backoff
		m.watchFailures.Store(0)
		m.lastWatchErr.Store(nil)
		backoff = cfg.InitialBackoff
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

		backoff = min(time.Duration(float64(backoff)*cfg.BackoffFactor), cfg.MaxBackoff)
	}
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
// This function is safe to call even during shutdown - cache operations
// are protected by the cache's internal lock, and we gracefully handle
// the case where the manager is closing.
func (m *manager) handleChange(ctx context.Context, change ChangeEvent) {
	// Use defer/recover to handle any panics during shutdown gracefully.
	// This protects against edge cases in the race between handleChange and Close().
	defer func() {
		if r := recover(); r != nil {
			// Only log if not during shutdown
			if m.isConnected() {
				m.logger.Error("panic in handleChange", "recover", r)
			}
		}
	}()

	// Early return if manager is closing or closed
	if !m.isConnected() {
		return
	}

	// Cache reference - capture once to avoid race with Close()
	cache := m.cache
	if cache == nil {
		return
	}

	switch change.Type {
	case ChangeTypeSet:
		if change.Value != nil {
			if err := cache.Set(ctx, change.Namespace, change.Key, change.Value); err != nil {
				// Only log if still connected (avoid spurious errors during shutdown)
				if m.isConnected() {
					m.logger.Warn("failed to update cache", "namespace", change.Namespace, "key", change.Key, "error", err)
				}
			}
		}
	case ChangeTypeDelete:
		if err := cache.Delete(ctx, change.Namespace, change.Key); err != nil {
			// Only log if still connected (avoid spurious errors during shutdown)
			if m.isConnected() {
				m.logger.Warn("failed to invalidate cache", "namespace", change.Namespace, "key", change.Key, "error", err)
			}
		}
	}
}
