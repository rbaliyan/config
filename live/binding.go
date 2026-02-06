// Package live provides auto-reloading struct bindings for configuration.
// It automatically keeps a struct synchronized with configuration values
// using polling.
package live

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/bind"
)

const (
	// DefaultPollInterval is the default interval for polling config changes.
	DefaultPollInterval = 30 * time.Second
)

var (
	// ErrInvalidTarget is returned when the target is not a pointer to a struct.
	ErrInvalidTarget = errors.New("live: target must be a pointer to a struct")
)

// Binding maintains a live connection between a config key and a struct.
// It automatically reloads the struct when the config changes via polling.
//
// Deprecated: Use [Ref] instead. Ref provides lock-free reads via atomic swap,
// generic type safety, and change detection via content hashing. Binding holds
// a write lock during the entire store fetch, blocking all readers.
//
// The Binding must be stopped by calling Stop() when no longer needed
// to release resources and stop the background goroutine.
type Binding struct {
	cfg      config.Config
	binder   *bind.Binder
	key      string
	target   any
	targetMu sync.RWMutex // protects target during reload

	pollInterval time.Duration
	onReload     func()
	onError      func(error)

	lastReload atomic.Value // time.Time
	lastError  atomic.Value // error

	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
}

// Option configures a Binding.
type Option func(*Binding)

// WithPollInterval sets the polling interval for checking config changes.
// Default is 30 seconds.
func WithPollInterval(d time.Duration) Option {
	return func(b *Binding) {
		if d > 0 {
			b.pollInterval = d
		}
	}
}

// WithOnReload sets a callback that is invoked after each successful reload.
// The callback is called with the target mutex held for reading, so it's safe
// to read from the target struct within the callback.
func WithOnReload(fn func()) Option {
	return func(b *Binding) {
		b.onReload = fn
	}
}

// WithOnError sets a callback that is invoked when reload fails.
func WithOnError(fn func(error)) Option {
	return func(b *Binding) {
		b.onError = fn
	}
}

// Bind creates a new live binding between a config key and the target struct.
// The target must be a pointer to a struct.
//
// The binding immediately loads the current config value into target,
// then continues polling for changes in the background.
//
// Call Stop() to stop the background goroutine and release resources.
//
// Example:
//
//	var dbConfig DatabaseConfig
//	binding, err := live.Bind(ctx, cfg, "database", &dbConfig,
//	    live.WithPollInterval(10*time.Second),
//	    live.WithOnReload(func() {
//	        log.Println("config reloaded")
//	    }),
//	)
//	if err != nil {
//	    return err
//	}
//	defer binding.Stop()
//
//	// dbConfig is now live - automatically updated on changes
//	fmt.Println(dbConfig.Host)
func Bind(ctx context.Context, cfg config.Config, key string, target any, opts ...Option) (*Binding, error) {
	// Validate target is a pointer to a struct
	rv := reflect.ValueOf(target)
	if rv.Kind() != reflect.Pointer || rv.Elem().Kind() != reflect.Struct {
		return nil, ErrInvalidTarget
	}

	b := &Binding{
		cfg:          cfg,
		binder:       bind.New(cfg),
		key:          key,
		target:       target,
		pollInterval: DefaultPollInterval,
		stopCh:       make(chan struct{}),
		doneCh:       make(chan struct{}),
	}

	for _, opt := range opts {
		opt(b)
	}

	// Perform initial load
	if err := b.reload(ctx); err != nil {
		return nil, err
	}

	// Start background polling
	go b.poll()

	return b, nil
}

// Stop stops the background polling goroutine.
// After calling Stop, the target struct will no longer be updated.
// Safe to call multiple times.
func (b *Binding) Stop() {
	b.stopOnce.Do(func() {
		close(b.stopCh)
		<-b.doneCh // Wait for poll goroutine to finish
	})
}

// LastReload returns the timestamp of the last successful reload.
// Returns zero time if no successful reload has occurred.
func (b *Binding) LastReload() time.Time {
	if v := b.lastReload.Load(); v != nil {
		return v.(time.Time)
	}
	return time.Time{}
}

// LastError returns the last error encountered during reload, if any.
// Returns nil if the last reload was successful or no reload has occurred.
func (b *Binding) LastError() error {
	v := b.lastError.Load()
	if v == nil {
		return nil
	}
	return v.(errorWrapper).err
}

// ReloadNow triggers an immediate reload, bypassing the poll interval.
// Useful for forcing a refresh after a known config change.
func (b *Binding) ReloadNow(ctx context.Context) error {
	return b.reload(ctx)
}

// Get provides safe read access to the target struct.
// The function fn is called with the target mutex held for reading.
func (b *Binding) Get(fn func(target any)) {
	b.targetMu.RLock()
	defer b.targetMu.RUnlock()
	fn(b.target)
}

// poll runs the background polling loop.
func (b *Binding) poll() {
	defer close(b.doneCh)

	ticker := time.NewTicker(b.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-b.stopCh:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), b.pollInterval/2)
			if err := b.reload(ctx); err != nil {
				b.lastError.Store(errorWrapper{err: err})
				if b.onError != nil {
					b.onError(err)
				}
			}
			cancel()
		}
	}
}

// reload fetches the latest config and updates the target struct.
func (b *Binding) reload(ctx context.Context) error {
	b.targetMu.Lock()
	defer b.targetMu.Unlock()

	bound := b.binder.Bind()
	if err := bound.GetStruct(ctx, b.key, b.target); err != nil {
		return err
	}

	b.lastReload.Store(time.Now())
	b.lastError.Store(errorWrapper{err: nil})

	if b.onReload != nil {
		b.onReload()
	}

	return nil
}

// errorWrapper wraps an error for atomic.Value (which requires consistent types).
type errorWrapper struct {
	err error
}
