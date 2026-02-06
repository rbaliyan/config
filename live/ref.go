package live

import (
	"context"
	"errors"
	"fmt"
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

// errorWrapper wraps an error for atomic.Value (which requires consistent types).
type errorWrapper struct {
	err error
}

// Ref holds a live, atomically-updated reference to a typed config struct.
// It polls for config changes in the background and swaps in new snapshots
// using atomic.Pointer, making Load() a single atomic read with zero
// contention, no mutex, and no allocation.
//
// The returned *T from Load() is never mutated by the library — it is a
// frozen snapshot that is safe to hold, pass to other goroutines, or compare
// with a previous snapshot.
//
// Usage:
//
//	ref, err := live.New[DatabaseConfig](ctx, cfg, "database",
//	    live.OnChange(func(old, new DatabaseConfig) {
//	        pool.Resize(new.MaxConns)
//	    }),
//	)
//	if err != nil { return err }
//	defer ref.Close()
//
//	// Hot path — single atomic load, zero cost
//	snapshot := ref.Load()
//	fmt.Println(snapshot.Host, snapshot.Port)
type Ref[T any] struct {
	current atomic.Pointer[T]

	cfg    config.Config
	binder *bind.Binder
	prefix string
	opts   refOptions[T]

	// Change detection
	lastDigest atomic.Uint64

	// Observability
	lastReload  atomic.Value // time.Time
	lastError   atomic.Value // errorWrapper
	reloadCount atomic.Int64

	// Stop
	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
}

// New creates a live Ref that automatically tracks config changes under
// the given key prefix. It performs an initial load synchronously; if the
// initial load fails, New returns an error and no Ref.
//
// The type parameter T must be a struct type.
//
// The Ref starts a background goroutine that polls for changes.
// Call Close() to stop it.
func New[T any](ctx context.Context, cfg config.Config, prefix string, opts ...RefOption[T]) (*Ref[T], error) {
	// Validate T is a struct
	if reflect.TypeFor[T]().Kind() != reflect.Struct {
		return nil, fmt.Errorf("live: Ref type parameter must be a struct, got %s", reflect.TypeFor[T]().Kind())
	}

	o := refOptions[T]{
		pollInterval: DefaultPollInterval,
	}
	for _, opt := range opts {
		opt(&o)
	}

	r := &Ref[T]{
		cfg:    cfg,
		binder: bind.New(cfg, o.bindOpts...),
		prefix: prefix,
		opts:   o,
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}

	// Synchronous initial load — fail fast
	initial, digest, err := r.loadFromStore(ctx)
	if err != nil {
		return nil, fmt.Errorf("live: initial load of %q: %w", prefix, err)
	}
	r.current.Store(initial)
	r.lastDigest.Store(digest)
	r.lastReload.Store(time.Now())
	r.lastError.Store(errorWrapper{err: nil})

	// Start background polling
	go r.run()

	return r, nil
}

// Load returns the current configuration snapshot.
// The returned pointer is immutable — the library will never modify
// the struct behind it. It is safe to hold, pass to other goroutines,
// or compare with a previous snapshot.
//
// This is a single atomic load — no mutex, no allocation.
func (r *Ref[T]) Load() *T {
	return r.current.Load()
}

// Close stops the background polling goroutine.
// After Close, Load() still works and returns the last known snapshot.
// Safe to call multiple times.
func (r *Ref[T]) Close() {
	r.stopOnce.Do(func() {
		close(r.stopCh)
		<-r.doneCh
	})
}

// LastReload returns the timestamp of the last successful reload.
// Returns zero time if no successful reload has occurred.
func (r *Ref[T]) LastReload() time.Time {
	if v := r.lastReload.Load(); v != nil {
		return v.(time.Time)
	}
	return time.Time{}
}

// LastError returns the last error encountered during reload, if any.
// Returns nil if the last reload was successful.
func (r *Ref[T]) LastError() error {
	v := r.lastError.Load()
	if v == nil {
		return nil
	}
	return v.(errorWrapper).err
}

// ReloadCount returns the total number of successful reloads
// (not counting the initial load).
func (r *Ref[T]) ReloadCount() int64 {
	return r.reloadCount.Load()
}

// ReloadNow triggers an immediate reload, bypassing the poll interval.
func (r *Ref[T]) ReloadNow(ctx context.Context) error {
	return r.reload(ctx)
}

// run is the background polling goroutine.
func (r *Ref[T]) run() {
	defer close(r.doneCh)

	ticker := time.NewTicker(r.opts.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), r.opts.pollInterval/2)
			r.reload(ctx)
			cancel()
		}
	}
}

// reload fetches the latest config and atomically swaps it in
// only if the data has actually changed (digest comparison).
func (r *Ref[T]) reload(ctx context.Context) error {
	newVal, newDigest, err := r.loadFromStore(ctx)
	if err != nil {
		r.lastError.Store(errorWrapper{err: err})
		r.safeCallback(func() {
			if r.opts.onError != nil {
				r.opts.onError(err)
			}
		})
		return err
	}

	// Skip swap and callback if nothing changed
	if newDigest == r.lastDigest.Load() {
		return nil
	}

	// Atomic swap
	old := r.current.Swap(newVal)
	r.lastDigest.Store(newDigest)
	r.lastReload.Store(time.Now())
	r.lastError.Store(errorWrapper{err: nil})
	r.reloadCount.Add(1)

	// Fire change callback with value copies
	if r.opts.onChange != nil && old != nil {
		oldCopy := *old
		newCopy := *newVal
		r.safeCallback(func() {
			r.opts.onChange(oldCopy, newCopy)
		})
	}

	return nil
}

// loadFromStore uses the bind pipeline to fetch, unmarshal, and digest config.
func (r *Ref[T]) loadFromStore(ctx context.Context) (*T, uint64, error) {
	var target T
	digest, err := r.binder.Bind().GetStructDigest(ctx, r.prefix, &target)
	if err != nil {
		return nil, 0, err
	}
	return &target, digest, nil
}

// safeCallback runs fn with panic recovery to prevent user code
// from crashing the background goroutine.
func (r *Ref[T]) safeCallback(fn func()) {
	defer func() {
		if v := recover(); v != nil {
			r.lastError.Store(errorWrapper{err: fmt.Errorf("panic in callback: %v", v)})
		}
	}()
	fn()
}

