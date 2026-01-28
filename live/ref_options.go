package live

import (
	"time"

	"github.com/rbaliyan/config/bind"
)

// RefOption configures a Ref[T].
type RefOption[T any] func(*refOptions[T])

type refOptions[T any] struct {
	pollInterval time.Duration    // default DefaultPollInterval
	onChange     func(oldVal, newVal T) // optional change callback
	onError     func(error)      // error callback
	bindOpts    []bind.Option    // forwarded to bind.New
}

// OnChange registers a callback invoked after each config change.
// Both old and new are value copies — safe to retain, pass around, or ignore.
// The callback is invoked synchronously on the poll goroutine.
// Keep it brief; blocking it delays the next update.
func OnChange[T any](fn func(oldVal, newVal T)) RefOption[T] {
	return func(o *refOptions[T]) {
		o.onChange = fn
	}
}

// OnError registers a callback invoked when a reload fails.
// The last good snapshot is preserved; Load() continues to work.
func OnError[T any](fn func(error)) RefOption[T] {
	return func(o *refOptions[T]) {
		o.onError = fn
	}
}

// WithRefPollInterval sets the polling interval for checking config changes.
// Default is DefaultPollInterval (30s).
func WithRefPollInterval[T any](d time.Duration) RefOption[T] {
	return func(o *refOptions[T]) {
		if d > 0 {
			o.pollInterval = d
		}
	}
}

// WithBindOptions forwards options to the underlying bind.Binder.
// Use this to configure field tag names, codecs, or validation.
func WithBindOptions[T any](opts ...bind.Option) RefOption[T] {
	return func(o *refOptions[T]) {
		o.bindOpts = append(o.bindOpts, opts...)
	}
}
