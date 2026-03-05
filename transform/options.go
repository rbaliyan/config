// Package transform provides a store decorator that applies a codec.Transformer
// at the storage boundary. This enables layered encryption, compression, or other
// byte-level transformations transparently on all stored values.
package transform

// Option configures the transform store.
type Option func(*options)

type options struct {
	watchBufferSize int
}

func defaultOptions() options {
	return options{
		watchBufferSize: 100,
	}
}

// WithWatchBufferSize sets the buffer size for the output channel returned by Watch.
// Defaults to 100.
func WithWatchBufferSize(size int) Option {
	return func(o *options) {
		if size > 0 {
			o.watchBufferSize = size
		}
	}
}
