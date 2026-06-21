package k8s

import "github.com/rbaliyan/config"

const (
	defaultSecretPrefix = "secret/"
	defaultWatchBufSize = 100
)

type storeOptions struct {
	k8sNamespace string // k8s namespace to scope to; "" = all namespaces
	secretPrefix string // config keys with this prefix → Secret; default "secret/"
	watchBufSize int    // per-subscriber buffer for Store.Watch channels; default 100

	onDropped func(event config.ChangeEvent) // optional callback when a watch event is dropped
}

func defaultOptions() storeOptions {
	return storeOptions{
		secretPrefix: defaultSecretPrefix,
		watchBufSize: defaultWatchBufSize,
	}
}

// Option configures the k8s store.
type Option func(*storeOptions)

// WithK8sNamespace pins all reads and writes to a single Kubernetes namespace.
//
// When set, every config namespace maps to this Kubernetes namespace and
// Watch is scoped to it. When empty (the default), the config namespace name
// is used directly as the Kubernetes namespace; an empty config namespace
// falls back to the Kubernetes "default" namespace.
func WithK8sNamespace(ns string) Option {
	return func(o *storeOptions) {
		o.k8sNamespace = ns
	}
}

// WithSecretKeyPrefix sets the config key prefix that routes to Kubernetes Secrets.
// Keys with this prefix are stored in/read from Secrets; all others use ConfigMaps.
// Set to "" to disable secret routing entirely.
// Default is "secret/".
func WithSecretKeyPrefix(prefix string) Option {
	return func(o *storeOptions) {
		o.secretPrefix = prefix
	}
}

// WithWatchBufferSize sets the per-subscriber buffer size for Store.Watch
// channels. Default is 100.
func WithWatchBufferSize(n int) Option {
	return func(o *storeOptions) {
		if n > 0 {
			o.watchBufSize = n
		}
	}
}

// WithOnDropped sets a callback invoked when a watch event is dropped because
// a subscriber's channel buffer is full. Use it for logging or metrics. The
// callback runs synchronously on the notify path, so it must be fast and must
// not block. The dropped-event count is also available via Store.DroppedEvents.
func WithOnDropped(fn func(event config.ChangeEvent)) Option {
	return func(o *storeOptions) {
		o.onDropped = fn
	}
}
