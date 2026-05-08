package k8s

const (
	defaultSecretPrefix = "secret/"
	defaultWatchBufSize = 100
)

type storeOptions struct {
	k8sNamespace string // k8s namespace to scope to; "" = all namespaces
	secretPrefix string // config keys with this prefix → Secret; default "secret/"
	watchBufSize int    // per-subscriber buffer for Store.Watch channels; default 100
}

func defaultOptions() storeOptions {
	return storeOptions{
		secretPrefix: defaultSecretPrefix,
		watchBufSize: defaultWatchBufSize,
	}
}

// Option configures the k8s store.
type Option func(*storeOptions)

// WithK8sNamespace restricts the store to a single Kubernetes namespace.
// When empty (the default), the store operates across all namespaces and the
// config namespace name is used directly as the Kubernetes namespace.
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
