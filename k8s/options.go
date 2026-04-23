package k8s

import (
	"time"
)

const (
	defaultSecretPrefix  = "secret/"
	defaultResyncPeriod  = 10 * time.Minute
	defaultWatchBufSize  = 100
)

type storeOptions struct {
	k8sNamespace string        // k8s namespace to watch; "" = all namespaces
	secretPrefix string        // config keys with this prefix → Secret; default "secret/"
	resyncPeriod time.Duration // informer resync; default 10m
	watchBufSize int           // default 100
}

func defaultOptions() storeOptions {
	return storeOptions{
		secretPrefix: defaultSecretPrefix,
		resyncPeriod: defaultResyncPeriod,
		watchBufSize: defaultWatchBufSize,
	}
}

// Option configures the k8s store.
type Option func(*storeOptions)

// WithK8sNamespace restricts the store to a single Kubernetes namespace.
// When empty (the default), the store watches all namespaces.
func WithK8sNamespace(ns string) Option {
	return func(o *storeOptions) {
		o.k8sNamespace = ns
	}
}

// WithSecretKeyPrefix sets the config key prefix that routes to Kubernetes Secrets.
// Keys with this prefix are stored in/read from Secrets; all others use ConfigMaps.
// Default is "secret/".
func WithSecretKeyPrefix(prefix string) Option {
	return func(o *storeOptions) {
		o.secretPrefix = prefix
	}
}

// WithResyncPeriod sets the informer resync period.
// Default is 10 minutes.
func WithResyncPeriod(d time.Duration) Option {
	return func(o *storeOptions) {
		if d > 0 {
			o.resyncPeriod = d
		}
	}
}

// WithWatchBufferSize sets the buffer size for watch event channels.
// Default is 100.
func WithWatchBufferSize(n int) Option {
	return func(o *storeOptions) {
		if n > 0 {
			o.watchBufSize = n
		}
	}
}
