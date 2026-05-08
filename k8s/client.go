package k8s

import (
	"context"
	"errors"
)

// Kind identifies which Kubernetes resource type a Resource represents.
// Store routes ConfigMap reads/writes for non-secret keys and Secret reads/writes
// for keys matching the configured secret prefix.
type Kind int

const (
	// KindConfigMap represents a Kubernetes ConfigMap.
	KindConfigMap Kind = iota
	// KindSecret represents a Kubernetes Secret.
	KindSecret
)

// String returns the human-readable name of the kind.
func (k Kind) String() string {
	switch k {
	case KindConfigMap:
		return "configmap"
	case KindSecret:
		return "secret"
	default:
		return "unknown"
	}
}

// Resource is the kubernetes-agnostic projection of a ConfigMap or Secret used
// by Store. ConfigMap string data and Secret byte data are both represented as
// []byte here; adapters convert between this form and the wire types.
type Resource struct {
	// Name is the resource name (e.g., "config-prod" or "config-secrets-prod").
	Name string
	// ResourceVersion is the Kubernetes ResourceVersion, used as the value version.
	ResourceVersion string
	// Annotations carries codec-name annotations populated by the Store.
	// Adapters must round-trip annotations exactly.
	Annotations map[string]string
	// Data is the resource payload keyed by Kubernetes data key (slash converted to dot).
	Data map[string][]byte
}

// EventType describes the type of change observed in a Watch stream.
type EventType int

const (
	// EventAdd is emitted when a watched resource is created or first observed.
	EventAdd EventType = iota
	// EventUpdate is emitted when a watched resource is modified.
	EventUpdate
	// EventDelete is emitted when a watched resource is removed.
	EventDelete
)

// Event is a single change notification produced by Client.Watch.
type Event struct {
	// Type is the kind of change.
	Type EventType
	// Kind identifies whether the change applies to a ConfigMap or a Secret.
	Kind Kind
	// Namespace is the Kubernetes namespace of the resource.
	Namespace string
	// Old is the previous resource state. Set on EventUpdate and EventDelete.
	Old *Resource
	// New is the new resource state. Set on EventAdd and EventUpdate.
	New *Resource
}

// ErrNotFound is returned by Client.Get when the requested resource does not
// exist. Adapters must translate Kubernetes "not found" API errors to this
// sentinel so the Store can surface config.ErrNotFound to callers.
var ErrNotFound = errors.New("k8s resource not found")

// Client is the kubernetes-facing surface required by Store. It is intentionally
// narrow so that adapters only need to translate a handful of operations and
// the main config module does not depend on k8s.io/* packages.
//
// Implementations must be safe for concurrent use by multiple goroutines.
//
// See k8s/example for a reference adapter built on top of kubernetes.Interface.
type Client interface {
	// Get fetches a resource by namespace and name. Implementations must return
	// ErrNotFound when the resource does not exist. Other errors are wrapped
	// and surfaced verbatim by the Store.
	Get(ctx context.Context, kind Kind, namespace, name string) (*Resource, error)

	// Upsert creates the resource if it does not exist, or updates it otherwise.
	// The returned Resource carries the post-write ResourceVersion.
	Upsert(ctx context.Context, kind Kind, namespace string, r *Resource) (*Resource, error)

	// Watch starts a watch over both ConfigMaps and Secrets in namespace
	// (or all namespaces when namespace is ""). The returned channel must:
	//
	//   - Emit one Event per observed add/update/delete.
	//   - Be closed when ctx is cancelled or the underlying watch terminates.
	//   - Reconnect transparently on transient errors.
	//
	// Adapters typically filter to resources whose names begin with the Store's
	// managed prefixes ("config-" and "config-secrets-") to avoid noisy events;
	// the Store also filters events on its side.
	Watch(ctx context.Context, namespace string) (<-chan Event, error)

	// Health performs a lightweight liveness check (e.g., listing namespaces).
	// Used by Store.Health.
	Health(ctx context.Context) error
}
