// Package kubeclient is a reference adapter that satisfies the [k8s.Client]
// interface defined by github.com/rbaliyan/config/k8s using the official
// Kubernetes Go client.
//
// The adapter intentionally lives in its own Go module so that the main config
// module does not pull in k8s.io/* dependencies. Copy this file into your own
// project (or import this module directly) to wire a [k8s.Store] up to a real
// cluster.
//
// Usage:
//
//	cfg, err := rest.InClusterConfig() // or BuildConfigFromFlags(...)
//	if err != nil { ... }
//	cs, err := kubernetes.NewForConfig(cfg)
//	if err != nil { ... }
//
//	client := kubeclient.New(cs)
//	store := k8s.NewStore(client, k8s.WithK8sNamespace("config-system"))
//	if err := store.Connect(ctx); err != nil { ... }
//	defer store.Close(ctx)
package kubeclient

import (
	"context"
	"fmt"

	"github.com/rbaliyan/config/k8s"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
)

// Adapter implements [k8s.Client] over kubernetes.Interface. Reads and writes
// go straight to the API server (the Store does not maintain a local cache);
// Watch wraps Kubernetes watch.Interface streams into a single Event channel.
type Adapter struct {
	cs kubernetes.Interface
}

// New constructs an Adapter wrapping the given Kubernetes clientset.
func New(cs kubernetes.Interface) *Adapter {
	return &Adapter{cs: cs}
}

// Get fetches a ConfigMap or Secret by namespace and name.
func (a *Adapter) Get(ctx context.Context, kind k8s.Kind, namespace, name string) (*k8s.Resource, error) {
	switch kind {
	case k8s.KindConfigMap:
		cm, err := a.cs.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil, k8s.ErrNotFound
			}
			return nil, err
		}
		return cmToResource(cm), nil
	case k8s.KindSecret:
		sec, err := a.cs.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil, k8s.ErrNotFound
			}
			return nil, err
		}
		return secretToResource(sec), nil
	default:
		return nil, fmt.Errorf("kubeclient: unknown kind %v", kind)
	}
}

// Upsert creates or updates a ConfigMap or Secret.
func (a *Adapter) Upsert(ctx context.Context, kind k8s.Kind, namespace string, r *k8s.Resource) (*k8s.Resource, error) {
	switch kind {
	case k8s.KindConfigMap:
		return a.upsertConfigMap(ctx, namespace, r)
	case k8s.KindSecret:
		return a.upsertSecret(ctx, namespace, r)
	default:
		return nil, fmt.Errorf("kubeclient: unknown kind %v", kind)
	}
}

func (a *Adapter) upsertConfigMap(ctx context.Context, namespace string, r *k8s.Resource) (*k8s.Resource, error) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:        r.Name,
			Namespace:   namespace,
			Annotations: r.Annotations,
		},
		Data: bytesToStringMap(r.Data),
	}
	created, err := a.cs.CoreV1().ConfigMaps(namespace).Create(ctx, cm, metav1.CreateOptions{})
	if err == nil {
		return cmToResource(created), nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return nil, err
	}
	updated, err := a.cs.CoreV1().ConfigMaps(namespace).Update(ctx, cm, metav1.UpdateOptions{})
	if err != nil {
		return nil, err
	}
	return cmToResource(updated), nil
}

func (a *Adapter) upsertSecret(ctx context.Context, namespace string, r *k8s.Resource) (*k8s.Resource, error) {
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:        r.Name,
			Namespace:   namespace,
			Annotations: r.Annotations,
		},
		Data: r.Data,
	}
	created, err := a.cs.CoreV1().Secrets(namespace).Create(ctx, sec, metav1.CreateOptions{})
	if err == nil {
		return secretToResource(created), nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return nil, err
	}
	updated, err := a.cs.CoreV1().Secrets(namespace).Update(ctx, sec, metav1.UpdateOptions{})
	if err != nil {
		return nil, err
	}
	return secretToResource(updated), nil
}

// Watch starts watching ConfigMaps and Secrets in namespace and forwards
// add/update/delete events on a single channel. The channel is closed when
// ctx is cancelled or when either upstream watch terminates. This adapter
// does NOT auto-reconnect on transient errors — wrap the upstream watches
// with k8s.io/client-go/tools/watch.NewRetryWatcher for resilient operation.
// Old is left nil on every Event; the Store treats every key in New as
// changed and skips delete inference, which is correct but noisier than a
// minimal-diff implementation.
func (a *Adapter) Watch(ctx context.Context, namespace string) (<-chan k8s.Event, error) {
	cmW, err := a.cs.CoreV1().ConfigMaps(namespace).Watch(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	secW, err := a.cs.CoreV1().Secrets(namespace).Watch(ctx, metav1.ListOptions{})
	if err != nil {
		cmW.Stop()
		return nil, err
	}

	out := make(chan k8s.Event, 32)
	go func() {
		defer close(out)
		defer cmW.Stop()
		defer secW.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-cmW.ResultChan():
				if !ok {
					return
				}
				if e, kind, fwd := translateConfigMapEvent(ev); fwd {
					send(ctx, out, k8s.Event{Type: e, Kind: kind, Namespace: namespace, Old: nil, New: cmObjToResource(ev.Object)})
				}
			case ev, ok := <-secW.ResultChan():
				if !ok {
					return
				}
				if e, kind, fwd := translateSecretEvent(ev); fwd {
					send(ctx, out, k8s.Event{Type: e, Kind: kind, Namespace: namespace, Old: nil, New: secObjToResource(ev.Object)})
				}
			}
		}
	}()
	return out, nil
}

// Health performs a lightweight liveness check by listing namespaces.
func (a *Adapter) Health(ctx context.Context) error {
	_, err := a.cs.CoreV1().Namespaces().List(ctx, metav1.ListOptions{Limit: 1})
	return err
}

// ---------------------------------------------------------------------------
// Conversion helpers
// ---------------------------------------------------------------------------

func cmToResource(cm *corev1.ConfigMap) *k8s.Resource {
	if cm == nil {
		return nil
	}
	return &k8s.Resource{
		Name:            cm.Name,
		ResourceVersion: cm.ResourceVersion,
		Annotations:     cm.Annotations,
		Data:            stringToBytesMap(cm.Data),
	}
}

func secretToResource(sec *corev1.Secret) *k8s.Resource {
	if sec == nil {
		return nil
	}
	return &k8s.Resource{
		Name:            sec.Name,
		ResourceVersion: sec.ResourceVersion,
		Annotations:     sec.Annotations,
		Data:            sec.Data,
	}
}

func cmObjToResource(obj any) *k8s.Resource {
	cm, ok := obj.(*corev1.ConfigMap)
	if !ok {
		return nil
	}
	return cmToResource(cm)
}

func secObjToResource(obj any) *k8s.Resource {
	sec, ok := obj.(*corev1.Secret)
	if !ok {
		return nil
	}
	return secretToResource(sec)
}

func translateConfigMapEvent(ev watch.Event) (k8s.EventType, k8s.Kind, bool) {
	switch ev.Type {
	case watch.Added:
		return k8s.EventAdd, k8s.KindConfigMap, true
	case watch.Modified:
		return k8s.EventUpdate, k8s.KindConfigMap, true
	case watch.Deleted:
		return k8s.EventDelete, k8s.KindConfigMap, true
	default:
		return 0, k8s.KindConfigMap, false
	}
}

func translateSecretEvent(ev watch.Event) (k8s.EventType, k8s.Kind, bool) {
	switch ev.Type {
	case watch.Added:
		return k8s.EventAdd, k8s.KindSecret, true
	case watch.Modified:
		return k8s.EventUpdate, k8s.KindSecret, true
	case watch.Deleted:
		return k8s.EventDelete, k8s.KindSecret, true
	default:
		return 0, k8s.KindSecret, false
	}
}

func send(ctx context.Context, out chan<- k8s.Event, ev k8s.Event) {
	select {
	case out <- ev:
	case <-ctx.Done():
	}
}

func stringToBytesMap(in map[string]string) map[string][]byte {
	if in == nil {
		return nil
	}
	out := make(map[string][]byte, len(in))
	for k, v := range in {
		out[k] = []byte(v)
	}
	return out
}

func bytesToStringMap(in map[string][]byte) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = string(v)
	}
	return out
}

var _ k8s.Client = (*Adapter)(nil)
