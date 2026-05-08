// Package k8s provides a config.Store backed by Kubernetes ConfigMaps and Secrets.
//
// Each config namespace is backed by one ConfigMap named "config-{namespace}" and,
// for keys matching the configured secret prefix, one Secret named
// "config-secrets-{namespace}". Config keys map to Kubernetes data keys by
// replacing "/" with "."; the codec used for each value is recorded in a
// per-resource annotation so reads can decode it.
//
// # Decoupled from kubernetes.io
//
// This package does NOT import any k8s.io/* packages. Instead it depends on a
// narrow [Client] interface that the caller must implement, typically over the
// real Kubernetes Go client. See the k8s/example sub-module for a reference
// adapter built on top of kubernetes.Interface.
//
// # Operational model
//
// Like the postgres and mongodb stores, the k8s store does not maintain a local
// cache of resources. Reads (Get, Find) call straight through to the Client and
// rely on the Manager-level cache for repeat lookups. Watch is fed by
// Client.Watch, which the Store fans out to per-subscriber channels.
//
// # Connect / Close
//
// Connect starts a background goroutine that consumes events from Client.Watch
// and dispatches them to active watchers. The Store does not wait for any
// initial sync — the first Get after Connect performs an API round-trip via
// the Client. Close stops the watch goroutine and closes all subscriber
// channels.
package k8s

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rbaliyan/config"
)

// configMapPrefix is the prefix for ConfigMap names managed by this store.
const configMapPrefix = "config-"

// secretNamePrefix is the prefix for Secret names managed by this store.
const secretNamePrefix = "config-secrets-"

// codecAnnotationPrefix prefixes the per-key codec annotation keys.
// Each data key stored in a ConfigMap/Secret has a matching annotation
// "config.rbaliyan.dev/codec-<k8sKey>" naming the codec used to encode it.
const codecAnnotationPrefix = "config.rbaliyan.dev/codec-"

func codecAnnotationKey(k8sKey string) string {
	return codecAnnotationPrefix + k8sKey
}

// resolveCodec returns the stored codec name for a given k8sKey, or "json" as fallback.
func resolveCodec(anns map[string]string, k8sKey string) string {
	if anns != nil {
		if c := anns[codecAnnotationKey(k8sKey)]; c != "" {
			return c
		}
	}
	return "json"
}

// Store implements config.Store backed by Kubernetes ConfigMaps and Secrets via
// a [Client] adapter.
type Store struct {
	client Client
	opts   storeOptions

	connected atomic.Bool
	closed    atomic.Bool

	watchCtx    context.Context
	watchCancel context.CancelFunc
	watchDone   chan struct{}

	watchMu  sync.RWMutex
	watchers map[*watchEntry]struct{}
}

var (
	_ config.Store         = (*Store)(nil)
	_ config.HealthChecker = (*Store)(nil)
)

// NewStore creates a new k8s-backed config store using the given Client.
// Call Connect before using the store.
func NewStore(client Client, opts ...Option) *Store {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	return &Store{
		client:   client,
		opts:     o,
		watchers: make(map[*watchEntry]struct{}),
	}
}

// BackendName returns the stable backend identifier used in error messages.
func (s *Store) BackendName() string { return "k8s" }

// Connect starts the background watch loop. The first read after Connect
// hits the Kubernetes API via the Client; there is no initial cache sync.
// The watch lifetime is bounded by Close, not by ctx — ctx is only used for
// validation prior to starting the watch.
func (s *Store) Connect(ctx context.Context) error {
	if s.closed.Load() {
		return config.ErrStoreClosed
	}
	if !s.connected.CompareAndSwap(false, true) {
		return nil // already connected
	}

	events, err := s.client.Watch(context.Background(), s.opts.k8sNamespace)
	if err != nil {
		s.connected.Store(false)
		return config.WrapStoreError("connect", "k8s", "watch", err)
	}

	s.watchCtx, s.watchCancel = context.WithCancel(context.Background())
	s.watchDone = make(chan struct{})

	go s.runWatch(events)
	return nil
}

// Close stops the watch goroutine and closes all subscriber channels.
// Close blocks until the watch goroutine has exited.
func (s *Store) Close(_ context.Context) error {
	if s.closed.Swap(true) {
		return nil // already closed
	}
	if s.watchCancel != nil {
		s.watchCancel()
	}
	if s.watchDone != nil {
		<-s.watchDone
	}

	s.watchMu.Lock()
	toClose := make([]*watchEntry, 0, len(s.watchers))
	for we := range s.watchers {
		toClose = append(toClose, we)
	}
	s.watchers = make(map[*watchEntry]struct{})
	s.watchMu.Unlock()

	for _, we := range toClose {
		we.cancel()
		we.closeOnce.Do(func() {
			we.mu.Lock()
			we.closed = true
			close(we.ch)
			we.mu.Unlock()
		})
	}

	return nil
}

// Get retrieves a configuration value by namespace and key.
func (s *Store) Get(ctx context.Context, namespace, key string) (config.Value, error) {
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
	}
	if !s.connected.Load() {
		return nil, config.ErrStoreNotConnected
	}
	if err := config.ValidateNamespace(namespace); err != nil {
		return nil, err
	}
	if err := config.ValidateKey(key); err != nil {
		return nil, err
	}

	k8sNS := s.k8sNamespace(namespace)
	k8sKey := configKeyToK8sKey(key)
	kind, name := s.resourceFor(namespace, key)

	r, err := s.client.Get(ctx, kind, k8sNS, name)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, &config.KeyNotFoundError{Key: key, Namespace: namespace}
		}
		return nil, config.WrapStoreError("get", "k8s", key, err)
	}
	raw, ok := r.Data[k8sKey]
	if !ok {
		return nil, &config.KeyNotFoundError{Key: key, Namespace: namespace}
	}
	return makeValueFromBytes(ctx, raw, namespace, key, r.ResourceVersion, resolveCodec(r.Annotations, k8sKey))
}

// Set creates or updates a configuration value.
func (s *Store) Set(ctx context.Context, namespace, key string, value config.Value) (config.Value, error) {
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
	}
	if !s.connected.Load() {
		return nil, config.ErrStoreNotConnected
	}
	if err := config.ValidateNamespace(namespace); err != nil {
		return nil, err
	}
	if err := config.ValidateKey(key); err != nil {
		return nil, err
	}

	data, err := value.Marshal(ctx)
	if err != nil {
		return nil, config.WrapStoreError("marshal", "k8s", key, err)
	}

	k8sNS := s.k8sNamespace(namespace)
	k8sKey := configKeyToK8sKey(key)
	codecName := value.Codec()
	kind, name := s.resourceFor(namespace, key)

	existing, err := s.client.Get(ctx, kind, k8sNS, name)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, config.WrapStoreError("set", "k8s", key, err)
	}

	r := mergeResource(existing, name, k8sKey, data, codecName)
	updated, err := s.client.Upsert(ctx, kind, k8sNS, r)
	if err != nil {
		return nil, config.WrapStoreError("set", "k8s", key, err)
	}
	return makeValueFromBytes(ctx, data, namespace, key, updated.ResourceVersion, codecName)
}

// Delete removes a configuration entry.
func (s *Store) Delete(ctx context.Context, namespace, key string) error {
	if s.closed.Load() {
		return config.ErrStoreClosed
	}
	if !s.connected.Load() {
		return config.ErrStoreNotConnected
	}
	if err := config.ValidateNamespace(namespace); err != nil {
		return err
	}
	if err := config.ValidateKey(key); err != nil {
		return err
	}

	k8sNS := s.k8sNamespace(namespace)
	k8sKey := configKeyToK8sKey(key)
	kind, name := s.resourceFor(namespace, key)

	existing, err := s.client.Get(ctx, kind, k8sNS, name)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return &config.KeyNotFoundError{Key: key, Namespace: namespace}
		}
		return config.WrapStoreError("delete", "k8s", key, err)
	}
	if _, ok := existing.Data[k8sKey]; !ok {
		return &config.KeyNotFoundError{Key: key, Namespace: namespace}
	}
	delete(existing.Data, k8sKey)
	if existing.Annotations != nil {
		delete(existing.Annotations, codecAnnotationKey(k8sKey))
	}
	if _, err := s.client.Upsert(ctx, kind, k8sNS, existing); err != nil {
		return config.WrapStoreError("delete", "k8s", key, err)
	}
	return nil
}

// Find returns a page of configuration entries matching the filter.
//
// In Keys mode (filter.Keys() non-empty), each key is resolved individually
// and secret-prefixed keys are read from the namespace Secret as expected.
// In Prefix/cursor mode, only the namespace ConfigMap is scanned; Secret
// data keys are not enumerated. Use Keys mode to read secret-prefixed keys
// in bulk.
func (s *Store) Find(ctx context.Context, namespace string, filter config.Filter) (config.Page, error) {
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
	}
	if !s.connected.Load() {
		return nil, config.ErrStoreNotConnected
	}
	if err := config.ValidateNamespace(namespace); err != nil {
		return nil, err
	}
	if filter == nil {
		filter = config.NewFilter().Build()
	}

	// Keys mode: per-key Get (handles secret routing automatically).
	if keys := filter.Keys(); len(keys) > 0 {
		results := make(map[string]config.Value)
		for _, key := range keys {
			val, err := s.Get(ctx, namespace, key)
			if err == nil {
				results[key] = val
			}
		}
		return config.NewPage(results, "", 0), nil
	}

	prefix := filter.Prefix()
	cursor := filter.Cursor()
	limit := filter.Limit()

	k8sNS := s.k8sNamespace(namespace)
	cmName := configMapResourceName(namespace)

	cm, err := s.client.Get(ctx, KindConfigMap, k8sNS, cmName)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return config.NewPage(make(map[string]config.Value), "", limit), nil
		}
		return nil, config.WrapStoreError("find", "k8s", "", err)
	}

	type kv struct {
		configKey string
		raw       []byte
	}
	var matches []kv
	for k8sKey, raw := range cm.Data {
		configKey := k8sKeyToConfigKey(k8sKey)
		if prefix != "" && !strings.HasPrefix(configKey, prefix) {
			continue
		}
		if cursor != "" && configKey <= cursor {
			continue
		}
		matches = append(matches, kv{configKey: configKey, raw: raw})
	}

	sort.Slice(matches, func(i, j int) bool {
		return matches[i].configKey < matches[j].configKey
	})

	if limit > 0 && len(matches) > limit {
		matches = matches[:limit]
	}

	results := make(map[string]config.Value, len(matches))
	var lastKey string
	for _, m := range matches {
		k8sKey := configKeyToK8sKey(m.configKey)
		v, valErr := makeValueFromBytes(ctx, m.raw, namespace, m.configKey, cm.ResourceVersion, resolveCodec(cm.Annotations, k8sKey))
		if valErr == nil {
			results[m.configKey] = v
			lastKey = m.configKey
		}
	}

	return config.NewPage(results, lastKey, limit), nil
}

// Watch returns a channel that receives change events for matching keys.
// Returns ErrStoreNotConnected if called before Connect. Events are dropped
// when the per-subscriber buffer is full (size set via WithWatchBufferSize).
// The channel is closed when ctx is cancelled or the Store is closed.
func (s *Store) Watch(ctx context.Context, filter config.WatchFilter) (<-chan config.ChangeEvent, error) {
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
	}
	if !s.connected.Load() {
		return nil, config.ErrStoreNotConnected
	}

	ctx, cancel := context.WithCancel(ctx)
	ch := make(chan config.ChangeEvent, s.opts.watchBufSize)

	we := &watchEntry{
		filter: filter,
		ch:     ch,
		ctx:    ctx,
		cancel: cancel,
	}

	s.watchMu.Lock()
	s.watchers[we] = struct{}{}
	s.watchMu.Unlock()

	go func() {
		select {
		case <-ctx.Done():
		case <-s.watchCtx.Done():
		}

		s.watchMu.Lock()
		delete(s.watchers, we)
		s.watchMu.Unlock()

		we.closeOnce.Do(func() {
			we.mu.Lock()
			we.closed = true
			close(ch)
			we.mu.Unlock()
		})
	}()

	return ch, nil
}

// Health performs a health check by delegating to the Client.
func (s *Store) Health(ctx context.Context) error {
	if s.closed.Load() {
		return config.ErrStoreClosed
	}
	if !s.connected.Load() {
		return config.ErrStoreNotConnected
	}
	if err := s.client.Health(ctx); err != nil {
		return config.WrapStoreError("health", "k8s", "", err)
	}
	return nil
}

// runWatch consumes events from the Client and dispatches per-key change events
// to subscribers. It exits when watchCtx is cancelled or events is closed.
func (s *Store) runWatch(events <-chan Event) {
	defer close(s.watchDone)
	for {
		select {
		case <-s.watchCtx.Done():
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			s.dispatchEvent(ev)
		}
	}
}

// dispatchEvent translates a single resource-level Event into per-key
// config.ChangeEvents and delivers them to matching watchers.
func (s *Store) dispatchEvent(ev Event) {
	now := time.Now().UTC()
	ns := s.configNamespace(ev)
	if ns == "" {
		return // not a resource we manage
	}

	switch ev.Type {
	case EventAdd:
		if ev.New == nil {
			return
		}
		for k8sKey, raw := range ev.New.Data {
			s.emitSet(ns, ev.New, k8sKey, raw, now)
		}
	case EventUpdate:
		if ev.New == nil {
			return
		}
		var oldData map[string][]byte
		if ev.Old != nil {
			oldData = ev.Old.Data
		}
		for k8sKey, newRaw := range ev.New.Data {
			oldRaw, existed := oldData[k8sKey]
			if !existed || !bytes.Equal(oldRaw, newRaw) {
				s.emitSet(ns, ev.New, k8sKey, newRaw, now)
			}
		}
		for k8sKey := range oldData {
			if _, ok := ev.New.Data[k8sKey]; !ok {
				s.emitDelete(ns, k8sKey, now)
			}
		}
	case EventDelete:
		if ev.Old == nil {
			return
		}
		for k8sKey := range ev.Old.Data {
			s.emitDelete(ns, k8sKey, now)
		}
	}
}

func (s *Store) emitSet(ns string, r *Resource, k8sKey string, raw []byte, ts time.Time) {
	configKey := k8sKeyToConfigKey(k8sKey)
	v, err := makeValueFromBytes(context.Background(), raw, ns, configKey, r.ResourceVersion, resolveCodec(r.Annotations, k8sKey))
	if err != nil {
		return
	}
	s.notifyWatchers(config.ChangeEvent{
		Type:      config.ChangeTypeSet,
		Namespace: ns,
		Key:       configKey,
		Value:     v,
		Timestamp: ts,
	})
}

func (s *Store) emitDelete(ns, k8sKey string, ts time.Time) {
	s.notifyWatchers(config.ChangeEvent{
		Type:      config.ChangeTypeDelete,
		Namespace: ns,
		Key:       k8sKeyToConfigKey(k8sKey),
		Timestamp: ts,
	})
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// k8sNamespace returns the Kubernetes namespace to use for a given config namespace.
func (s *Store) k8sNamespace(configNS string) string {
	if s.opts.k8sNamespace != "" {
		return s.opts.k8sNamespace
	}
	if configNS == "" {
		return "default"
	}
	return configNS
}

// resourceFor returns the kind and resource name backing the given config key.
func (s *Store) resourceFor(namespace, key string) (Kind, string) {
	if isSecretKey(key, s.opts.secretPrefix) {
		return KindSecret, secretResourceName(namespace)
	}
	return KindConfigMap, configMapResourceName(namespace)
}

// configNamespace recovers the config namespace from a watched event.
// Returns "" if the resource is not managed by this store.
func (s *Store) configNamespace(ev Event) string {
	var name string
	switch {
	case ev.New != nil:
		name = ev.New.Name
	case ev.Old != nil:
		name = ev.Old.Name
	default:
		return ""
	}
	if rest, ok := strings.CutPrefix(name, secretNamePrefix); ok {
		return rest
	}
	if rest, ok := strings.CutPrefix(name, configMapPrefix); ok {
		return rest
	}
	return ""
}

// configMapResourceName returns the ConfigMap name for a config namespace.
func configMapResourceName(namespace string) string {
	return configMapPrefix + namespace
}

// secretResourceName returns the Secret name for a config namespace.
func secretResourceName(namespace string) string {
	return secretNamePrefix + namespace
}

// configKeyToK8sKey converts a config key to a valid Kubernetes data key.
// "/" is replaced with "." to form a DNS-safe label.
func configKeyToK8sKey(key string) string {
	return strings.ReplaceAll(key, "/", ".")
}

// k8sKeyToConfigKey converts a Kubernetes data key back to a config key.
func k8sKeyToConfigKey(key string) string {
	return strings.ReplaceAll(key, ".", "/")
}

// isSecretKey reports whether a config key should be routed to a Secret.
func isSecretKey(key, prefix string) bool {
	return prefix != "" && strings.HasPrefix(key, prefix)
}

// mergeResource constructs a Resource for an Upsert: clones existing or builds
// a new one with the given data key and codec annotation set.
func mergeResource(existing *Resource, name, k8sKey string, raw []byte, codecName string) *Resource {
	if existing == nil {
		return &Resource{
			Name:        name,
			Annotations: map[string]string{codecAnnotationKey(k8sKey): codecName},
			Data:        map[string][]byte{k8sKey: raw},
		}
	}
	if existing.Data == nil {
		existing.Data = make(map[string][]byte)
	}
	if existing.Annotations == nil {
		existing.Annotations = make(map[string]string)
	}
	existing.Data[k8sKey] = raw
	existing.Annotations[codecAnnotationKey(k8sKey)] = codecName
	return existing
}

// makeValueFromBytes builds a config.Value from raw bytes plus k8s metadata.
func makeValueFromBytes(ctx context.Context, raw []byte, namespace, key, resourceVersion, codecName string) (config.Value, error) {
	if codecName == "" {
		codecName = "json"
	}
	version := parseResourceVersion(resourceVersion)
	entryID := namespace + "/" + key
	return config.NewValueFromBytes(
		ctx,
		raw,
		codecName,
		config.WithValueMetadata(version, time.Time{}, time.Time{}),
		config.WithValueEntryID(entryID),
	)
}

// parseResourceVersion parses a Kubernetes ResourceVersion string to int64.
// Returns 0 if the string is empty or non-numeric.
func parseResourceVersion(rv string) int64 {
	v, _ := strconv.ParseInt(rv, 10, 64)
	return v
}
