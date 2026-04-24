// Package k8s provides a config.Store backed by Kubernetes ConfigMaps and Secrets.
//
// Config namespaces map to Kubernetes namespaces (or to a fixed k8s namespace
// when WithK8sNamespace is set). Each config namespace is backed by one ConfigMap
// named "config-{namespace}" and, for secret-prefixed keys, one Secret named
// "config-secrets-{namespace}".
//
// Key mapping: config key "/" is replaced by "." to form a valid Kubernetes data key.
// Keys prefixed with opts.secretPrefix (default "secret/") are stored in Secrets;
// all other keys are stored in ConfigMaps.
//
// Watch support is implemented via k8s.io/client-go informers. The informer cache
// is populated on Connect and kept in sync automatically; Get/Find read from the
// local cache without making API calls.
package k8s

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rbaliyan/config"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// configMapPrefix is the prefix for ConfigMap names managed by this store.
const configMapPrefix = "config-"

// secretNamePrefix is the prefix for Secret names managed by this store.
const secretNamePrefix = "config-secrets-"

// codecAnnotationPrefix is used to round-trip codec names across writes/reads.
// Each k8sKey stored in a ConfigMap/Secret has a matching annotation
// "config.rbaliyan.dev/codec-<k8sKey>" with its codec name.
const codecAnnotationPrefix = "config.rbaliyan.dev/codec-"

func codecAnnotationKey(k8sKey string) string {
	return codecAnnotationPrefix + k8sKey
}

// resolveCodec returns the stored codec name for a given k8sKey or "json" as fallback.
func resolveCodec(anns map[string]string, k8sKey string) string {
	if anns != nil {
		if c := anns[codecAnnotationKey(k8sKey)]; c != "" {
			return c
		}
	}
	return "json"
}

// Store implements config.Store backed by Kubernetes ConfigMaps and Secrets.
type Store struct {
	client  kubernetes.Interface
	factory informers.SharedInformerFactory
	opts    storeOptions

	closed   atomic.Bool
	stopChan chan struct{}

	watchMu  sync.RWMutex
	watchers map[*watchEntry]struct{}
}

var (
	_ config.Store         = (*Store)(nil)
	_ config.HealthChecker = (*Store)(nil)
)

// NewStore creates a new k8s-backed config store.
// Call Connect before using the store.
func NewStore(client kubernetes.Interface, opts ...Option) *Store {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	return &Store{
		client:   client,
		opts:     o,
		stopChan: make(chan struct{}),
		watchers: make(map[*watchEntry]struct{}),
	}
}

// BackendName returns the stable backend identifier used in error messages.
func (s *Store) BackendName() string { return "k8s" }

// Connect starts the informer factory and waits for the initial cache sync.
// The context deadline/timeout controls how long to wait for cache sync.
func (s *Store) Connect(ctx context.Context) error {
	if s.closed.Load() {
		return config.ErrStoreClosed
	}

	var factoryOpts []informers.SharedInformerOption
	if s.opts.k8sNamespace != "" {
		factoryOpts = append(factoryOpts, informers.WithNamespace(s.opts.k8sNamespace))
	}

	s.factory = informers.NewSharedInformerFactoryWithOptions(
		s.client,
		s.opts.resyncPeriod,
		factoryOpts...,
	)

	cmInformer := s.factory.Core().V1().ConfigMaps().Informer()
	secInformer := s.factory.Core().V1().Secrets().Informer()

	if _, err := cmInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { s.onConfigMapAdd(obj) },
		UpdateFunc: func(old, newObj any) { s.onConfigMapUpdate(old, newObj) },
		DeleteFunc: func(obj any) { s.onConfigMapDelete(obj) },
	}); err != nil {
		return config.WrapStoreError("connect", "k8s", "configmap-handler", err)
	}

	if _, err := secInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { s.onSecretAdd(obj) },
		UpdateFunc: func(old, newObj any) { s.onSecretUpdate(old, newObj) },
		DeleteFunc: func(obj any) { s.onSecretDelete(obj) },
	}); err != nil {
		return config.WrapStoreError("connect", "k8s", "secret-handler", err)
	}

	s.factory.Start(s.stopChan)

	if !cache.WaitForCacheSync(ctx.Done(), cmInformer.HasSynced, secInformer.HasSynced) {
		err := ctx.Err()
		if err == nil {
			err = fmt.Errorf("cache sync timed out")
		}
		return config.WrapStoreError("connect", "k8s", "cache-sync", err)
	}
	return nil
}

// Close stops the informer factory and closes all watch channels.
// Close blocks until informer goroutines have drained so callers can
// safely release any resources they injected into the client.
func (s *Store) Close(_ context.Context) error {
	if s.closed.Swap(true) {
		return nil // already closed
	}

	close(s.stopChan)

	// Shutdown blocks until all informers have fully stopped. It is safe
	// to call even if Start was never invoked.
	if s.factory != nil {
		s.factory.Shutdown()
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
// Reads from the informer cache (no API call).
func (s *Store) Get(ctx context.Context, namespace, key string) (config.Value, error) {
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
	}
	if s.factory == nil {
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

	if isSecretKey(key, s.opts.secretPrefix) {
		secName := secretResourceName(namespace)
		lister := s.factory.Core().V1().Secrets().Lister()
		secret, err := lister.Secrets(k8sNS).Get(secName)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil, &config.KeyNotFoundError{Key: key, Namespace: namespace}
			}
			return nil, config.WrapStoreError("get", "k8s", key, err)
		}
		rawBytes, ok := secret.Data[k8sKey]
		if !ok {
			return nil, &config.KeyNotFoundError{Key: key, Namespace: namespace}
		}
		return makeValueFromBytes(rawBytes, namespace, key, secret.ResourceVersion, resolveCodec(secret.Annotations, k8sKey))
	}

	cmName := configMapResourceName(namespace)
	lister := s.factory.Core().V1().ConfigMaps().Lister()
	cm, err := lister.ConfigMaps(k8sNS).Get(cmName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, &config.KeyNotFoundError{Key: key, Namespace: namespace}
		}
		return nil, config.WrapStoreError("get", "k8s", key, err)
	}
	rawStr, ok := cm.Data[k8sKey]
	if !ok {
		return nil, &config.KeyNotFoundError{Key: key, Namespace: namespace}
	}
	return makeValueFromString(rawStr, namespace, key, cm.ResourceVersion, resolveCodec(cm.Annotations, k8sKey))
}

// Set creates or updates a configuration value.
// Writes directly to the Kubernetes API (not via informer cache).
func (s *Store) Set(ctx context.Context, namespace, key string, value config.Value) (config.Value, error) {
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
	}
	if s.factory == nil {
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

	if isSecretKey(key, s.opts.secretPrefix) {
		return s.setSecret(ctx, namespace, key, k8sNS, k8sKey, codecName, data)
	}
	return s.setConfigMap(ctx, namespace, key, k8sNS, k8sKey, codecName, string(data))
}

func (s *Store) setConfigMap(ctx context.Context, namespace, key, k8sNS, k8sKey, codecName, strVal string) (config.Value, error) {
	cmName := configMapResourceName(namespace)
	cm, err := s.client.CoreV1().ConfigMaps(k8sNS).Get(ctx, cmName, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, config.WrapStoreError("set", "k8s", key, err)
	}
	if err != nil {
		// Create a new ConfigMap.
		cm = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:        cmName,
				Namespace:   k8sNS,
				Annotations: map[string]string{codecAnnotationKey(k8sKey): codecName},
			},
			Data: map[string]string{k8sKey: strVal},
		}
		created, createErr := s.client.CoreV1().ConfigMaps(k8sNS).Create(ctx, cm, metav1.CreateOptions{})
		if createErr != nil {
			return nil, config.WrapStoreError("set", "k8s", key, createErr)
		}
		return makeValueFromString(strVal, namespace, key, created.ResourceVersion, codecName)
	}

	if cm.Data == nil {
		cm.Data = make(map[string]string)
	}
	if cm.Annotations == nil {
		cm.Annotations = make(map[string]string)
	}
	cm.Data[k8sKey] = strVal
	cm.Annotations[codecAnnotationKey(k8sKey)] = codecName

	updated, updateErr := s.client.CoreV1().ConfigMaps(k8sNS).Update(ctx, cm, metav1.UpdateOptions{})
	if updateErr != nil {
		return nil, config.WrapStoreError("set", "k8s", key, updateErr)
	}
	return makeValueFromString(strVal, namespace, key, updated.ResourceVersion, codecName)
}

func (s *Store) setSecret(ctx context.Context, namespace, key, k8sNS, k8sKey, codecName string, rawBytes []byte) (config.Value, error) {
	secName := secretResourceName(namespace)
	sec, err := s.client.CoreV1().Secrets(k8sNS).Get(ctx, secName, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, config.WrapStoreError("set", "k8s", key, err)
	}
	if err != nil {
		// Create a new Secret.
		sec = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:        secName,
				Namespace:   k8sNS,
				Annotations: map[string]string{codecAnnotationKey(k8sKey): codecName},
			},
			Data: map[string][]byte{k8sKey: rawBytes},
		}
		created, createErr := s.client.CoreV1().Secrets(k8sNS).Create(ctx, sec, metav1.CreateOptions{})
		if createErr != nil {
			return nil, config.WrapStoreError("set", "k8s", key, createErr)
		}
		return makeValueFromBytes(rawBytes, namespace, key, created.ResourceVersion, codecName)
	}

	if sec.Data == nil {
		sec.Data = make(map[string][]byte)
	}
	if sec.Annotations == nil {
		sec.Annotations = make(map[string]string)
	}
	sec.Data[k8sKey] = rawBytes
	sec.Annotations[codecAnnotationKey(k8sKey)] = codecName

	updated, updateErr := s.client.CoreV1().Secrets(k8sNS).Update(ctx, sec, metav1.UpdateOptions{})
	if updateErr != nil {
		return nil, config.WrapStoreError("set", "k8s", key, updateErr)
	}
	return makeValueFromBytes(rawBytes, namespace, key, updated.ResourceVersion, codecName)
}

// Delete removes a configuration entry.
func (s *Store) Delete(ctx context.Context, namespace, key string) error {
	if s.closed.Load() {
		return config.ErrStoreClosed
	}
	if s.factory == nil {
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

	if isSecretKey(key, s.opts.secretPrefix) {
		return s.deleteFromSecret(ctx, namespace, key, k8sNS, k8sKey)
	}
	return s.deleteFromConfigMap(ctx, namespace, key, k8sNS, k8sKey)
}

func (s *Store) deleteFromConfigMap(ctx context.Context, namespace, key, k8sNS, k8sKey string) error {
	cmName := configMapResourceName(namespace)
	cm, err := s.client.CoreV1().ConfigMaps(k8sNS).Get(ctx, cmName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return &config.KeyNotFoundError{Key: key, Namespace: namespace}
		}
		return config.WrapStoreError("delete", "k8s", key, err)
	}
	if _, ok := cm.Data[k8sKey]; !ok {
		return &config.KeyNotFoundError{Key: key, Namespace: namespace}
	}
	delete(cm.Data, k8sKey)
	delete(cm.Annotations, codecAnnotationKey(k8sKey))
	_, updateErr := s.client.CoreV1().ConfigMaps(k8sNS).Update(ctx, cm, metav1.UpdateOptions{})
	if updateErr != nil {
		return config.WrapStoreError("delete", "k8s", key, updateErr)
	}
	return nil
}

func (s *Store) deleteFromSecret(ctx context.Context, namespace, key, k8sNS, k8sKey string) error {
	secName := secretResourceName(namespace)
	sec, err := s.client.CoreV1().Secrets(k8sNS).Get(ctx, secName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return &config.KeyNotFoundError{Key: key, Namespace: namespace}
		}
		return config.WrapStoreError("delete", "k8s", key, err)
	}
	if _, ok := sec.Data[k8sKey]; !ok {
		return &config.KeyNotFoundError{Key: key, Namespace: namespace}
	}
	delete(sec.Data, k8sKey)
	delete(sec.Annotations, codecAnnotationKey(k8sKey))
	_, updateErr := s.client.CoreV1().Secrets(k8sNS).Update(ctx, sec, metav1.UpdateOptions{})
	if updateErr != nil {
		return config.WrapStoreError("delete", "k8s", key, updateErr)
	}
	return nil
}

// Find returns a page of configuration entries matching the filter.
// Reads from the informer cache (no API call).
func (s *Store) Find(ctx context.Context, namespace string, filter config.Filter) (config.Page, error) {
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
	}
	if s.factory == nil {
		return nil, config.ErrStoreNotConnected
	}
	if err := config.ValidateNamespace(namespace); err != nil {
		return nil, err
	}
	if filter == nil {
		filter = config.NewFilter().Build()
	}

	k8sNS := s.k8sNamespace(namespace)
	cmName := configMapResourceName(namespace)

	// Keys mode: exact key lookup.
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

	// Prefix mode: scan ConfigMap data.
	prefix := filter.Prefix()
	cursor := filter.Cursor()
	limit := filter.Limit()

	lister := s.factory.Core().V1().ConfigMaps().Lister()
	cm, err := lister.ConfigMaps(k8sNS).Get(cmName)
	if err != nil {
		// No ConfigMap yet — return empty page.
		return config.NewPage(make(map[string]config.Value), "", limit), nil
	}

	// Collect and sort config keys for consistent pagination.
	type kv struct {
		configKey string
		rawVal    string
	}
	var matches []kv
	for k8sKey, rawVal := range cm.Data {
		configKey := k8sKeyToConfigKey(k8sKey)
		if prefix != "" && !strings.HasPrefix(configKey, prefix) {
			continue
		}
		// Cursor is the last config key from the previous page.
		if cursor != "" && configKey <= cursor {
			continue
		}
		matches = append(matches, kv{configKey: configKey, rawVal: rawVal})
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
		v, valErr := makeValueFromString(m.rawVal, namespace, m.configKey, cm.ResourceVersion, resolveCodec(cm.Annotations, k8sKey))
		if valErr == nil {
			results[m.configKey] = v
			lastKey = m.configKey
		}
	}

	return config.NewPage(results, lastKey, limit), nil
}

// Watch returns a channel that receives change events for matching keys.
func (s *Store) Watch(ctx context.Context, filter config.WatchFilter) (<-chan config.ChangeEvent, error) {
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
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
		case <-s.stopChan:
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

// Health performs a basic health check by listing namespaces.
func (s *Store) Health(ctx context.Context) error {
	if s.closed.Load() {
		return config.ErrStoreClosed
	}
	if s.factory == nil {
		return config.ErrStoreNotConnected
	}
	_, err := s.client.CoreV1().Namespaces().List(ctx, metav1.ListOptions{Limit: 1})
	if err != nil {
		return config.WrapStoreError("health", "k8s", "", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Informer event handlers
// ---------------------------------------------------------------------------

func (s *Store) onConfigMapAdd(obj any) {
	cm, ok := obj.(*corev1.ConfigMap)
	if !ok {
		return
	}
	ns := s.configNamespace(cm.Namespace, cm.Name)
	if ns == "" {
		return
	}
	now := time.Now().UTC()
	for k8sKey, rawVal := range cm.Data {
		configKey := k8sKeyToConfigKey(k8sKey)
		v, err := makeValueFromString(rawVal, ns, configKey, cm.ResourceVersion, resolveCodec(cm.Annotations, k8sKey))
		if err != nil {
			continue
		}
		s.notifyWatchers(config.ChangeEvent{
			Type:      config.ChangeTypeSet,
			Namespace: ns,
			Key:       configKey,
			Value:     v,
			Timestamp: now,
		})
	}
}

func (s *Store) onConfigMapUpdate(old, newObj any) {
	oldCM, ok1 := old.(*corev1.ConfigMap)
	newCM, ok2 := newObj.(*corev1.ConfigMap)
	if !ok1 || !ok2 {
		return
	}
	ns := s.configNamespace(newCM.Namespace, newCM.Name)
	if ns == "" {
		return
	}
	now := time.Now().UTC()

	// Keys added or updated.
	for k8sKey, newVal := range newCM.Data {
		configKey := k8sKeyToConfigKey(k8sKey)
		oldVal, existed := oldCM.Data[k8sKey]
		if !existed || oldVal != newVal {
			v, err := makeValueFromString(newVal, ns, configKey, newCM.ResourceVersion, resolveCodec(newCM.Annotations, k8sKey))
			if err != nil {
				continue
			}
			s.notifyWatchers(config.ChangeEvent{
				Type:      config.ChangeTypeSet,
				Namespace: ns,
				Key:       configKey,
				Value:     v,
				Timestamp: now,
			})
		}
	}

	// Keys deleted.
	for k8sKey := range oldCM.Data {
		if _, ok := newCM.Data[k8sKey]; !ok {
			configKey := k8sKeyToConfigKey(k8sKey)
			s.notifyWatchers(config.ChangeEvent{
				Type:      config.ChangeTypeDelete,
				Namespace: ns,
				Key:       configKey,
				Timestamp: now,
			})
		}
	}
}

func (s *Store) onConfigMapDelete(obj any) {
	cm, ok := obj.(*corev1.ConfigMap)
	if !ok {
		// Handle DeletedFinalStateUnknown tombstone.
		tombstone, isTombstone := obj.(cache.DeletedFinalStateUnknown)
		if !isTombstone {
			return
		}
		cm, ok = tombstone.Obj.(*corev1.ConfigMap)
		if !ok {
			return
		}
	}
	ns := s.configNamespace(cm.Namespace, cm.Name)
	if ns == "" {
		return
	}
	now := time.Now().UTC()
	for k8sKey := range cm.Data {
		configKey := k8sKeyToConfigKey(k8sKey)
		s.notifyWatchers(config.ChangeEvent{
			Type:      config.ChangeTypeDelete,
			Namespace: ns,
			Key:       configKey,
			Timestamp: now,
		})
	}
}

func (s *Store) onSecretAdd(obj any) {
	sec, ok := obj.(*corev1.Secret)
	if !ok {
		return
	}
	ns := s.configNamespace(sec.Namespace, sec.Name)
	if ns == "" {
		return
	}
	now := time.Now().UTC()
	for k8sKey, rawBytes := range sec.Data {
		configKey := k8sKeyToConfigKey(k8sKey)
		v, err := makeValueFromBytes(rawBytes, ns, configKey, sec.ResourceVersion, resolveCodec(sec.Annotations, k8sKey))
		if err != nil {
			continue
		}
		s.notifyWatchers(config.ChangeEvent{
			Type:      config.ChangeTypeSet,
			Namespace: ns,
			Key:       configKey,
			Value:     v,
			Timestamp: now,
		})
	}
}

func (s *Store) onSecretUpdate(old, newObj any) {
	oldSec, ok1 := old.(*corev1.Secret)
	newSec, ok2 := newObj.(*corev1.Secret)
	if !ok1 || !ok2 {
		return
	}
	ns := s.configNamespace(newSec.Namespace, newSec.Name)
	if ns == "" {
		return
	}
	now := time.Now().UTC()

	for k8sKey, newBytes := range newSec.Data {
		configKey := k8sKeyToConfigKey(k8sKey)
		oldBytes, existed := oldSec.Data[k8sKey]
		if !existed || string(oldBytes) != string(newBytes) {
			v, err := makeValueFromBytes(newBytes, ns, configKey, newSec.ResourceVersion, resolveCodec(newSec.Annotations, k8sKey))
			if err != nil {
				continue
			}
			s.notifyWatchers(config.ChangeEvent{
				Type:      config.ChangeTypeSet,
				Namespace: ns,
				Key:       configKey,
				Value:     v,
				Timestamp: now,
			})
		}
	}

	for k8sKey := range oldSec.Data {
		if _, ok := newSec.Data[k8sKey]; !ok {
			configKey := k8sKeyToConfigKey(k8sKey)
			s.notifyWatchers(config.ChangeEvent{
				Type:      config.ChangeTypeDelete,
				Namespace: ns,
				Key:       configKey,
				Timestamp: now,
			})
		}
	}
}

func (s *Store) onSecretDelete(obj any) {
	sec, ok := obj.(*corev1.Secret)
	if !ok {
		tombstone, isTombstone := obj.(cache.DeletedFinalStateUnknown)
		if !isTombstone {
			return
		}
		sec, ok = tombstone.Obj.(*corev1.Secret)
		if !ok {
			return
		}
	}
	ns := s.configNamespace(sec.Namespace, sec.Name)
	if ns == "" {
		return
	}
	now := time.Now().UTC()
	for k8sKey := range sec.Data {
		configKey := k8sKeyToConfigKey(k8sKey)
		s.notifyWatchers(config.ChangeEvent{
			Type:      config.ChangeTypeDelete,
			Namespace: ns,
			Key:       configKey,
			Timestamp: now,
		})
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// k8sNamespace returns the Kubernetes namespace to use for a given config namespace.
// If opts.k8sNamespace is set, all config namespaces map to it.
// Otherwise the config namespace name is used directly.
func (s *Store) k8sNamespace(configNS string) string {
	if s.opts.k8sNamespace != "" {
		return s.opts.k8sNamespace
	}
	if configNS == "" {
		return "default"
	}
	return configNS
}

// configNamespace reverses the k8s resource name back to the config namespace.
// Returns "" if the resource is not managed by this store.
func (s *Store) configNamespace(k8sNS, resourceName string) string {
	if strings.HasPrefix(resourceName, secretNamePrefix) {
		return strings.TrimPrefix(resourceName, secretNamePrefix)
	}
	if strings.HasPrefix(resourceName, configMapPrefix) {
		return strings.TrimPrefix(resourceName, configMapPrefix)
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

// makeValueFromString creates a config.Value from a ConfigMap string value.
// codecName should be the value's codec as recorded in the ConfigMap annotations;
// fall back to "json" if unknown.
func makeValueFromString(rawStr, namespace, key, resourceVersion, codecName string) (config.Value, error) {
	if codecName == "" {
		codecName = "json"
	}
	version := parseResourceVersion(resourceVersion)
	entryID := namespace + "/" + key
	return config.NewValueFromBytes(
		context.Background(),
		[]byte(rawStr),
		codecName,
		config.WithValueMetadata(version, time.Time{}, time.Time{}),
		config.WithValueEntryID(entryID),
	)
}

// makeValueFromBytes creates a config.Value from a Secret byte value.
func makeValueFromBytes(rawBytes []byte, namespace, key, resourceVersion, codecName string) (config.Value, error) {
	if codecName == "" {
		codecName = "json"
	}
	version := parseResourceVersion(resourceVersion)
	entryID := namespace + "/" + key
	return config.NewValueFromBytes(
		context.Background(),
		rawBytes,
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
