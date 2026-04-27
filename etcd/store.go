// Package etcd provides an etcd v3-backed config.Store.
//
// Values are stored as JSON envelopes containing the codec name and
// base64-encoded payload so that the codec is preserved across round-trips:
//
//	{"codec":"json","type":1,"version":3,"created_at":"...","updated_at":"...","value":"<base64>"}
//
// Key mapping: etcd key = "/<namespace>/<key>" (or "/<key>" for the default
// empty namespace).
package etcd

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/rbaliyan/config"
)

// Compile-time interface checks.
var (
	_ config.Store     = (*Store)(nil)
	_ config.BulkStore = (*Store)(nil)
)

// envelope is the JSON structure stored in etcd.
type envelope struct {
	Codec     string      `json:"codec"`
	Type      config.Type `json:"type"`
	Version   int64       `json:"version"`
	CreatedAt time.Time   `json:"created_at"`
	UpdatedAt time.Time   `json:"updated_at"`
	Value     string      `json:"value"` // base64-encoded raw bytes
}

// watchEntry is a single active Watch subscription.
type watchEntry struct {
	filter    config.WatchFilter
	ch        chan config.ChangeEvent
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.Mutex
	closed    bool
	closeOnce sync.Once
}

// options holds the functional-options configuration for the Store.
type options struct {
	watchBufferSize int
	logger          *slog.Logger
}

// Option configures the etcd store.
type Option func(*options)

// WithWatchBufferSize sets the channel buffer size for Watch subscriptions.
// Default is 64.
func WithWatchBufferSize(n int) Option {
	return func(o *options) {
		if n > 0 {
			o.watchBufferSize = n
		}
	}
}

// WithLogger sets a custom logger for the store.
func WithLogger(logger *slog.Logger) Option {
	return func(o *options) {
		if logger != nil {
			o.logger = logger
		}
	}
}

// Store is an etcd v3-backed config.Store.
type Store struct {
	client    *clientv3.Client
	namespace string
	opts      options
	closed    atomic.Bool

	watchMu       sync.RWMutex
	watchers      map[*watchEntry]struct{}
	stopWatch     chan struct{}
	watchWg       sync.WaitGroup
	droppedEvents atomic.Int64
}

// New creates a new etcd v3 store.
// client must be a connected *clientv3.Client.
// namespace is used as the etcd key prefix: "/<namespace>/<key>".
// An empty namespace stores keys at "/<key>".
func New(client *clientv3.Client, namespace string, opts ...Option) (*Store, error) {
	if client == nil {
		return nil, fmt.Errorf("etcd: client must not be nil")
	}
	if err := config.ValidateNamespace(namespace); err != nil {
		return nil, err
	}

	o := options{
		watchBufferSize: 64,
		logger:          slog.Default(),
	}
	for _, opt := range opts {
		opt(&o)
	}

	return &Store{
		client:    client,
		namespace: namespace,
		opts:      o,
		watchers:  make(map[*watchEntry]struct{}),
		stopWatch: make(chan struct{}),
	}, nil
}

// etcdKey returns the full etcd key for a (namespace, key) pair.
func (s *Store) etcdKey(namespace, key string) string {
	if namespace == "" {
		return "/" + key
	}
	return "/" + namespace + "/" + key
}

// namespacePrefix returns the etcd key prefix covering all keys in s.namespace.
func (s *Store) namespacePrefix() string {
	if s.namespace == "" {
		return "/"
	}
	return "/" + s.namespace + "/"
}

// parseKey extracts the config key from a full etcd key.
func (s *Store) parseKey(etcdKey string) string {
	prefix := s.namespacePrefix()
	return strings.TrimPrefix(etcdKey, prefix)
}

// Connect is a no-op; the etcd client handles connectivity internally.
func (s *Store) Connect(_ context.Context) error {
	return nil
}

// Close stops all active Watch goroutines.
func (s *Store) Close(_ context.Context) error {
	if s.closed.Swap(true) {
		return nil
	}

	close(s.stopWatch)
	s.watchWg.Wait()

	s.watchMu.Lock()
	for we := range s.watchers {
		we.cancel()
		we.closeOnce.Do(func() {
			we.mu.Lock()
			we.closed = true
			close(we.ch)
			we.mu.Unlock()
		})
	}
	s.watchers = make(map[*watchEntry]struct{})
	s.watchMu.Unlock()

	return nil
}

// Get retrieves a configuration value by namespace and key.
func (s *Store) Get(ctx context.Context, namespace, key string) (config.Value, error) {
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
	}
	if err := config.ValidateNamespace(namespace); err != nil {
		return nil, err
	}

	resp, err := s.client.Get(ctx, s.etcdKey(namespace, key))
	if err != nil {
		return nil, config.WrapStoreError("get", "etcd", key, err)
	}
	if len(resp.Kvs) == 0 {
		return nil, &config.KeyNotFoundError{Key: key, Namespace: namespace}
	}

	return s.kvToValue(ctx, resp.Kvs[0], key)
}

// Set creates or updates a configuration value.
func (s *Store) Set(ctx context.Context, namespace, key string, value config.Value) (config.Value, error) {
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
	}
	if err := config.ValidateNamespace(namespace); err != nil {
		return nil, err
	}
	if err := config.ValidateKey(key); err != nil {
		return nil, err
	}

	data, err := value.Marshal(ctx)
	if err != nil {
		return nil, config.WrapStoreError("marshal", "etcd", key, err)
	}

	eKey := s.etcdKey(namespace, key)
	now := time.Now().UTC()
	writeMode := config.GetWriteMode(value)

	switch writeMode {
	case config.WriteModeCreate:
		// Read-then-CAS to ensure key does not exist.
		existing, err := s.client.Get(ctx, eKey)
		if err != nil {
			return nil, config.WrapStoreError("get", "etcd", key, err)
		}
		if len(existing.Kvs) > 0 {
			return nil, &config.KeyExistsError{Key: key, Namespace: namespace}
		}
		env := envelope{
			Codec:     value.Codec(),
			Type:      value.Type(),
			Version:   1,
			CreatedAt: now,
			UpdatedAt: now,
			Value:     base64.StdEncoding.EncodeToString(data),
		}
		raw, err := json.Marshal(env)
		if err != nil {
			return nil, config.WrapStoreError("encode", "etcd", key, err)
		}
		// Use a transaction: create only if the key does not yet exist.
		txnResp, err := s.client.Txn(ctx).
			If(clientv3.Compare(clientv3.Version(eKey), "=", 0)).
			Then(clientv3.OpPut(eKey, string(raw))).
			Commit()
		if err != nil {
			return nil, config.WrapStoreError("set", "etcd", key, err)
		}
		if !txnResp.Succeeded {
			return nil, &config.KeyExistsError{Key: key, Namespace: namespace}
		}
		return config.NewValueFromBytes(ctx, data, value.Codec(),
			config.WithValueType(value.Type()),
			config.WithValueMetadata(1, now, now),
		)

	case config.WriteModeUpdate:
		existing, err := s.client.Get(ctx, eKey)
		if err != nil {
			return nil, config.WrapStoreError("get", "etcd", key, err)
		}
		if len(existing.Kvs) == 0 {
			return nil, &config.KeyNotFoundError{Key: key, Namespace: namespace}
		}
		prev, decErr := decodeEnvelope(existing.Kvs[0].Value)
		if decErr != nil {
			prev = &envelope{Version: 0, CreatedAt: now}
		}
		newVersion := prev.Version + 1
		env := envelope{
			Codec:     value.Codec(),
			Type:      value.Type(),
			Version:   newVersion,
			CreatedAt: prev.CreatedAt,
			UpdatedAt: now,
			Value:     base64.StdEncoding.EncodeToString(data),
		}
		raw, err := json.Marshal(env)
		if err != nil {
			return nil, config.WrapStoreError("encode", "etcd", key, err)
		}
		if _, err := s.client.Put(ctx, eKey, string(raw)); err != nil {
			return nil, config.WrapStoreError("set", "etcd", key, err)
		}
		return config.NewValueFromBytes(ctx, data, value.Codec(),
			config.WithValueType(value.Type()),
			config.WithValueMetadata(newVersion, prev.CreatedAt, now),
		)

	default: // Upsert
		existing, err := s.client.Get(ctx, eKey)
		if err != nil {
			return nil, config.WrapStoreError("get", "etcd", key, err)
		}
		var createdAt time.Time
		var version int64
		if len(existing.Kvs) > 0 {
			if prev, decErr := decodeEnvelope(existing.Kvs[0].Value); decErr == nil {
				createdAt = prev.CreatedAt
				version = prev.Version
			}
		}
		if createdAt.IsZero() {
			createdAt = now
		}
		version++
		env := envelope{
			Codec:     value.Codec(),
			Type:      value.Type(),
			Version:   version,
			CreatedAt: createdAt,
			UpdatedAt: now,
			Value:     base64.StdEncoding.EncodeToString(data),
		}
		raw, err := json.Marshal(env)
		if err != nil {
			return nil, config.WrapStoreError("encode", "etcd", key, err)
		}
		if _, err := s.client.Put(ctx, eKey, string(raw)); err != nil {
			return nil, config.WrapStoreError("set", "etcd", key, err)
		}
		return config.NewValueFromBytes(ctx, data, value.Codec(),
			config.WithValueType(value.Type()),
			config.WithValueMetadata(version, createdAt, now),
		)
	}
}

// Delete removes a configuration value by namespace and key.
func (s *Store) Delete(ctx context.Context, namespace, key string) error {
	if s.closed.Load() {
		return config.ErrStoreClosed
	}
	if err := config.ValidateNamespace(namespace); err != nil {
		return err
	}

	resp, err := s.client.Delete(ctx, s.etcdKey(namespace, key))
	if err != nil {
		return config.WrapStoreError("delete", "etcd", key, err)
	}
	if resp.Deleted == 0 {
		return &config.KeyNotFoundError{Key: key, Namespace: namespace}
	}
	return nil
}

// Find returns a page of keys and values matching the filter within a namespace.
func (s *Store) Find(ctx context.Context, namespace string, filter config.Filter) (config.Page, error) {
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
	}
	if filter == nil {
		filter = config.NewFilter().Build()
	}
	if err := config.ValidateNamespace(namespace); err != nil {
		return nil, err
	}

	// Keys mode: fetch each key individually.
	if keys := filter.Keys(); len(keys) > 0 {
		results := make(map[string]config.Value, len(keys))
		for _, key := range keys {
			resp, err := s.client.Get(ctx, s.etcdKey(namespace, key))
			if err != nil {
				return nil, config.WrapStoreError("find", "etcd", key, err)
			}
			if len(resp.Kvs) == 0 {
				continue
			}
			val, err := s.kvToValue(ctx, resp.Kvs[0], key)
			if err != nil {
				s.log().Warn("etcd: skipping corrupt entry in find", "key", key, "error", err)
				continue
			}
			results[key] = val
		}
		return config.NewPage(results, "", 0), nil
	}

	// Prefix mode: range query under namespace prefix + user prefix.
	nsPrefix := s.namespacePrefix()
	userPrefix := filter.Prefix()
	listPrefix := nsPrefix + userPrefix

	// Build range end for prefix scan.
	rangeEnd := clientv3.GetPrefixRangeEnd(listPrefix)

	// Cursor-based pagination: cursor is the last full etcd key returned in a
	// previous page; resume from the key immediately after it.
	cursor := filter.Cursor()
	var startKey string
	if cursor != "" {
		startKey = cursor + "\x00" // first key lexicographically after cursor
	} else {
		startKey = listPrefix
	}

	getOpts := []clientv3.OpOption{
		clientv3.WithRange(rangeEnd),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	}

	limit := filter.Limit()
	if limit > 0 {
		getOpts = append(getOpts, clientv3.WithLimit(int64(limit)))
	}

	resp, err := s.client.Get(ctx, startKey, getOpts...)
	if err != nil {
		return nil, config.WrapStoreError("find", "etcd", "", err)
	}

	// Sort for deterministic output (WithSort should handle this, but be explicit).
	kvs := resp.Kvs
	sort.Slice(kvs, func(i, j int) bool {
		return string(kvs[i].Key) < string(kvs[j].Key)
	})

	results := make(map[string]config.Value, len(kvs))
	var lastKey string
	for _, kv := range kvs {
		fullKey := string(kv.Key)
		if !strings.HasPrefix(fullKey, listPrefix) {
			continue
		}
		configKey := s.parseKey(fullKey)
		val, err := s.kvToValue(ctx, kv, configKey)
		if err != nil {
			s.log().Warn("etcd: skipping corrupt entry in find", "key", configKey, "error", err)
			lastKey = fullKey
			continue
		}
		results[configKey] = val
		lastKey = fullKey
	}

	return config.NewPage(results, lastKey, limit), nil
}

// Watch returns a channel that receives change events.
// It uses etcd's native Watch API to receive real-time change notifications.
func (s *Store) Watch(ctx context.Context, filter config.WatchFilter) (<-chan config.ChangeEvent, error) {
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
	}

	ctx, cancel := context.WithCancel(ctx)
	ch := make(chan config.ChangeEvent, s.opts.watchBufferSize)

	we := &watchEntry{
		filter: filter,
		ch:     ch,
		ctx:    ctx,
		cancel: cancel,
	}

	s.watchMu.Lock()
	s.watchers[we] = struct{}{}
	s.watchMu.Unlock()

	s.watchWg.Add(1)
	go s.watchLoop(we)

	return ch, nil
}

// watchLoop subscribes to etcd changes for the namespace prefix and dispatches events.
func (s *Store) watchLoop(we *watchEntry) {
	defer s.watchWg.Done()
	defer func() {
		s.watchMu.Lock()
		delete(s.watchers, we)
		s.watchMu.Unlock()

		we.closeOnce.Do(func() {
			we.mu.Lock()
			we.closed = true
			close(we.ch)
			we.mu.Unlock()
		})
	}()

	nsPrefix := s.namespacePrefix()
	watchChan := s.client.Watch(we.ctx, nsPrefix, clientv3.WithPrefix())

	for {
		select {
		case <-we.ctx.Done():
			return
		case <-s.stopWatch:
			return
		case wresp, ok := <-watchChan:
			if !ok {
				// Channel closed — context cancelled or fatal error.
				return
			}
			if wresp.Err() != nil {
				s.log().Warn("etcd: watch response error", "error", wresp.Err())
				continue
			}
			for _, ev := range wresp.Events {
				s.handleWatchEvent(we, ev)
			}
		}
	}
}

// handleWatchEvent converts an etcd watch event to a config.ChangeEvent and dispatches it.
func (s *Store) handleWatchEvent(we *watchEntry, ev *clientv3.Event) {
	if ev.Kv == nil {
		return
	}
	fullKey := string(ev.Kv.Key)
	configKey := s.parseKey(fullKey)

	var event config.ChangeEvent
	event.Namespace = s.namespace
	event.Key = configKey
	event.Timestamp = time.Now().UTC()

	switch ev.Type {
	case clientv3.EventTypeDelete:
		event.Type = config.ChangeTypeDelete
	case clientv3.EventTypePut:
		event.Type = config.ChangeTypeSet
		val, err := s.kvToValue(we.ctx, ev.Kv, configKey)
		if err != nil {
			s.log().Warn("etcd: skipping corrupt entry in watch", "key", configKey, "error", err)
			return
		}
		event.Value = val
	default:
		return
	}

	if config.MatchesWatchFilter(event, we.filter) {
		s.sendToWatcher(we, event)
	}
}

// sendToWatcher non-blockingly sends an event to a watcher channel.
func (s *Store) sendToWatcher(we *watchEntry, event config.ChangeEvent) {
	we.mu.Lock()
	if we.closed {
		we.mu.Unlock()
		return
	}
	select {
	case we.ch <- event:
	case <-we.ctx.Done():
	default:
		s.droppedEvents.Add(1)
	}
	we.mu.Unlock()
}

// GetMany retrieves multiple values in a single operation (BulkStore).
func (s *Store) GetMany(ctx context.Context, namespace string, keys []string) (map[string]config.Value, error) {
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
	}
	if err := config.ValidateNamespace(namespace); err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return make(map[string]config.Value), nil
	}

	results := make(map[string]config.Value, len(keys))
	for _, key := range keys {
		resp, err := s.client.Get(ctx, s.etcdKey(namespace, key))
		if err != nil {
			return nil, config.WrapStoreError("get_many", "etcd", key, err)
		}
		if len(resp.Kvs) == 0 {
			continue
		}
		val, err := s.kvToValue(ctx, resp.Kvs[0], key)
		if err != nil {
			s.log().Warn("etcd: skipping corrupt entry in get_many", "key", key, "error", err)
			continue
		}
		results[key] = val
	}
	return results, nil
}

// SetMany creates or updates multiple values in a single operation (BulkStore).
func (s *Store) SetMany(ctx context.Context, namespace string, values map[string]config.Value) error {
	if s.closed.Load() {
		return config.ErrStoreClosed
	}
	if err := config.ValidateNamespace(namespace); err != nil {
		return err
	}
	if len(values) == 0 {
		return nil
	}

	keyErrors := make(map[string]error)
	succeeded := make([]string, 0, len(values))

	for key, value := range values {
		if err := config.ValidateKey(key); err != nil {
			keyErrors[key] = err
			continue
		}
		if _, err := s.Set(ctx, namespace, key, value); err != nil {
			keyErrors[key] = err
			continue
		}
		succeeded = append(succeeded, key)
	}

	if len(keyErrors) > 0 {
		return &config.BulkWriteError{
			Errors:    keyErrors,
			Succeeded: succeeded,
		}
	}
	return nil
}

// DeleteMany removes multiple values (BulkStore). Returns number deleted.
func (s *Store) DeleteMany(ctx context.Context, namespace string, keys []string) (int64, error) {
	if s.closed.Load() {
		return 0, config.ErrStoreClosed
	}
	if err := config.ValidateNamespace(namespace); err != nil {
		return 0, err
	}
	if len(keys) == 0 {
		return 0, nil
	}

	var deleted int64
	for _, key := range keys {
		err := s.Delete(ctx, namespace, key)
		if err == nil {
			deleted++
		} else if !config.IsNotFound(err) {
			return deleted, err
		}
	}
	return deleted, nil
}

// DroppedEvents returns the count of watch events dropped due to full channels.
func (s *Store) DroppedEvents() int64 {
	return s.droppedEvents.Load()
}

// kvToValue converts an etcd KeyValue to a config.Value by decoding the envelope.
func (s *Store) kvToValue(ctx context.Context, kv *mvccpb.KeyValue, key string) (config.Value, error) {
	env, err := decodeEnvelope(kv.Value)
	if err != nil {
		return nil, config.WrapStoreError("decode", "etcd", key, err)
	}
	raw, err := base64.StdEncoding.DecodeString(env.Value)
	if err != nil {
		return nil, config.WrapStoreError("decode", "etcd", key, fmt.Errorf("base64: %w", err))
	}
	return config.NewValueFromBytes(ctx, raw, env.Codec,
		config.WithValueType(env.Type),
		config.WithValueMetadata(env.Version, env.CreatedAt, env.UpdatedAt),
		config.WithValueEntryID(fmt.Sprintf("%d", kv.ModRevision)),
	)
}

// decodeEnvelope unmarshals the JSON envelope stored in etcd.
func decodeEnvelope(data []byte) (*envelope, error) {
	var env envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("unmarshal envelope: %w", err)
	}
	return &env, nil
}

// log returns the configured logger.
func (s *Store) log() *slog.Logger {
	if s.opts.logger != nil {
		return s.opts.logger
	}
	return slog.Default()
}
