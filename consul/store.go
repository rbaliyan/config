// Package consul provides a Consul KV-backed config.Store.
// It uses Consul's blocking queries to implement Watch.
//
// Values are stored as JSON envelopes containing the codec name and
// base64-encoded payload so that the codec is preserved across round-trips:
//
//	{"codec":"json","type":1,"version":3,"created_at":"...","updated_at":"...","value":"<base64>"}
//
// Key mapping: Consul key = "<namespace>/<key>" (or just "<key>" for the
// default empty namespace).
package consul

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

	consulapi "github.com/hashicorp/consul/api"

	"github.com/rbaliyan/config"
)

// Compile-time interface checks.
var (
	_ config.Store     = (*Store)(nil)
	_ config.BulkStore = (*Store)(nil)
)

// envelope is the JSON structure stored in Consul KV.
type envelope struct {
	Codec     string     `json:"codec"`
	Type      config.Type `json:"type"`
	Version   int64      `json:"version"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
	Value     string     `json:"value"` // base64-encoded raw bytes
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
	watchTimeout    time.Duration
	watchBufferSize int
	logger          *slog.Logger
}

// Option configures the Consul store.
type Option func(*options)

// WithWatchTimeout sets the WaitTime used in blocking queries.
// Default is 30 seconds.
func WithWatchTimeout(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.watchTimeout = d
		}
	}
}

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

// Store is a Consul KV-backed config.Store.
type Store struct {
	client    *consulapi.Client
	namespace string
	opts      options
	closed    atomic.Bool

	watchMu       sync.RWMutex
	watchers      map[*watchEntry]struct{}
	stopWatch     chan struct{}
	watchWg       sync.WaitGroup
	droppedEvents atomic.Int64
}

// New creates a new Consul KV store.
// client must be a connected *consulapi.Client.
// namespace is used as the Consul key prefix: "<namespace>/<key>".
// An empty namespace stores keys directly without a prefix component.
func New(client *consulapi.Client, namespace string, opts ...Option) (*Store, error) {
	if client == nil {
		return nil, fmt.Errorf("consul: client must not be nil")
	}
	if err := config.ValidateNamespace(namespace); err != nil {
		return nil, err
	}

	o := options{
		watchTimeout:    30 * time.Second,
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

// consulKey returns the Consul KV key for a given config key.
// Format: "<namespace>/<key>" or "<key>" when namespace is empty.
func (s *Store) consulKey(key string) string {
	if s.namespace == "" {
		return key
	}
	return s.namespace + "/" + key
}

// parseKey extracts the config key from a Consul KV key.
// It strips the namespace prefix (and the trailing slash) if present.
func (s *Store) parseKey(consulKey string) string {
	if s.namespace == "" {
		return consulKey
	}
	prefix := s.namespace + "/"
	return strings.TrimPrefix(consulKey, prefix)
}

// namespacePrefix returns the Consul KV prefix that covers all keys in the namespace.
func (s *Store) namespacePrefix() string {
	if s.namespace == "" {
		return ""
	}
	return s.namespace + "/"
}

// Connect is a no-op for the Consul store; the client handles connectivity.
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

	kv := s.client.KV()
	pair, _, err := kv.Get(s.kvKey(namespace, key), s.queryOpts(ctx, 0))
	if err != nil {
		return nil, config.WrapStoreError("get", "consul", key, err)
	}
	if pair == nil {
		return nil, &config.KeyNotFoundError{Key: key, Namespace: namespace}
	}

	return s.pairToValue(ctx, pair, key)
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
		return nil, config.WrapStoreError("marshal", "consul", key, err)
	}

	cKey := s.kvKey(namespace, key)
	kv := s.client.KV()
	writeMode := config.GetWriteMode(value)
	now := time.Now().UTC()

	switch writeMode {
	case config.WriteModeCreate:
		// Load current to detect existence; use CAS with ModifyIndex=0 to create-only.
		existing, _, err := kv.Get(cKey, s.queryOpts(ctx, 0))
		if err != nil {
			return nil, config.WrapStoreError("get", "consul", key, err)
		}
		if existing != nil {
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
			return nil, config.WrapStoreError("encode", "consul", key, err)
		}
		ok, _, err := kv.CAS(&consulapi.KVPair{Key: cKey, Value: raw, ModifyIndex: 0}, nil)
		if err != nil {
			return nil, config.WrapStoreError("set", "consul", key, err)
		}
		if !ok {
			return nil, &config.KeyExistsError{Key: key, Namespace: namespace}
		}
		return config.NewValueFromBytes(ctx, data, value.Codec(),
			config.WithValueType(value.Type()),
			config.WithValueMetadata(1, now, now),
		)

	case config.WriteModeUpdate:
		existing, _, err := kv.Get(cKey, s.queryOpts(ctx, 0))
		if err != nil {
			return nil, config.WrapStoreError("get", "consul", key, err)
		}
		if existing == nil {
			return nil, &config.KeyNotFoundError{Key: key, Namespace: namespace}
		}
		prev, decErr := decodeEnvelope(existing.Value)
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
			return nil, config.WrapStoreError("encode", "consul", key, err)
		}
		if _, err := kv.Put(&consulapi.KVPair{Key: cKey, Value: raw}, nil); err != nil {
			return nil, config.WrapStoreError("set", "consul", key, err)
		}
		return config.NewValueFromBytes(ctx, data, value.Codec(),
			config.WithValueType(value.Type()),
			config.WithValueMetadata(newVersion, prev.CreatedAt, now),
		)

	default: // Upsert
		existing, _, err := kv.Get(cKey, s.queryOpts(ctx, 0))
		if err != nil {
			return nil, config.WrapStoreError("get", "consul", key, err)
		}
		var createdAt time.Time
		var version int64
		if existing != nil {
			if prev, decErr := decodeEnvelope(existing.Value); decErr == nil {
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
			return nil, config.WrapStoreError("encode", "consul", key, err)
		}
		if _, err := kv.Put(&consulapi.KVPair{Key: cKey, Value: raw}, nil); err != nil {
			return nil, config.WrapStoreError("set", "consul", key, err)
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

	cKey := s.kvKey(namespace, key)
	kv := s.client.KV()

	// Verify existence before deleting so we can return ErrNotFound.
	existing, _, err := kv.Get(cKey, s.queryOpts(ctx, 0))
	if err != nil {
		return config.WrapStoreError("get", "consul", key, err)
	}
	if existing == nil {
		return &config.KeyNotFoundError{Key: key, Namespace: namespace}
	}

	if _, err := kv.Delete(cKey, nil); err != nil {
		return config.WrapStoreError("delete", "consul", key, err)
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

	kv := s.client.KV()

	// Keys mode: fetch specific keys individually.
	if keys := filter.Keys(); len(keys) > 0 {
		results := make(map[string]config.Value, len(keys))
		for _, key := range keys {
			pair, _, err := kv.Get(s.kvKey(namespace, key), s.queryOpts(ctx, 0))
			if err != nil {
				return nil, config.WrapStoreError("find", "consul", key, err)
			}
			if pair == nil {
				continue
			}
			val, err := s.pairToValue(ctx, pair, key)
			if err != nil {
				s.log().Warn("consul: skipping corrupt entry in find", "key", key, "error", err)
				continue
			}
			results[key] = val
		}
		return config.NewPage(results, "", 0), nil
	}

	// Prefix mode: list all keys under the namespace prefix + optional user prefix.
	nsPrefix := s.namespacePrefix()
	userPrefix := filter.Prefix()
	listPrefix := nsPrefix + userPrefix

	pairs, _, err := kv.List(listPrefix, s.queryOpts(ctx, 0))
	if err != nil {
		return nil, config.WrapStoreError("find", "consul", "", err)
	}

	// Sort by Consul key for stable ordering.
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].Key < pairs[j].Key
	})

	// Apply cursor (cursor is the last Consul key returned).
	cursor := filter.Cursor()
	if cursor != "" {
		// cursor is the full consul key; skip entries up to and including it
		i := 0
		for i < len(pairs) && pairs[i].Key <= cursor {
			i++
		}
		pairs = pairs[i:]
	}

	limit := filter.Limit()
	if limit > 0 && len(pairs) > limit {
		pairs = pairs[:limit]
	}

	results := make(map[string]config.Value, len(pairs))
	var lastKey string
	for _, pair := range pairs {
		configKey := s.parseKey(pair.Key)
		// Skip entries that don't belong to the expected namespace/prefix combo.
		if nsPrefix != "" && !strings.HasPrefix(pair.Key, nsPrefix) {
			continue
		}
		val, err := s.pairToValue(ctx, pair, configKey)
		if err != nil {
			s.log().Warn("consul: skipping corrupt entry in find", "key", configKey, "error", err)
			lastKey = pair.Key
			continue
		}
		results[configKey] = val
		lastKey = pair.Key
	}

	return config.NewPage(results, lastKey, limit), nil
}

// Watch returns a channel that receives change events.
// It uses Consul blocking queries to detect changes under the namespace prefix.
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

// watchLoop polls Consul for changes using blocking queries.
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

	var lastIndex uint64
	kv := s.client.KV()
	nsPrefix := s.namespacePrefix()

	// Snapshot used to detect deletes.
	prevKeys := make(map[string]uint64) // consulKey → ModifyIndex

	for {
		select {
		case <-we.ctx.Done():
			return
		case <-s.stopWatch:
			return
		default:
		}

		q := &consulapi.QueryOptions{
			WaitIndex: lastIndex,
			WaitTime:  s.opts.watchTimeout,
			AllowStale: false,
		}
		q = q.WithContext(we.ctx)

		pairs, meta, err := kv.List(nsPrefix, q)
		if err != nil {
			// Context cancelled — normal shutdown.
			select {
			case <-we.ctx.Done():
				return
			case <-s.stopWatch:
				return
			default:
				s.log().Warn("consul: blocking query error", "error", err)
				// Back off briefly before retrying.
				select {
				case <-we.ctx.Done():
					return
				case <-s.stopWatch:
					return
				case <-time.After(time.Second):
				}
				continue
			}
		}

		newIndex := meta.LastIndex
		if newIndex == lastIndex {
			// No change (timeout elapsed).
			continue
		}
		if newIndex < lastIndex {
			// Index reset (e.g. Consul restarted) — rescan.
			lastIndex = 0
			prevKeys = make(map[string]uint64)
			continue
		}

		// Build new key-set snapshot.
		newKeys := make(map[string]uint64, len(pairs))
		for _, pair := range pairs {
			newKeys[pair.Key] = pair.ModifyIndex

			// Changed or new key.
			if pair.ModifyIndex > prevKeys[pair.Key] {
				configKey := s.parseKey(pair.Key)
				val, err := s.pairToValue(we.ctx, pair, configKey)
				if err != nil {
					s.log().Warn("consul: skipping corrupt entry in watch", "key", configKey, "error", err)
					continue
				}
				event := config.ChangeEvent{
					Type:      config.ChangeTypeSet,
					Namespace: s.namespace,
					Key:       configKey,
					Value:     val,
					Timestamp: time.Now().UTC(),
				}
				if config.MatchesWatchFilter(event, we.filter) {
					s.sendToWatcher(we, event)
				}
			}
		}

		// Detect deletions.
		for oldKey := range prevKeys {
			if _, exists := newKeys[oldKey]; !exists {
				configKey := s.parseKey(oldKey)
				event := config.ChangeEvent{
					Type:      config.ChangeTypeDelete,
					Namespace: s.namespace,
					Key:       configKey,
					Timestamp: time.Now().UTC(),
				}
				if config.MatchesWatchFilter(event, we.filter) {
					s.sendToWatcher(we, event)
				}
			}
		}

		prevKeys = newKeys
		lastIndex = newIndex
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
	kv := s.client.KV()
	for _, key := range keys {
		pair, _, err := kv.Get(s.kvKey(namespace, key), s.queryOpts(ctx, 0))
		if err != nil {
			return nil, config.WrapStoreError("get_many", "consul", key, err)
		}
		if pair == nil {
			continue
		}
		val, err := s.pairToValue(ctx, pair, key)
		if err != nil {
			s.log().Warn("consul: skipping corrupt entry in get_many", "key", key, "error", err)
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
		// Use upsert semantics for SetMany.
		set, err := s.Set(ctx, namespace, key, value)
		if err != nil {
			keyErrors[key] = err
			continue
		}
		_ = set
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

// kvKey returns the full Consul KV key for a (namespace, key) pair.
// When namespace differs from s.namespace (e.g. empty vs non-empty), we use
// an override prefix to scope multi-namespace usage on a single Consul path.
func (s *Store) kvKey(namespace, key string) string {
	if namespace == s.namespace {
		return s.consulKey(key)
	}
	// Support callers passing different namespaces than the store namespace.
	// Prefix with the requested namespace so they are isolated.
	if namespace == "" {
		return key
	}
	return namespace + "/" + key
}

// queryOpts returns Consul query options for standard (non-blocking) requests.
func (s *Store) queryOpts(ctx context.Context, waitIndex uint64) *consulapi.QueryOptions {
	q := &consulapi.QueryOptions{
		WaitIndex:  waitIndex,
		AllowStale: false,
	}
	return q.WithContext(ctx)
}

// pairToValue converts a Consul KVPair to a config.Value.
func (s *Store) pairToValue(ctx context.Context, pair *consulapi.KVPair, key string) (config.Value, error) {
	env, err := decodeEnvelope(pair.Value)
	if err != nil {
		return nil, config.WrapStoreError("decode", "consul", key, err)
	}
	raw, err := base64.StdEncoding.DecodeString(env.Value)
	if err != nil {
		return nil, config.WrapStoreError("decode", "consul", key, fmt.Errorf("base64: %w", err))
	}
	return config.NewValueFromBytes(ctx, raw, env.Codec,
		config.WithValueType(env.Type),
		config.WithValueMetadata(env.Version, env.CreatedAt, env.UpdatedAt),
		config.WithValueEntryID(fmt.Sprintf("%d", pair.ModifyIndex)),
	)
}

// decodeEnvelope unmarshals the JSON envelope stored in Consul.
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
