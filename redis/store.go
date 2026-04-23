package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rbaliyan/config"
	goredis "github.com/redis/go-redis/v9"
)

type entryPayload struct {
	Value     []byte    `json:"v"`
	Codec     string    `json:"c"`
	Type      int       `json:"t"`
	Version   int64     `json:"ver"`
	CreatedAt time.Time `json:"ca"`
	UpdatedAt time.Time `json:"ua"`
	EntryID   string    `json:"id"`
}

type changePayload struct {
	Type      string        `json:"type"`
	Namespace string        `json:"namespace"`
	Key       string        `json:"key"`
	Payload   *entryPayload `json:"payload,omitempty"`
}

const luaSetPublish = `
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('PUBLISH', ARGV[3], ARGV[4])
return 1
`

// luaSetNXPublish: atomic "create-if-not-exists then publish".
// Returns 1 on success, 0 if the key already exists.
const luaSetNXPublish = `
if redis.call('HSETNX', KEYS[1], ARGV[1], ARGV[2]) == 1 then
    redis.call('PUBLISH', ARGV[3], ARGV[4])
    return 1
end
return 0
`

var (
	setPublishScript   = goredis.NewScript(luaSetPublish)
	setNXPublishScript = goredis.NewScript(luaSetNXPublish)
)

// Store implements config.Store backed by Redis. See the package-level
// documentation for the key layout and change-notification mechanism.
type Store struct {
	client goredis.UniversalClient
	opts   storeOptions
	closed atomic.Bool

	// idSeq guarantees monotonically increasing, collision-free entry IDs
	// even when Set is called multiple times within the same nanosecond.
	idSeq atomic.Int64

	watchMu  sync.RWMutex
	watchers map[*watchEntry]struct{}
	stopChan chan struct{}
	stopWg   sync.WaitGroup
}

var (
	_ config.Store         = (*Store)(nil)
	_ config.HealthChecker = (*Store)(nil)
	_ config.StatsProvider = (*Store)(nil)
)

// NewStore creates a Redis-backed config store. Call Connect before using
// the store; Connect dials Redis and starts the pub/sub subscription that
// drives Watch.
func NewStore(opts ...Option) *Store {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	return &Store{
		opts:     o,
		watchers: make(map[*watchEntry]struct{}),
		stopChan: make(chan struct{}),
	}
}

func (s *Store) hashKey(namespace string) string {
	return fmt.Sprintf("%s:%s", s.opts.keyPrefix, namespace)
}

func (s *Store) chanKey() string {
	return fmt.Sprintf("%s:changes", s.opts.keyPrefix)
}

func (s *Store) Connect(ctx context.Context) error {
	if s.closed.Load() {
		return config.ErrStoreClosed
	}
	var client goredis.UniversalClient
	if len(s.opts.clusterAddrs) > 0 {
		client = goredis.NewClusterClient(&goredis.ClusterOptions{
			Addrs:     s.opts.clusterAddrs,
			Password:  s.opts.password,
			TLSConfig: s.opts.tlsConfig,
		})
	} else {
		client = goredis.NewClient(&goredis.Options{
			Addr:         s.opts.addr,
			Password:     s.opts.password,
			DB:           s.opts.db,
			TLSConfig:    s.opts.tlsConfig,
			DialTimeout:  s.opts.dialTimeout,
			ReadTimeout:  s.opts.readTimeout,
			WriteTimeout: s.opts.writeTimeout,
		})
	}
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return config.WrapStoreError("connect", "redis", s.opts.addr, err)
	}
	s.client = client
	s.stopWg.Add(1)
	go s.runPubSub()
	return nil
}

func (s *Store) Close(ctx context.Context) error {
	if s.closed.Swap(true) {
		return nil
	}
	close(s.stopChan)
	s.stopWg.Wait()

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

	if s.client != nil {
		return s.client.Close()
	}
	return nil
}

func (s *Store) Get(ctx context.Context, namespace, key string) (config.Value, error) {
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
	}
	if s.client == nil {
		return nil, config.ErrStoreNotConnected
	}
	if err := config.ValidateNamespace(namespace); err != nil {
		return nil, err
	}
	if err := config.ValidateKey(key); err != nil {
		return nil, err
	}

	data, err := s.client.HGet(ctx, s.hashKey(namespace), key).Bytes()
	if err == goredis.Nil {
		return nil, &config.KeyNotFoundError{Key: key, Namespace: namespace}
	}
	if err != nil {
		return nil, config.WrapStoreError("get", "redis", key, err)
	}

	var ep entryPayload
	if err := json.Unmarshal(data, &ep); err != nil {
		return nil, config.WrapStoreError("get", "redis", key, err)
	}

	return config.NewValueFromBytes(ctx, ep.Value, ep.Codec,
		config.WithValueType(config.Type(ep.Type)),
		config.WithValueMetadata(ep.Version, ep.CreatedAt, ep.UpdatedAt),
		config.WithValueEntryID(ep.EntryID),
	)
}

func (s *Store) Set(ctx context.Context, namespace, key string, value config.Value) (config.Value, error) {
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
	}
	if s.client == nil {
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
		return nil, config.WrapStoreError("marshal", "redis", key, err)
	}

	hashKey := s.hashKey(namespace)
	channel := s.chanKey()
	now := time.Now().UTC()
	writeMode := config.GetWriteMode(value)

	switch writeMode {
	case config.WriteModeCreate:
		// Build the payload with its final EntryID so the atomic HSETNX
		// observes the complete entry on its very first write.
		entryID := s.makeEntryID(namespace, key)
		ep := s.buildEntryPayload(data, value, 1, now, now, entryID)
		epJSON, err := json.Marshal(ep)
		if err != nil {
			return nil, config.WrapStoreError("set", "redis", key, err)
		}

		cp := changePayload{Type: "set", Namespace: namespace, Key: key, Payload: ep}
		cpJSON, _ := json.Marshal(cp)

		res, err := setNXPublishScript.Run(ctx, s.client, []string{hashKey},
			key, string(epJSON), channel, string(cpJSON)).Int64()
		if err != nil {
			return nil, config.WrapStoreError("set", "redis", key, err)
		}
		if res != 1 {
			return nil, &config.KeyExistsError{Key: key, Namespace: namespace}
		}

	case config.WriteModeUpdate:
		existing, err := s.client.HGet(ctx, hashKey, key).Bytes()
		if err == goredis.Nil {
			return nil, &config.KeyNotFoundError{Key: key, Namespace: namespace}
		}
		if err != nil {
			return nil, config.WrapStoreError("set", "redis", key, err)
		}

		var prev entryPayload
		if err := json.Unmarshal(existing, &prev); err != nil {
			return nil, config.WrapStoreError("set", "redis", key, err)
		}

		ep := s.buildEntryPayload(data, value, prev.Version+1, prev.CreatedAt, now, prev.EntryID)
		epJSON, err := json.Marshal(ep)
		if err != nil {
			return nil, config.WrapStoreError("set", "redis", key, err)
		}

		cp := changePayload{Type: "set", Namespace: namespace, Key: key, Payload: ep}
		cpJSON, _ := json.Marshal(cp)

		if err := setPublishScript.Run(ctx, s.client, []string{hashKey}, key, string(epJSON), channel, string(cpJSON)).Err(); err != nil {
			return nil, config.WrapStoreError("set", "redis", key, err)
		}

	default:
		existing, err := s.client.HGet(ctx, hashKey, key).Bytes()
		var version int64 = 1
		var createdAt = now
		var entryID string
		if err == nil {
			var prev entryPayload
			if jsonErr := json.Unmarshal(existing, &prev); jsonErr == nil {
				version = prev.Version + 1
				createdAt = prev.CreatedAt
				entryID = prev.EntryID
			}
		}
		if entryID == "" {
			entryID = s.makeEntryID(namespace, key)
		}

		ep := s.buildEntryPayload(data, value, version, createdAt, now, entryID)
		epJSON, err := json.Marshal(ep)
		if err != nil {
			return nil, config.WrapStoreError("set", "redis", key, err)
		}

		cp := changePayload{Type: "set", Namespace: namespace, Key: key, Payload: ep}
		cpJSON, _ := json.Marshal(cp)

		if err := setPublishScript.Run(ctx, s.client, []string{hashKey}, key, string(epJSON), channel, string(cpJSON)).Err(); err != nil {
			return nil, config.WrapStoreError("set", "redis", key, err)
		}
	}

	return s.Get(ctx, namespace, key)
}

func (s *Store) buildEntryPayload(data []byte, value config.Value, version int64, createdAt, updatedAt time.Time, entryID string) *entryPayload {
	return &entryPayload{
		Value:     data,
		Codec:     value.Codec(),
		Type:      int(value.Type()),
		Version:   version,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
		EntryID:   entryID,
	}
}

// makeEntryID returns a monotonically increasing int64 ID. Each call
// atomically increments a per-Store counter and adds it to the current
// nanosecond clock. Because the counter only climbs, two calls made inside
// the same nanosecond still produce distinct, ordered IDs. Find parses
// EntryID with ParseInt for pagination, so the output must remain numeric.
func (s *Store) makeEntryID(_, _ string) string {
	return strconv.FormatInt(time.Now().UnixNano()+s.idSeq.Add(1), 10)
}

func (s *Store) Delete(ctx context.Context, namespace, key string) error {
	if s.closed.Load() {
		return config.ErrStoreClosed
	}
	if s.client == nil {
		return config.ErrStoreNotConnected
	}
	if err := config.ValidateNamespace(namespace); err != nil {
		return err
	}
	if err := config.ValidateKey(key); err != nil {
		return err
	}

	n, err := s.client.HDel(ctx, s.hashKey(namespace), key).Result()
	if err != nil {
		return config.WrapStoreError("delete", "redis", key, err)
	}
	if n == 0 {
		return &config.KeyNotFoundError{Key: key, Namespace: namespace}
	}

	cp := changePayload{Type: "delete", Namespace: namespace, Key: key}
	cpJSON, _ := json.Marshal(cp)
	_ = s.client.Publish(ctx, s.chanKey(), cpJSON)

	return nil
}

func (s *Store) Find(ctx context.Context, namespace string, filter config.Filter) (config.Page, error) {
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
	}
	if s.client == nil {
		return nil, config.ErrStoreNotConnected
	}
	if filter == nil {
		filter = config.NewFilter().Build()
	}
	if err := config.ValidateNamespace(namespace); err != nil {
		return nil, err
	}

	hashKey := s.hashKey(namespace)
	limit := filter.Limit()

	if keys := filter.Keys(); len(keys) > 0 {
		results := make(map[string]config.Value, len(keys))
		for _, k := range keys {
			raw, err := s.client.HGet(ctx, hashKey, k).Bytes()
			if err == goredis.Nil {
				continue
			}
			if err != nil {
				return nil, config.WrapStoreError("find", "redis", k, err)
			}
			var ep entryPayload
			if err := json.Unmarshal(raw, &ep); err != nil {
				continue
			}
			v, err := config.NewValueFromBytes(ctx, ep.Value, ep.Codec,
				config.WithValueType(config.Type(ep.Type)),
				config.WithValueMetadata(ep.Version, ep.CreatedAt, ep.UpdatedAt),
				config.WithValueEntryID(ep.EntryID),
			)
			if err != nil {
				continue
			}
			results[k] = v
		}
		return config.NewPage(results, "", 0), nil
	}

	all, err := s.client.HGetAll(ctx, hashKey).Result()
	if err != nil {
		return nil, config.WrapStoreError("find", "redis", "", err)
	}

	type entry struct {
		key     string
		ep      entryPayload
		entryID int64
	}

	prefix := filter.Prefix()
	cursor := filter.Cursor()

	var entries []entry
	for k, raw := range all {
		if prefix != "" && !strings.HasPrefix(k, prefix) {
			continue
		}
		var ep entryPayload
		if err := json.Unmarshal([]byte(raw), &ep); err != nil {
			continue
		}
		id, _ := strconv.ParseInt(ep.EntryID, 10, 64)
		entries = append(entries, entry{key: k, ep: ep, entryID: id})
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].entryID < entries[j].entryID
	})

	var cursorID int64
	if cursor != "" {
		cursorID, _ = strconv.ParseInt(cursor, 10, 64)
	}

	results := make(map[string]config.Value)
	var lastID int64
	count := 0
	for _, e := range entries {
		if cursorID > 0 && e.entryID <= cursorID {
			continue
		}
		if limit > 0 && count >= limit {
			break
		}
		v, err := config.NewValueFromBytes(ctx, e.ep.Value, e.ep.Codec,
			config.WithValueType(config.Type(e.ep.Type)),
			config.WithValueMetadata(e.ep.Version, e.ep.CreatedAt, e.ep.UpdatedAt),
			config.WithValueEntryID(e.ep.EntryID),
		)
		if err != nil {
			continue
		}
		results[e.key] = v
		lastID = e.entryID
		count++
	}

	nextCursor := ""
	if lastID > 0 {
		nextCursor = fmt.Sprintf("%d", lastID)
	}

	return config.NewPage(results, nextCursor, limit), nil
}

func (s *Store) Watch(ctx context.Context, filter config.WatchFilter) (<-chan config.ChangeEvent, error) {
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
	}
	if s.client == nil {
		return nil, config.ErrStoreNotConnected
	}

	watchCtx, cancel := context.WithCancel(ctx)
	we := &watchEntry{
		filter: filter,
		ch:     make(chan config.ChangeEvent, s.opts.watchBufSize),
		ctx:    watchCtx,
		cancel: cancel,
	}

	s.watchMu.Lock()
	s.watchers[we] = struct{}{}
	s.watchMu.Unlock()

	go func() {
		select {
		case <-watchCtx.Done():
		case <-s.stopChan:
		}
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

	return we.ch, nil
}

func (s *Store) Health(ctx context.Context) error {
	if s.closed.Load() {
		return config.ErrStoreClosed
	}
	if s.client == nil {
		return config.ErrStoreNotConnected
	}
	return s.client.Ping(ctx).Err()
}

func (s *Store) Stats(ctx context.Context) (*config.StoreStats, error) {
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
	}
	if s.client == nil {
		return nil, config.ErrStoreNotConnected
	}

	stats := &config.StoreStats{
		EntriesByType:      make(map[config.Type]int64),
		EntriesByNamespace: make(map[string]int64),
	}

	// Restrict SCAN to the hash pattern so the pubsub channel key
	// ("{prefix}:changes") is not returned, and TYPE-check each key
	// to skip anything that isn't a hash (defensive).
	pattern := fmt.Sprintf("%s:*", s.opts.keyPrefix)
	chanKey := s.chanKey()
	var cursor uint64
	for {
		keys, nextCursor, err := s.client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return nil, config.WrapStoreError("stats", "redis", "", err)
		}

		for _, k := range keys {
			if k == chanKey {
				continue
			}
			t, err := s.client.Type(ctx, k).Result()
			if err != nil || t != "hash" {
				continue
			}
			ns := strings.TrimPrefix(k, s.opts.keyPrefix+":")
			count, err := s.client.HLen(ctx, k).Result()
			if err != nil {
				continue
			}
			stats.TotalEntries += count
			stats.EntriesByNamespace[ns] += count
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	return stats, nil
}

// runPubSub subscribes to the change channel and relays messages to
// watchers. It reconnects on transient channel closure so Watch survives
// momentary Redis disconnects. Termination is driven exclusively by s.stopChan.
func (s *Store) runPubSub() {
	defer s.stopWg.Done()

	const (
		initialBackoff = 100 * time.Millisecond
		maxBackoff     = 10 * time.Second
	)
	backoff := initialBackoff

	for {
		select {
		case <-s.stopChan:
			return
		default:
		}

		pubsub := s.client.Subscribe(context.Background(), s.chanKey())
		ch := pubsub.Channel()

	loop:
		for {
			select {
			case <-s.stopChan:
				_ = pubsub.Close()
				return
			case msg, ok := <-ch:
				if !ok {
					// Channel closed (remote disconnect) — break out and reconnect.
					break loop
				}
				backoff = initialBackoff
				var cp changePayload
				if err := json.Unmarshal([]byte(msg.Payload), &cp); err != nil {
					continue
				}
				event := changePayloadToEvent(cp)
				s.notifyWatchers(event)
			}
		}

		_ = pubsub.Close()

		// Reconnect with bounded exponential backoff.
		select {
		case <-s.stopChan:
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

func changePayloadToEvent(cp changePayload) config.ChangeEvent {
	event := config.ChangeEvent{
		Namespace: cp.Namespace,
		Key:       cp.Key,
		Timestamp: time.Now().UTC(),
	}

	switch cp.Type {
	case "delete":
		event.Type = config.ChangeTypeDelete
	default:
		event.Type = config.ChangeTypeSet
		if cp.Payload != nil {
			v, err := config.NewValueFromBytes(context.Background(), cp.Payload.Value, cp.Payload.Codec,
				config.WithValueType(config.Type(cp.Payload.Type)),
				config.WithValueMetadata(cp.Payload.Version, cp.Payload.CreatedAt, cp.Payload.UpdatedAt),
				config.WithValueEntryID(cp.Payload.EntryID),
			)
			if err == nil {
				event.Value = v
			}
		}
	}

	return event
}
