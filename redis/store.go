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
	Value     []byte     `json:"v"`
	Codec     string     `json:"c"`
	Type      int        `json:"t"`
	Version   int64      `json:"ver"`
	CreatedAt time.Time  `json:"ca"`
	UpdatedAt time.Time  `json:"ua"`
	EntryID   string     `json:"id"`
	ExpiresAt *time.Time `json:"exp,omitempty"` // nil = no TTL
	// ExpiresAtUnixNano mirrors ExpiresAt as integer unix-nanos so the Lua
	// scripts can compare against the current time without parsing RFC3339.
	// Zero means no TTL (matches the "exp omitempty" semantic).
	ExpiresAtUnixNano int64 `json:"expN,omitempty"`
}

type changePayload struct {
	Type      string        `json:"type"`
	Namespace string        `json:"namespace"`
	Key       string        `json:"key"`
	Payload   *entryPayload `json:"payload,omitempty"`
}

// luaUpsertPublish atomically performs an upsert: it reads the existing
// field, computes the next version and preserves createdAt/entryID when the
// field already exists and is not yet expired, writes the merged payload, and
// publishes a change notification — all in a single Redis round-trip so
// concurrent writers cannot produce duplicate versions.
//
// ARGV layout:
//
//	[1] hash field (config key)
//	[2] candidate payload JSON (initial version=1, fresh entryID, createdAt=updatedAt=now)
//	[3] pub/sub channel
//	[4] change payload prefix ("{\"type\":\"set\",...,\"payload\":")
//	[5] change payload suffix ("}")
//	[6] candidate exp as unix-nanos (0 if no TTL); used to overwrite cand.exp
//	    after JSON decode so Lua compares numbers rather than ISO strings.
//	[7] prior-exp threshold as unix-nanos: the current time at which a prior
//	    entry whose stored exp is ≤ this value should be treated as absent.
//
// An expired prior entry is treated as absent: the candidate is written as a
// fresh create (version=1, fresh entryID, createdAt=now). Expiry comparisons
// use unix-nanos integers (passed as ARGV) instead of RFC3339 string compares,
// which avoids subtle bugs with variable-length nanosecond suffixes.
const luaUpsertPublish = `
local key = ARGV[1]
local cand = cjson.decode(ARGV[2])
local candExp = tonumber(ARGV[6])
local now = tonumber(ARGV[7])
local prior = redis.call('HGET', KEYS[1], key)
if prior then
    local p = cjson.decode(prior)
    local pExp = tonumber(p.expN)
    local expired = (pExp ~= nil and pExp > 0 and pExp <= now)
    if not expired then
        cand.ver = (tonumber(p.ver) or 0) + 1
        cand.ca = p.ca
        cand.id = p.id
        if (candExp == nil or candExp == 0) and pExp ~= nil and pExp > 0 then
            cand.exp = p.exp
            cand.expN = p.expN
        end
    end
end
local payload = cjson.encode(cand)
redis.call('HSET', KEYS[1], key, payload)
redis.call('PUBLISH', ARGV[3], ARGV[4] .. payload .. ARGV[5])
return payload
`

// luaUpdatePublish atomically updates only when the field exists and is not
// expired; returns nil otherwise. Shares the merge-then-publish semantics of
// luaUpsertPublish.
const luaUpdatePublish = `
local key = ARGV[1]
local cand = cjson.decode(ARGV[2])
local candExp = tonumber(ARGV[6])
local now = tonumber(ARGV[7])
local prior = redis.call('HGET', KEYS[1], key)
if not prior then return nil end
local p = cjson.decode(prior)
local pExp = tonumber(p.expN)
if pExp ~= nil and pExp > 0 and pExp <= now then
    return nil
end
cand.ver = (tonumber(p.ver) or 0) + 1
cand.ca = p.ca
cand.id = p.id
if (candExp == nil or candExp == 0) and pExp ~= nil and pExp > 0 then
    cand.exp = p.exp
    cand.expN = p.expN
end
local payload = cjson.encode(cand)
redis.call('HSET', KEYS[1], key, payload)
redis.call('PUBLISH', ARGV[3], ARGV[4] .. payload .. ARGV[5])
return payload
`

// luaSetNXPublish: atomic "create-if-not-exists then publish". An expired
// prior field is treated as absent — the candidate overwrites it. Returns
// the payload on success, nil if a non-expired field already exists.
const luaSetNXPublish = `
local key = ARGV[1]
local now = tonumber(ARGV[7])
local prior = redis.call('HGET', KEYS[1], key)
if prior then
    local p = cjson.decode(prior)
    local pExp = tonumber(p.expN)
    local expired = (pExp ~= nil and pExp > 0 and pExp <= now)
    if not expired then
        return nil
    end
end
redis.call('HSET', KEYS[1], key, ARGV[2])
redis.call('PUBLISH', ARGV[3], ARGV[4] .. ARGV[2] .. ARGV[5])
return ARGV[2]
`

var (
	upsertPublishScript = goredis.NewScript(luaUpsertPublish)
	updatePublishScript = goredis.NewScript(luaUpdatePublish)
	setNXPublishScript  = goredis.NewScript(luaSetNXPublish)
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

// BackendName returns the stable backend identifier used in error messages.
func (s *Store) BackendName() string { return "redis" }

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
	s.idSeq.Store(time.Now().UnixNano())
	s.stopWg.Add(1)
	go s.runPubSub() // #nosec G118 -- intentional long-lived goroutine; manages lifecycle via stop channel, not caller ctx
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

	if ep.ExpiresAt != nil && time.Now().UTC().After(*ep.ExpiresAt) {
		return nil, &config.KeyNotFoundError{Key: key, Namespace: namespace}
	}

	var expiresAt time.Time
	if ep.ExpiresAt != nil {
		expiresAt = *ep.ExpiresAt
	}
	return config.NewValueFromBytes(ctx, ep.Value, ep.Codec,
		config.WithValueType(config.Type(ep.Type)),
		config.WithValueMetadata(ep.Version, ep.CreatedAt, ep.UpdatedAt),
		config.WithValueEntryID(ep.EntryID),
		config.WithValueExpiresAt(expiresAt),
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

	// Build the candidate payload used for a fresh create. The upsert/update
	// scripts overwrite ver/ca/id in-place on the prior entry when one exists,
	// so version/createdAt/entryID computed here are only used on the "no
	// prior entry" branch.
	cand := s.buildEntryPayload(data, value, 1, now, now, s.makeEntryID())
	candJSON, err := json.Marshal(cand)
	if err != nil {
		return nil, config.WrapStoreError("set", "redis", key, err)
	}

	// Change payload is split into prefix/suffix around the stored payload so
	// the Lua scripts can emit the exact bytes they persisted.
	cpPrefix, cpSuffix, err := changePayloadFrames(namespace, key)
	if err != nil {
		return nil, config.WrapStoreError("set", "redis", key, err)
	}

	var script *goredis.Script
	switch writeMode {
	case config.WriteModeCreate:
		script = setNXPublishScript
	case config.WriteModeUpdate:
		script = updatePublishScript
	default:
		script = upsertPublishScript
	}

	// Pass times to Lua as integer unix-nanos so comparisons are exact and
	// independent of RFC3339 string formatting quirks.
	candExpNano := int64(0)
	if cand.ExpiresAt != nil {
		candExpNano = cand.ExpiresAt.UnixNano()
	}
	nowNano := now.UnixNano()
	res, err := script.Run(ctx, s.client, []string{hashKey},
		key, string(candJSON), channel, cpPrefix, cpSuffix,
		strconv.FormatInt(candExpNano, 10),
		strconv.FormatInt(nowNano, 10),
	).Result()
	if err != nil && err != goredis.Nil {
		return nil, config.WrapStoreError("set", "redis", key, err)
	}

	switch writeMode {
	case config.WriteModeCreate:
		if err == goredis.Nil || res == nil {
			return nil, &config.KeyExistsError{Key: key, Namespace: namespace}
		}
	case config.WriteModeUpdate:
		if err == goredis.Nil || res == nil {
			return nil, &config.KeyNotFoundError{Key: key, Namespace: namespace}
		}
	}

	payload, ok := res.(string)
	if !ok {
		return nil, config.WrapStoreError("set", "redis", key,
			fmt.Errorf("unexpected script result type %T", res))
	}

	var ep entryPayload
	if err := json.Unmarshal([]byte(payload), &ep); err != nil {
		return nil, config.WrapStoreError("set", "redis", key, err)
	}
	var expiresAt time.Time
	if ep.ExpiresAt != nil {
		expiresAt = *ep.ExpiresAt
	}
	return config.NewValueFromBytes(ctx, ep.Value, ep.Codec,
		config.WithValueType(config.Type(ep.Type)),
		config.WithValueMetadata(ep.Version, ep.CreatedAt, ep.UpdatedAt),
		config.WithValueEntryID(ep.EntryID),
		config.WithValueExpiresAt(expiresAt),
	)
}

// changePayloadFrames returns the ("prefix", "suffix") of a change payload so
// that the Lua scripts can splice the persisted entry JSON between them and
// publish an identical representation of what was written.
func changePayloadFrames(namespace, key string) (string, string, error) {
	prefix, err := json.Marshal(struct {
		Type      string `json:"type"`
		Namespace string `json:"namespace"`
		Key       string `json:"key"`
	}{Type: "set", Namespace: namespace, Key: key})
	if err != nil {
		return "", "", err
	}
	// Rewrite the trailing "}" to ",\"payload\":" and keep "}" as the suffix.
	head := string(prefix[:len(prefix)-1]) + `,"payload":`
	return head, `}`, nil
}

func (s *Store) buildEntryPayload(data []byte, value config.Value, version int64, createdAt, updatedAt time.Time, entryID string) *entryPayload {
	ep := &entryPayload{
		Value:     data,
		Codec:     value.Codec(),
		Type:      int(value.Type()),
		Version:   version,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
		EntryID:   entryID,
	}
	expiry := config.GetExpiresAt(value)
	if !expiry.IsZero() {
		ep.ExpiresAt = &expiry
		ep.ExpiresAtUnixNano = expiry.UnixNano()
	}
	return ep
}

// makeEntryID returns a monotonically increasing, globally unique int64 ID.
// idSeq is seeded from UnixNano at Connect time, so IDs from a new session
// are always greater than any ID written in a prior session.
func (s *Store) makeEntryID() string {
	return strconv.FormatInt(s.idSeq.Add(1), 10)
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

	now := time.Now().UTC()

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
			if ep.ExpiresAt != nil && now.After(*ep.ExpiresAt) {
				continue
			}
			var expiresAt time.Time
			if ep.ExpiresAt != nil {
				expiresAt = *ep.ExpiresAt
			}
			v, err := config.NewValueFromBytes(ctx, ep.Value, ep.Codec,
				config.WithValueType(config.Type(ep.Type)),
				config.WithValueMetadata(ep.Version, ep.CreatedAt, ep.UpdatedAt),
				config.WithValueEntryID(ep.EntryID),
				config.WithValueExpiresAt(expiresAt),
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
		if ep.ExpiresAt != nil && now.After(*ep.ExpiresAt) {
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
		var expiresAt time.Time
		if e.ep.ExpiresAt != nil {
			expiresAt = *e.ep.ExpiresAt
		}
		v, err := config.NewValueFromBytes(ctx, e.ep.Value, e.ep.Codec,
			config.WithValueType(config.Type(e.ep.Type)),
			config.WithValueMetadata(e.ep.Version, e.ep.CreatedAt, e.ep.UpdatedAt),
			config.WithValueEntryID(e.ep.EntryID),
			config.WithValueExpiresAt(expiresAt),
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

// Stats returns store statistics. Uses Redis SCAN to enumerate hash keys,
// TYPE-checks each one to skip non-hash entries (e.g. the pubsub channel
// key), then HLEN per hash. Cost is O(keys) — for very large deployments
// prefer a precomputed counter; the implementation is intentionally
// straightforward and avoids maintaining a parallel index.
func (s *Store) Stats(ctx context.Context) (config.StoreStats, error) {
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
	}
	if s.client == nil {
		return nil, config.ErrStoreNotConnected
	}

	byType := make(map[config.Type]int64)
	byNamespace := make(map[string]int64)
	var total int64

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
			total += count
			byNamespace[ns] += count
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	return config.NewStoreStats(total, byType, byNamespace), nil
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
			var expiresAt time.Time
			if cp.Payload.ExpiresAt != nil {
				expiresAt = *cp.Payload.ExpiresAt
			}
			v, err := config.NewValueFromBytes(context.Background(), cp.Payload.Value, cp.Payload.Codec,
				config.WithValueType(config.Type(cp.Payload.Type)),
				config.WithValueMetadata(cp.Payload.Version, cp.Payload.CreatedAt, cp.Payload.UpdatedAt),
				config.WithValueEntryID(cp.Payload.EntryID),
				config.WithValueExpiresAt(expiresAt),
			)
			if err == nil {
				event.Value = v
			}
		}
	}

	return event
}
