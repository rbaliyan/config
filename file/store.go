package file

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
	"github.com/rbaliyan/config/codec"
)

// Store implements config.Store backed by a configuration file.
//
// By default the Store is read-only: Set and Delete return
// config.ErrReadOnly, and Watch returns config.ErrWatchNotSupported. Pass
// WithWritable to NewStore to enable writes (persisted to a sidecar file)
// and Watch (polling-based).
//
// Top-level keys in the file become namespaces.
// Nested keys are flattened with "/" separator (configurable via
// WithKeySeparator).
//
// Given this YAML:
//
//	db:
//	  host: localhost
//	  pool:
//	    max_size: 50
//
// The store exposes:
//
//	Namespace "db", Key "host"          → "localhost"
//	Namespace "db", Key "pool/max_size" → 50
type Store struct {
	path   string
	opts   storeOptions
	loader *Loader

	mu      sync.RWMutex
	entries map[string]map[string]*storeEntry // namespace -> key -> entry
	nextID  int64
	closed  atomic.Bool

	sc *sidecar // non-nil when opts.writable

	watchMu  sync.RWMutex
	watchers map[*watchEntry]struct{}
	pollStop chan struct{}
	pollWg   sync.WaitGroup
}

type storeEntry struct {
	id        string
	key       string
	namespace string
	value     []byte
	codec     string
	valueType config.Type
	version   int64
	createdAt time.Time
	updatedAt time.Time
}

func (e *storeEntry) toValue(ctx context.Context) (config.Value, error) {
	return config.NewValueFromBytes(
		ctx,
		e.value,
		e.codec,
		config.WithValueType(e.valueType),
		config.WithValueMetadata(e.version, e.createdAt, e.updatedAt),
		config.WithValueEntryID(e.id),
	)
}

type watchEntry struct {
	filter    config.WatchFilter
	ch        chan config.ChangeEvent
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.Mutex
	closed    bool
	closeOnce sync.Once
}

// Compile-time interface checks.
var (
	_ config.Store         = (*Store)(nil)
	_ config.HealthChecker = (*Store)(nil)
	_ config.StatsProvider = (*Store)(nil)
)

// NewStore creates a new file-backed Store.
// The file is read when Connect is called.
func NewStore(path string, opts ...StoreOption) *Store {
	o := storeOptions{
		keySeparator:     "/",
		defaultNamespace: "default",
		sidecarSuffix:    ".writes.yaml",
		watchInterval:    2 * time.Second,
		watchBufSize:     100,
	}
	for _, opt := range opts {
		opt(&o)
	}

	return &Store{
		path:     path,
		opts:     o,
		loader:   New(path, o.loaderOpts...),
		entries:  make(map[string]map[string]*storeEntry),
		watchers: make(map[*watchEntry]struct{}),
		pollStop: make(chan struct{}),
	}
}

// BackendName returns the stable backend identifier used in error messages.
func (s *Store) BackendName() string { return "file" }

// Connect reads and parses the config file, populating the store.
func (s *Store) Connect(ctx context.Context) error {
	if s.closed.Load() {
		return config.ErrStoreClosed
	}

	if err := s.loader.Load(); err != nil {
		return config.WrapStoreError("connect", "file", s.path, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	raw := s.loader.AllSettings()
	s.entries = make(map[string]map[string]*storeEntry)
	s.nextID = 0

	for topKey, topVal := range raw {
		m, ok := topVal.(map[string]any)
		if !ok {
			ns := s.opts.defaultNamespace
			if err := s.addEntryValidated(ctx, ns, topKey, topVal, now); err != nil {
				s.logWarn("skipping entry", "namespace", ns, "key", topKey, "error", err)
			}
			continue
		}
		if err := config.ValidateNamespace(topKey); err != nil {
			s.logWarn("skipping namespace: invalid name", "namespace", topKey, "error", err)
			continue
		}
		s.flattenMap(ctx, topKey, "", m, now)
	}

	if s.opts.writable {
		sidecarPath := s.path + s.opts.sidecarSuffix
		s.sc = newSidecar(sidecarPath)
		if err := s.sc.load(); err != nil {
			return config.WrapStoreError("connect", "file", sidecarPath, err)
		}
		s.applySidecar(ctx, now)
		// Add to the WaitGroup BEFORE starting the goroutine so a concurrent
		// Close() that reaches Wait before the goroutine begins still blocks
		// correctly until the goroutine exits.
		s.pollWg.Add(1)
		go s.startPolling()
	}

	return nil
}

// applySidecar merges sidecar data over base entries. Must be called with s.mu held.
func (s *Store) applySidecar(ctx context.Context, now time.Time) {
	all := s.sc.allSettings()
	for ns, keys := range all {
		for key, val := range keys {
			if val == nil {
				// tombstone: remove from view
				if s.entries[ns] != nil {
					delete(s.entries[ns], key)
				}
				continue
			}
			existing := s.entriesGet(ns, key)
			version := int64(1)
			createdAt := now
			var entryID string
			if existing != nil {
				version = existing.version + 1
				createdAt = existing.createdAt
				entryID = existing.id
			} else {
				s.nextID++
				entryID = fmt.Sprintf("%d", s.nextID)
			}
			c := codec.Default()
			data, err := c.Encode(ctx, val)
			if err != nil {
				s.logWarn("sidecar encode error", "namespace", ns, "key", key, "error", err)
				continue
			}
			e := &storeEntry{
				id:        entryID,
				key:       key,
				namespace: ns,
				value:     data,
				codec:     c.Name(),
				valueType: config.NewValue(val).Type(),
				version:   version,
				createdAt: createdAt,
				updatedAt: now,
			}
			if s.entries[ns] == nil {
				s.entries[ns] = make(map[string]*storeEntry)
			}
			s.entries[ns][key] = e
		}
	}
}

func (s *Store) entriesGet(ns, key string) *storeEntry {
	m, ok := s.entries[ns]
	if !ok {
		return nil
	}
	return m[key]
}

// Close releases resources.
func (s *Store) Close(ctx context.Context) error {
	if s.closed.Swap(true) {
		return nil
	}

	if s.opts.writable {
		close(s.pollStop)
		s.pollWg.Wait()
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

	s.mu.Lock()
	s.entries = make(map[string]map[string]*storeEntry)
	s.mu.Unlock()
	return nil
}

// Get retrieves a value by namespace and key.
func (s *Store) Get(ctx context.Context, namespace, key string) (config.Value, error) {
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
	}
	if err := config.ValidateNamespace(namespace); err != nil {
		return nil, err
	}
	if err := config.ValidateKey(key); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	nsEntries, ok := s.entries[namespace]
	if !ok {
		return nil, &config.KeyNotFoundError{Key: key, Namespace: namespace}
	}
	e, ok := nsEntries[key]
	if !ok {
		return nil, &config.KeyNotFoundError{Key: key, Namespace: namespace}
	}

	return e.toValue(ctx)
}

// Set creates or updates a value. Only supported when WithWritable() is used.
func (s *Store) Set(ctx context.Context, namespace, key string, value config.Value) (config.Value, error) {
	if !s.opts.writable {
		return nil, config.ErrReadOnly
	}
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
	}
	if err := config.ValidateNamespace(namespace); err != nil {
		return nil, err
	}
	if err := config.ValidateKey(key); err != nil {
		return nil, err
	}

	// Encode value with default codec to get raw bytes for the storeEntry.
	c := codec.Default()
	data, err := c.Encode(ctx, value)
	if err != nil {
		return nil, config.WrapStoreError("set", "file", key, err)
	}

	// Decode back to raw any for sidecar (YAML storage needs native values).
	var raw any
	if err := c.Decode(ctx, data, &raw); err != nil {
		return nil, config.WrapStoreError("set", "file", key, err)
	}

	if err := s.sc.set(namespace, key, raw); err != nil {
		return nil, config.WrapStoreError("set", "file", key, err)
	}

	now := time.Now().UTC()

	s.mu.Lock()
	existing := s.entriesGet(namespace, key)
	version := int64(1)
	createdAt := now
	var entryID string
	if existing != nil {
		version = existing.version + 1
		createdAt = existing.createdAt
		entryID = existing.id
	} else {
		s.nextID++
		entryID = fmt.Sprintf("%d", s.nextID)
	}
	e := &storeEntry{
		id:        entryID,
		key:       key,
		namespace: namespace,
		value:     data,
		codec:     c.Name(),
		valueType: value.Type(),
		version:   version,
		createdAt: createdAt,
		updatedAt: now,
	}
	if s.entries[namespace] == nil {
		s.entries[namespace] = make(map[string]*storeEntry)
	}
	s.entries[namespace][key] = e
	s.mu.Unlock()

	newValue, err := e.toValue(ctx)
	if err != nil {
		return nil, config.WrapStoreError("set", "file", key, err)
	}

	event := config.ChangeEvent{
		Type:      config.ChangeTypeSet,
		Namespace: namespace,
		Key:       key,
		Value:     newValue,
		Timestamp: now,
	}
	go s.notifyWatchers(event)

	return newValue, nil
}

// Delete removes a value. Only supported when WithWritable() is used.
func (s *Store) Delete(ctx context.Context, namespace, key string) error {
	if !s.opts.writable {
		return config.ErrReadOnly
	}
	if s.closed.Load() {
		return config.ErrStoreClosed
	}
	if err := config.ValidateNamespace(namespace); err != nil {
		return err
	}
	if err := config.ValidateKey(key); err != nil {
		return err
	}

	if err := s.sc.delete(namespace, key); err != nil {
		return config.WrapStoreError("delete", "file", key, err)
	}

	s.mu.Lock()
	if s.entries[namespace] != nil {
		delete(s.entries[namespace], key)
	}
	s.mu.Unlock()

	event := config.ChangeEvent{
		Type:      config.ChangeTypeDelete,
		Namespace: namespace,
		Key:       key,
		Value:     nil,
		Timestamp: time.Now().UTC(),
	}
	go s.notifyWatchers(event)

	return nil
}

// Find returns entries matching the filter within a namespace.
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

	s.mu.RLock()
	defer s.mu.RUnlock()

	nsEntries := s.entries[namespace]
	results := make(map[string]config.Value)

	// Keys mode
	if keys := filter.Keys(); len(keys) > 0 {
		for _, key := range keys {
			if e, ok := nsEntries[key]; ok {
				val, err := e.toValue(ctx)
				if err != nil {
					return nil, fmt.Errorf("file: find key %q in %q: %w", key, namespace, err)
				}
				results[key] = val
			}
		}
		return config.NewPage(results, "", 0), nil
	}

	// Prefix mode with pagination
	prefix := filter.Prefix()
	cursor := filter.Cursor()
	limit := filter.Limit()

	type entryWithID struct {
		e  *storeEntry
		id int64
	}
	var matching []entryWithID

	for _, e := range nsEntries {
		if prefix != "" && !strings.HasPrefix(e.key, prefix) {
			continue
		}
		id, _ := strconv.ParseInt(e.id, 10, 64)
		if cursor != "" {
			cursorID, _ := strconv.ParseInt(cursor, 10, 64)
			if id <= cursorID {
				continue
			}
		}
		matching = append(matching, entryWithID{e: e, id: id})
	}

	sort.Slice(matching, func(i, j int) bool {
		return matching[i].id < matching[j].id
	})

	if limit > 0 && len(matching) > limit {
		matching = matching[:limit]
	}

	var lastID string
	for _, m := range matching {
		val, err := m.e.toValue(ctx)
		if err != nil {
			return nil, fmt.Errorf("file: find key %q in %q: %w", m.e.key, namespace, err)
		}
		results[m.e.key] = val
		lastID = m.e.id
	}

	return config.NewPage(results, lastID, limit), nil
}

// Watch returns a channel for change events. Only supported when WithWritable() is used.
func (s *Store) Watch(ctx context.Context, filter config.WatchFilter) (<-chan config.ChangeEvent, error) {
	if !s.opts.writable {
		return nil, config.ErrWatchNotSupported
	}
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
		case <-s.pollStop:
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

// Health returns nil if the store has loaded successfully.
func (s *Store) Health(ctx context.Context) error {
	if s.closed.Load() {
		return config.ErrStoreClosed
	}
	if !s.loader.Loaded() {
		return config.ErrStoreNotConnected
	}
	return nil
}

// Stats returns store statistics.
func (s *Store) Stats(ctx context.Context) (*config.StoreStats, error) {
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := &config.StoreStats{
		EntriesByType:      make(map[config.Type]int64),
		EntriesByNamespace: make(map[string]int64),
	}

	for ns, entries := range s.entries {
		for _, e := range entries {
			stats.TotalEntries++
			stats.EntriesByType[e.valueType]++
			stats.EntriesByNamespace[ns]++
		}
	}

	return stats, nil
}

// Namespaces returns all namespace names in the store.
func (s *Store) Namespaces() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ns := make([]string, 0, len(s.entries))
	for name := range s.entries {
		ns = append(ns, name)
	}
	sort.Strings(ns)
	return ns
}

// startPolling polls the base file and sidecar for changes.
// The caller is responsible for pollWg.Add(1) before invoking this goroutine
// so Close() can safely Wait even if startPolling has not yet begun executing.
func (s *Store) startPolling() {
	defer s.pollWg.Done()

	ticker := time.NewTicker(s.opts.watchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.pollStop:
			return
		case <-ticker.C:
			s.poll()
		}
	}
}

func (s *Store) poll() {
	ctx := context.Background()

	_ = s.loader.Load()
	_ = s.sc.load()

	now := time.Now().UTC()

	s.mu.Lock()

	// Snapshot old entries for diffing.
	old := make(map[string]map[string]*storeEntry)
	for ns, keys := range s.entries {
		kcp := make(map[string]*storeEntry, len(keys))
		for k, e := range keys {
			kcp[k] = e
		}
		old[ns] = kcp
	}

	// Rebuild from base file.
	// Do NOT reset nextID: Set() allocates IDs monotonically and resetting
	// here would cause a subsequent Set to collide with an existing ID.
	raw := s.loader.AllSettings()
	s.entries = make(map[string]map[string]*storeEntry)

	for topKey, topVal := range raw {
		m, ok := topVal.(map[string]any)
		if !ok {
			ns := s.opts.defaultNamespace
			if err := s.addEntryValidated(ctx, ns, topKey, topVal, now); err != nil {
				s.logWarn("skipping entry", "namespace", ns, "key", topKey, "error", err)
			}
			continue
		}
		if err := config.ValidateNamespace(topKey); err != nil {
			continue
		}
		s.flattenMap(ctx, topKey, "", m, now)
	}

	// Apply sidecar.
	s.applySidecar(ctx, now)

	// Diff new vs old to determine events.
	type diffEvent struct {
		ct    config.ChangeType
		ns    string
		key   string
		entry *storeEntry
	}
	var events []diffEvent

	// Check for sets and updates.
	for ns, keys := range s.entries {
		for key, newEntry := range keys {
			oldEntry := old[ns][key]
			if oldEntry == nil || string(oldEntry.value) != string(newEntry.value) {
				events = append(events, diffEvent{ct: config.ChangeTypeSet, ns: ns, key: key, entry: newEntry})
			}
		}
	}

	// Check for deletes.
	for ns, keys := range old {
		for key := range keys {
			if s.entries[ns] == nil || s.entries[ns][key] == nil {
				events = append(events, diffEvent{ct: config.ChangeTypeDelete, ns: ns, key: key})
			}
		}
	}

	s.mu.Unlock()

	for _, ev := range events {
		var val config.Value
		if ev.ct == config.ChangeTypeSet && ev.entry != nil {
			var err error
			val, err = ev.entry.toValue(ctx)
			if err != nil {
				continue
			}
		}
		event := config.ChangeEvent{
			Type:      ev.ct,
			Namespace: ev.ns,
			Key:       ev.key,
			Value:     val,
			Timestamp: now,
		}
		s.notifyWatchers(event)
	}
}

// notifyWatchers sends an event to all matching watchers.
func (s *Store) notifyWatchers(event config.ChangeEvent) {
	s.watchMu.RLock()
	watchers := make([]*watchEntry, 0, len(s.watchers))
	for we := range s.watchers {
		watchers = append(watchers, we)
	}
	s.watchMu.RUnlock()

	for _, we := range watchers {
		if config.MatchesWatchFilter(event, we.filter) {
			s.sendToWatcher(we, event)
		}
	}
}

// sendToWatcher safely sends an event to a watcher.
func (s *Store) sendToWatcher(we *watchEntry, event config.ChangeEvent) {
	we.mu.Lock()
	defer we.mu.Unlock()
	if we.closed {
		return
	}
	select {
	case we.ch <- event:
	default:
		// drop event (buffer full)
	}
}

// flattenMap recursively flattens a map into store entries.
func (s *Store) flattenMap(ctx context.Context, namespace, prefix string, m map[string]any, now time.Time) {
	for k, v := range m {
		key := k
		if prefix != "" {
			key = prefix + s.opts.keySeparator + k
		}

		switch val := v.(type) {
		case map[string]any:
			// Recurse into nested maps
			s.flattenMap(ctx, namespace, key, val, now)
		default:
			// Leaf value (scalar, list, etc.)
			if err := s.addEntryValidated(ctx, namespace, key, val, now); err != nil {
				s.logWarn("skipping entry", "namespace", namespace, "key", key, "error", err)
			}
		}
	}
}

// addEntryValidated validates the key before creating a store entry.
func (s *Store) addEntryValidated(ctx context.Context, namespace, key string, value any, now time.Time) error {
	if err := config.ValidateKey(key); err != nil {
		return fmt.Errorf("invalid key: %w", err)
	}
	return s.addEntry(ctx, namespace, key, value, now)
}

// addEntry creates a store entry for a single key-value pair.
func (s *Store) addEntry(ctx context.Context, namespace, key string, value any, now time.Time) error {
	c := codec.Default()
	data, err := c.Encode(ctx, value)
	if err != nil {
		return fmt.Errorf("encode %q/%q: %w", namespace, key, err)
	}

	s.nextID++
	e := &storeEntry{
		id:        fmt.Sprintf("%d", s.nextID),
		key:       key,
		namespace: namespace,
		value:     data,
		codec:     c.Name(),
		valueType: config.NewValue(value).Type(),
		version:   1,
		createdAt: now,
		updatedAt: now,
	}

	if s.entries[namespace] == nil {
		s.entries[namespace] = make(map[string]*storeEntry)
	}
	s.entries[namespace][key] = e
	return nil
}

// logWarn logs a warning if a logger is configured.
func (s *Store) logWarn(msg string, args ...any) {
	if logger := s.loader.opts.logger; logger != nil {
		logger.Warn(msg, args...)
	}
}
