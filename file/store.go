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
// It is read-only — Set and Delete return config.ErrReadOnly.
//
// Top-level keys in the file become namespaces.
// Nested keys are flattened with "/" separator (configurable).
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

func (e *storeEntry) toValue() (config.Value, error) {
	return config.NewValueFromBytes(
		e.value,
		e.codec,
		config.WithValueType(e.valueType),
		config.WithValueMetadata(e.version, e.createdAt, e.updatedAt),
		config.WithValueEntryID(e.id),
	)
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
	}
	for _, opt := range opts {
		opt(&o)
	}

	return &Store{
		path:    path,
		opts:    o,
		loader:  New(path, o.loaderOpts...),
		entries: make(map[string]map[string]*storeEntry),
	}
}

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
			// Top-level scalar → put in default namespace
			ns := s.opts.defaultNamespace
			if err := s.addEntryValidated(ns, topKey, topVal, now); err != nil {
				s.logWarn("skipping entry", "namespace", ns, "key", topKey, "error", err)
			}
			continue
		}
		// Top-level map → namespace
		if err := config.ValidateNamespace(topKey); err != nil {
			s.logWarn("skipping namespace: invalid name", "namespace", topKey, "error", err)
			continue
		}
		s.flattenMap(topKey, "", m, now)
	}

	return nil
}

// Close releases resources.
func (s *Store) Close(ctx context.Context) error {
	if s.closed.Swap(true) {
		return nil
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

	return e.toValue()
}

// Set is not supported for file-backed stores.
func (s *Store) Set(ctx context.Context, namespace, key string, value config.Value) (config.Value, error) {
	return nil, config.ErrReadOnly
}

// Delete is not supported for file-backed stores.
func (s *Store) Delete(ctx context.Context, namespace, key string) error {
	return config.ErrReadOnly
}

// Find returns entries matching the filter within a namespace.
func (s *Store) Find(ctx context.Context, namespace string, filter config.Filter) (config.Page, error) {
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
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
				val, err := e.toValue()
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
		val, err := m.e.toValue()
		if err != nil {
			return nil, fmt.Errorf("file: find key %q in %q: %w", m.e.key, namespace, err)
		}
		results[m.e.key] = val
		lastID = m.e.id
	}

	return config.NewPage(results, lastID, limit), nil
}

// Watch is not supported for file-backed stores.
func (s *Store) Watch(ctx context.Context, filter config.WatchFilter) (<-chan config.ChangeEvent, error) {
	return nil, config.ErrWatchNotSupported
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

// flattenMap recursively flattens a map into store entries.
func (s *Store) flattenMap(namespace, prefix string, m map[string]any, now time.Time) {
	for k, v := range m {
		key := k
		if prefix != "" {
			key = prefix + s.opts.keySeparator + k
		}

		switch val := v.(type) {
		case map[string]any:
			// Recurse into nested maps
			s.flattenMap(namespace, key, val, now)
		default:
			// Leaf value (scalar, list, etc.)
			if err := s.addEntryValidated(namespace, key, val, now); err != nil {
				s.logWarn("skipping entry", "namespace", namespace, "key", key, "error", err)
			}
		}
	}
}

// addEntryValidated validates the key before creating a store entry.
func (s *Store) addEntryValidated(namespace, key string, value any, now time.Time) error {
	if err := config.ValidateKey(key); err != nil {
		return fmt.Errorf("invalid key: %w", err)
	}
	return s.addEntry(namespace, key, value, now)
}

// addEntry creates a store entry for a single key-value pair.
func (s *Store) addEntry(namespace, key string, value any, now time.Time) error {
	c := codec.Default()
	data, err := c.Encode(value)
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
		valueType: config.DetectType(value),
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

