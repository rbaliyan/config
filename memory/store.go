package memory

import (
	"context"
	"fmt"
	"path"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/codec"
)

// entry is the internal storage representation.
type entry struct {
	id        string // Unique ID for pagination
	key       string
	namespace string
	tags      string // Sorted "key1=value1,key2=value2"
	value     []byte
	codec     string
	valueType config.Type
	version   int64
	createdAt time.Time
	updatedAt time.Time
}

func (e *entry) clone() *entry {
	if e == nil {
		return nil
	}
	clone := *e
	if e.value != nil {
		clone.value = make([]byte, len(e.value))
		copy(clone.value, e.value)
	}
	return &clone
}

func (e *entry) toValue() (config.Value, error) {
	// Parse tags from string
	tags, _ := config.ParseTags(e.tags)

	return config.NewValueFromBytes(
		e.value,
		e.codec,
		config.WithValueType(e.valueType),
		config.WithValueMetadata(e.version, e.createdAt, e.updatedAt),
		config.WithValueTags(tags),
	)
}

// Store is an in-memory configuration store implementation.
// Suitable for testing and single-instance deployments.
type Store struct {
	mu       sync.RWMutex
	entries  map[string]*entry // key format: "namespace:key" or "namespace:key:tags"
	nextID   int64             // Auto-increment ID for pagination
	closed   atomic.Bool
	stopChan chan struct{}

	// Watch management
	watchMu       sync.RWMutex
	watchers      map[*watchEntry]struct{}
	droppedEvents atomic.Int64 // Counter for dropped events due to full channels

	bufferSize int
	codec      codec.Codec
	onDropped  func(event config.ChangeEvent) // Optional callback when event is dropped
}

type watchEntry struct {
	filter    config.WatchFilter
	ch        chan config.ChangeEvent
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
}

// Option configures the memory store.
type Option func(*Store)

// WithWatchBufferSize sets the buffer size for watch channels.
func WithWatchBufferSize(size int) Option {
	return func(s *Store) {
		if size > 0 {
			s.bufferSize = size
		}
	}
}

// WithCodec sets the codec for encoding values.
func WithCodec(c codec.Codec) Option {
	return func(s *Store) {
		if c != nil {
			s.codec = c
		}
	}
}

// WithOnDropped sets a callback that is invoked when a watch event is dropped
// due to a full channel buffer. This can be used for logging or metrics.
// The callback is invoked synchronously, so it should be fast.
func WithOnDropped(fn func(event config.ChangeEvent)) Option {
	return func(s *Store) {
		s.onDropped = fn
	}
}

// NewStore creates a new in-memory store.
func NewStore(opts ...Option) *Store {
	s := &Store{
		entries:    make(map[string]*entry),
		watchers:   make(map[*watchEntry]struct{}),
		stopChan:   make(chan struct{}),
		bufferSize: 100,
		codec:      codec.Default(),
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

// Compile-time interface checks
var (
	_ config.Store         = (*Store)(nil)
	_ config.HealthChecker = (*Store)(nil)
	_ config.StatsProvider = (*Store)(nil)
	_ config.BulkStore     = (*Store)(nil)
)

// keySeparator is used to separate key components to avoid collisions.
// Using null byte as it's virtually never used in config keys.
const keySeparator = "\x00"

func (s *Store) entryKey(namespace, key string, tags []config.Tag) string {
	tagStr := config.FormatTags(tags)
	if tagStr == "" {
		return namespace + keySeparator + key
	}
	return namespace + keySeparator + key + keySeparator + tagStr
}

// Connect establishes connection (no-op for memory store).
func (s *Store) Connect(ctx context.Context) error {
	return nil
}

// Close releases resources and stops watchers.
func (s *Store) Close(ctx context.Context) error {
	if s.closed.Swap(true) {
		return nil // Already closed
	}

	close(s.stopChan)

	// Copy watchers and clear map while holding lock, then close channels outside lock
	// This prevents blocking other operations while closing channels
	s.watchMu.Lock()
	toClose := make([]*watchEntry, 0, len(s.watchers))
	for entry := range s.watchers {
		toClose = append(toClose, entry)
	}
	s.watchers = make(map[*watchEntry]struct{})
	s.watchMu.Unlock()

	// Close all watchers outside the lock
	for _, entry := range toClose {
		entry.cancel()
		entry.closeOnce.Do(func() {
			close(entry.ch)
		})
	}

	return nil
}

// Get retrieves a configuration value by namespace, key, and optional tags.
func (s *Store) Get(ctx context.Context, namespace, key string, tags ...config.Tag) (config.Value, error) {
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
	}
	if err := config.ValidateNamespace(namespace); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.entries[s.entryKey(namespace, key, tags)]
	if !ok {
		return nil, &config.KeyNotFoundError{Key: key, Namespace: namespace}
	}

	return e.clone().toValue()
}

// Set creates or updates a configuration value.
func (s *Store) Set(ctx context.Context, namespace, key string, value config.Value) error {
	if s.closed.Load() {
		return config.ErrStoreClosed
	}
	if err := config.ValidateNamespace(namespace); err != nil {
		return err
	}
	if key == "" {
		return config.ErrInvalidKey
	}

	// Marshal the value
	data, err := value.Marshal()
	if err != nil {
		return config.WrapStoreError("marshal", namespace, key, err)
	}

	// Extract tags from value metadata
	var tags []config.Tag
	if meta := value.Metadata(); meta != nil {
		tags = meta.Tags()
	}
	tagStr := config.FormatTags(tags)

	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	ek := s.entryKey(namespace, key, tags)
	existing, exists := s.entries[ek]

	newEntry := &entry{
		key:       key,
		namespace: namespace,
		tags:      tagStr,
		value:     data,
		codec:     value.Codec(),
		valueType: value.Type(),
	}

	if exists {
		// Update: increment version, preserve created_at and ID
		newEntry.id = existing.id
		newEntry.version = existing.version + 1
		newEntry.createdAt = existing.createdAt
		newEntry.updatedAt = now
	} else {
		// Create: initialize version, timestamps, and generate ID
		s.nextID++
		newEntry.id = fmt.Sprintf("%d", s.nextID)
		newEntry.version = 1
		newEntry.createdAt = now
		newEntry.updatedAt = now
	}

	s.entries[ek] = newEntry

	// Build Value for notification
	newValue, _ := newEntry.clone().toValue()

	// Notify watchers asynchronously
	event := config.ChangeEvent{
		Type:      config.ChangeTypeSet,
		Namespace: namespace,
		Key:       key,
		Tags:      tags,
		Value:     newValue,
		Timestamp: now,
	}
	go s.notifyWatchers(event)

	return nil
}

// Delete removes a configuration value by namespace, key, and optional tags.
func (s *Store) Delete(ctx context.Context, namespace, key string, tags ...config.Tag) error {
	if s.closed.Load() {
		return config.ErrStoreClosed
	}
	if err := config.ValidateNamespace(namespace); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	ek := s.entryKey(namespace, key, tags)
	if _, ok := s.entries[ek]; !ok {
		return &config.KeyNotFoundError{Key: key, Namespace: namespace}
	}

	delete(s.entries, ek)

	// Notify watchers
	event := config.ChangeEvent{
		Type:      config.ChangeTypeDelete,
		Namespace: namespace,
		Key:       key,
		Tags:      tags,
		Value:     nil,
		Timestamp: time.Now().UTC(),
	}
	go s.notifyWatchers(event)

	return nil
}

// Find returns a page of keys and values matching the filter within a namespace.
func (s *Store) Find(ctx context.Context, namespace string, filter config.Filter) (config.Page, error) {
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
	}
	if err := config.ValidateNamespace(namespace); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	results := make(map[string]config.Value)
	filterTags := filter.Tags()

	// Keys mode: get specific keys (no pagination, but respect tag filter)
	if keys := filter.Keys(); len(keys) > 0 {
		for _, key := range keys {
			// Find entries matching the key (may have different tags)
			for _, e := range s.entries {
				if e.namespace != namespace || e.key != key {
					continue
				}
				// Filter by tags if specified
				if len(filterTags) > 0 {
					entryTags, _ := config.ParseTags(e.tags)
					if !config.MatchTags(entryTags, filterTags) {
						continue
					}
				}
				if val, err := e.clone().toValue(); err == nil {
					results[key] = val
				}
			}
		}
		return config.NewPage(results, "", 0), nil
	}

	// Prefix mode: get all entries matching prefix and tags
	prefix := filter.Prefix()
	cursor := filter.Cursor()
	limit := filter.Limit()

	// Collect matching entries
	type entryWithID struct {
		e  *entry
		id int64
	}
	var matching []entryWithID

	for _, e := range s.entries {
		if e.namespace != namespace {
			continue
		}
		if prefix != "" && !strings.HasPrefix(e.key, prefix) {
			continue
		}
		// Filter by tags if specified
		if len(filterTags) > 0 {
			entryTags, _ := config.ParseTags(e.tags)
			if !config.MatchTags(entryTags, filterTags) {
				continue
			}
		}
		// Parse ID for cursor comparison
		id, _ := strconv.ParseInt(e.id, 10, 64)
		if cursor != "" {
			cursorID, _ := strconv.ParseInt(cursor, 10, 64)
			if id <= cursorID {
				continue
			}
		}
		matching = append(matching, entryWithID{e: e, id: id})
	}

	// Sort by ID for consistent pagination
	sort.Slice(matching, func(i, j int) bool {
		return matching[i].id < matching[j].id
	})

	// Apply limit
	if limit > 0 && len(matching) > limit {
		matching = matching[:limit]
	}

	// Build results map
	var lastID string
	for _, m := range matching {
		if val, err := m.e.clone().toValue(); err == nil {
			results[m.e.key] = val
			lastID = m.e.id
		}
	}

	return config.NewPage(results, lastID, limit), nil
}

// Watch returns a channel that receives change events.
func (s *Store) Watch(ctx context.Context, filter config.WatchFilter) (<-chan config.ChangeEvent, error) {
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
	}

	ctx, cancel := context.WithCancel(ctx)
	ch := make(chan config.ChangeEvent, s.bufferSize)

	we := &watchEntry{
		filter: filter,
		ch:     ch,
		ctx:    ctx,
		cancel: cancel,
	}

	s.watchMu.Lock()
	s.watchers[we] = struct{}{}
	s.watchMu.Unlock()

	// Cleanup when context is cancelled
	go func() {
		select {
		case <-ctx.Done():
		case <-s.stopChan:
		}

		s.watchMu.Lock()
		delete(s.watchers, we)
		s.watchMu.Unlock()

		// Close channel safely using sync.Once
		we.closeOnce.Do(func() {
			close(ch)
		})
	}()

	return ch, nil
}

// Health performs a health check.
func (s *Store) Health(ctx context.Context) error {
	if s.closed.Load() {
		return config.ErrStoreClosed
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
		TotalEntries:       int64(len(s.entries)),
		EntriesByType:      make(map[config.Type]int64),
		EntriesByNamespace: make(map[string]int64),
	}

	for _, e := range s.entries {
		stats.EntriesByType[e.valueType]++
		stats.EntriesByNamespace[e.namespace]++
	}

	return stats, nil
}

// notifyWatchers sends an event to all matching watchers.
func (s *Store) notifyWatchers(event config.ChangeEvent) {
	// Check if store is closed
	if s.closed.Load() {
		return
	}

	// Copy watchers while holding lock, then send outside lock
	// This prevents blocking watcher registration/cleanup during sends
	s.watchMu.RLock()
	watchers := make([]*watchEntry, 0, len(s.watchers))
	for we := range s.watchers {
		watchers = append(watchers, we)
	}
	s.watchMu.RUnlock()

	for _, we := range watchers {
		if s.matchesFilter(event, we.filter) {
			s.sendToWatcher(we, event)
		}
	}
}

// sendToWatcher safely sends an event to a watcher, handling closed channels.
func (s *Store) sendToWatcher(we *watchEntry, event config.ChangeEvent) {
	// Use defer/recover to handle the case where the channel is closed
	// between when we copied the watchers list and when we try to send
	defer func() {
		if r := recover(); r != nil {
			// Channel was closed, this is expected during shutdown
		}
	}()

	select {
	case we.ch <- event:
	case <-we.ctx.Done():
	default:
		// Channel full, increment dropped counter and notify callback
		s.droppedEvents.Add(1)
		if s.onDropped != nil {
			s.onDropped(event)
		}
	}
}

// DroppedEvents returns the total number of watch events that were dropped
// due to full channel buffers since the store was created.
func (s *Store) DroppedEvents() int64 {
	return s.droppedEvents.Load()
}

// matchesFilter checks if an event matches a watch filter.
func (s *Store) matchesFilter(event config.ChangeEvent, filter config.WatchFilter) bool {
	// Check namespace filter
	if len(filter.Namespaces) > 0 && !slices.Contains(filter.Namespaces, event.Namespace) {
		return false
	}

	// Check prefix filter
	if len(filter.Prefixes) > 0 {
		found := false
		for _, prefix := range filter.Prefixes {
			if matchPattern(prefix, event.Key) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}

// matchPattern checks if key matches a glob pattern.
func matchPattern(pattern, key string) bool {
	if pattern == "" || pattern == "*" {
		return true
	}

	// Use path.Match for glob matching
	matched, _ := path.Match(pattern, key)
	if matched {
		return true
	}

	// Also support prefix matching (e.g., "app/database" matches "app/database/timeout")
	if strings.HasPrefix(key, pattern) {
		return true
	}

	// Support wildcard suffix (e.g., "app/*" matches "app/database")
	if strings.HasSuffix(pattern, "*") {
		prefix := pattern[:len(pattern)-1]
		return strings.HasPrefix(key, prefix)
	}

	return false
}

// GetMany retrieves multiple values in a single operation.
// Note: This only retrieves entries without tags. For tagged entries, use Get with tags.
func (s *Store) GetMany(ctx context.Context, namespace string, keys []string) (map[string]config.Value, error) {
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	results := make(map[string]config.Value, len(keys))
	for _, key := range keys {
		if e, ok := s.entries[s.entryKey(namespace, key, nil)]; ok {
			if val, err := e.clone().toValue(); err == nil {
				results[key] = val
			}
		}
	}

	return results, nil
}

// SetMany creates or updates multiple values in a single operation.
func (s *Store) SetMany(ctx context.Context, namespace string, values map[string]config.Value) error {
	if s.closed.Load() {
		return config.ErrStoreClosed
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	events := make([]config.ChangeEvent, 0, len(values))

	for key, value := range values {
		if key == "" {
			continue
		}

		data, err := value.Marshal()
		if err != nil {
			continue
		}

		// Extract tags from value metadata
		var tags []config.Tag
		if meta := value.Metadata(); meta != nil {
			tags = meta.Tags()
		}
		tagStr := config.FormatTags(tags)

		ek := s.entryKey(namespace, key, tags)
		existing, exists := s.entries[ek]

		newEntry := &entry{
			key:       key,
			namespace: namespace,
			tags:      tagStr,
			value:     data,
			codec:     value.Codec(),
			valueType: value.Type(),
		}

		if exists {
			newEntry.id = existing.id
			newEntry.version = existing.version + 1
			newEntry.createdAt = existing.createdAt
			newEntry.updatedAt = now
		} else {
			s.nextID++
			newEntry.id = fmt.Sprintf("%d", s.nextID)
			newEntry.version = 1
			newEntry.createdAt = now
			newEntry.updatedAt = now
		}

		s.entries[ek] = newEntry

		newValue, _ := newEntry.clone().toValue()
		event := config.ChangeEvent{
			Type:      config.ChangeTypeSet,
			Namespace: namespace,
			Key:       key,
			Tags:      tags,
			Value:     newValue,
			Timestamp: now,
		}
		events = append(events, event)
	}

	// Notify watchers for all changes
	go func() {
		for _, event := range events {
			s.notifyWatchers(event)
		}
	}()

	return nil
}

// DeleteMany removes multiple values in a single operation.
// Note: This only deletes entries without tags. For tagged entries, use Delete with tags.
func (s *Store) DeleteMany(ctx context.Context, namespace string, keys []string) (int64, error) {
	if s.closed.Load() {
		return 0, config.ErrStoreClosed
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var deleted int64
	now := time.Now().UTC()
	events := make([]config.ChangeEvent, 0, len(keys))

	for _, key := range keys {
		ek := s.entryKey(namespace, key, nil)
		if _, ok := s.entries[ek]; ok {
			delete(s.entries, ek)
			deleted++

			events = append(events, config.ChangeEvent{
				Type:      config.ChangeTypeDelete,
				Namespace: namespace,
				Key:       key,
				Tags:      nil, // DeleteMany only works on entries without tags
				Value:     nil,
				Timestamp: now,
			})
		}
	}

	// Notify watchers for all changes
	go func() {
		for _, event := range events {
			s.notifyWatchers(event)
		}
	}()

	return deleted, nil
}
