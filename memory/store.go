// Package memory provides an in-memory configuration store for testing and single-instance deployments.
package memory

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
)

// historyEntry stores a snapshot of a value at a specific version.
type historyEntry struct {
	value     config.Value
	version   int64
	updatedAt time.Time
}

// entry is the internal storage representation.
type entry struct {
	id        string // Unique ID for pagination
	key       string
	namespace string
	value     config.Value
	version   int64
	createdAt time.Time
	updatedAt time.Time
	history   []historyEntry // all versions ordered by version ascending
}

// toValue returns the stored Value wrapped with the entry's current metadata.
func (e *entry) toValue() config.Value {
	return &metadataValue{
		Value:     e.value,
		version:   e.version,
		createdAt: e.createdAt,
		updatedAt: e.updatedAt,
		entryID:   e.id,
	}
}

// metadataValue wraps a config.Value with store-level metadata (version, timestamps, ID).
type metadataValue struct {
	config.Value
	version   int64
	createdAt time.Time
	updatedAt time.Time
	entryID   string
}

func (v *metadataValue) Metadata() config.Metadata {
	return &entryMetadata{
		version:   v.version,
		createdAt: v.createdAt,
		updatedAt: v.updatedAt,
		entryID:   v.entryID,
	}
}

// entryMetadata implements config.Metadata and the EntryID() method used by
// store internals via structural type assertion.
type entryMetadata struct {
	version   int64
	createdAt time.Time
	updatedAt time.Time
	entryID   string
}

func (m *entryMetadata) Version() int64       { return m.version }
func (m *entryMetadata) CreatedAt() time.Time { return m.createdAt }
func (m *entryMetadata) UpdatedAt() time.Time { return m.updatedAt }
func (m *entryMetadata) IsStale() bool        { return false }
func (m *entryMetadata) EntryID() string      { return m.entryID }

// Store is an in-memory configuration store implementation.
// Suitable for testing and single-instance deployments.
type Store struct {
	mu       sync.RWMutex
	entries  map[string]*entry // key format: "namespace\x00key"
	nextID   int64             // Auto-increment ID for pagination
	closed   atomic.Bool
	stopChan chan struct{}

	// Alias management
	aliasMu sync.RWMutex
	aliases map[string]*aliasEntry // alias key → alias entry

	// Watch management
	watchMu       sync.RWMutex
	watchers      map[*watchEntry]struct{}
	droppedEvents atomic.Int64 // Counter for dropped events due to full channels

	bufferSize int
	maxHistory int                             // Max historical versions per key (0 = unlimited)
	onDropped  func(event config.ChangeEvent) // Optional callback when event is dropped
}

// aliasEntry is the internal storage representation for an alias.
type aliasEntry struct {
	alias     string
	target    string
	version   int64
	createdAt time.Time
}

// toValue returns the alias target wrapped as a config.Value with metadata.
func (e *aliasEntry) toValue() config.Value {
	return &metadataValue{
		Value:     config.NewValue(e.target),
		version:   e.version,
		createdAt: e.createdAt,
		updatedAt: e.createdAt,
		entryID:   "alias:" + e.alias,
	}
}

type watchEntry struct {
	filter    config.WatchFilter
	ch        chan config.ChangeEvent
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.Mutex // protects send/close operations
	closed    bool       // guarded by mu
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

// WithOnDropped sets a callback that is invoked when a watch event is dropped
// due to a full channel buffer. This can be used for logging or metrics.
// The callback is invoked synchronously, so it should be fast.
func WithOnDropped(fn func(event config.ChangeEvent)) Option {
	return func(s *Store) {
		s.onDropped = fn
	}
}

// WithMaxHistory sets the maximum number of historical versions to retain per key.
// When exceeded, the oldest versions are discarded. 0 means unlimited (default).
func WithMaxHistory(n int) Option {
	return func(s *Store) {
		if n >= 0 {
			s.maxHistory = n
		}
	}
}

// NewStore creates a new in-memory store.
func NewStore(opts ...Option) *Store {
	s := &Store{
		entries:    make(map[string]*entry),
		aliases:    make(map[string]*aliasEntry),
		watchers:   make(map[*watchEntry]struct{}),
		stopChan:   make(chan struct{}),
		bufferSize: 100,
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

// Compile-time interface checks
var (
	_ config.Store          = (*Store)(nil)
	_ config.HealthChecker  = (*Store)(nil)
	_ config.StatsProvider  = (*Store)(nil)
	_ config.BulkStore      = (*Store)(nil)
	_ config.VersionedStore = (*Store)(nil)
	_ config.AliasStore     = (*Store)(nil)
)

// keySeparator uses null byte to avoid collisions.
// Neither namespace nor key can contain null bytes (per validation rules),
// so "ns\x00key" is guaranteed unique for any (namespace, key) pair.
const keySeparator = "\x00"

func (s *Store) entryKey(namespace, key string) string {
	return namespace + keySeparator + key
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
			entry.mu.Lock()
			entry.closed = true
			close(entry.ch)
			entry.mu.Unlock()
		})
	}

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

	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.entries[s.entryKey(namespace, key)]
	if !ok {
		return nil, &config.KeyNotFoundError{Key: key, Namespace: namespace}
	}

	return e.toValue(), nil
}

// Set creates or updates a configuration value.
// Returns the stored Value with updated metadata (version, timestamps).
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

	// Validate that the value can be marshaled (same as real stores).
	if _, err := value.Marshal(ctx); err != nil {
		return nil, config.WrapStoreError("marshal", "memory", key, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	ek := s.entryKey(namespace, key)
	existing, exists := s.entries[ek]

	// Check write mode conditions
	writeMode := config.GetWriteMode(value)
	switch writeMode {
	case config.WriteModeCreate:
		if exists {
			return nil, &config.KeyExistsError{Key: key, Namespace: namespace}
		}
	case config.WriteModeUpdate:
		if !exists {
			return nil, &config.KeyNotFoundError{Key: key, Namespace: namespace}
		}
	}

	newEntry := &entry{
		key:       key,
		namespace: namespace,
		value:     value,
	}

	if exists {
		// Update: preserve history, increment version, preserve created_at and ID
		newEntry.id = existing.id
		newEntry.version = existing.version + 1
		newEntry.createdAt = existing.createdAt
		newEntry.updatedAt = now
		// Copy existing history and append the previous current version.
		// We copy to avoid aliasing the old entry's backing array.
		newEntry.history = make([]historyEntry, len(existing.history), len(existing.history)+1)
		copy(newEntry.history, existing.history)
		newEntry.history = append(newEntry.history, historyEntry{
			value:     existing.value,
			version:   existing.version,
			updatedAt: existing.updatedAt,
		})
		s.trimHistory(newEntry)
	} else {
		// Create: initialize version, timestamps, and generate ID
		s.nextID++
		newEntry.id = fmt.Sprintf("%d", s.nextID)
		newEntry.version = 1
		newEntry.createdAt = now
		newEntry.updatedAt = now
	}

	s.entries[ek] = newEntry

	// Build Value for return and notification
	newValue := newEntry.toValue()

	// Notify watchers asynchronously
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

// Delete removes a configuration value by namespace and key.
// Note: deleting a key also removes all version history for that key.
func (s *Store) Delete(ctx context.Context, namespace, key string) error {
	if s.closed.Load() {
		return config.ErrStoreClosed
	}
	if err := config.ValidateNamespace(namespace); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	ek := s.entryKey(namespace, key)
	if _, ok := s.entries[ek]; !ok {
		return &config.KeyNotFoundError{Key: key, Namespace: namespace}
	}

	delete(s.entries, ek)

	// Notify watchers
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

	s.mu.RLock()
	defer s.mu.RUnlock()

	results := make(map[string]config.Value)

	// Keys mode: get specific keys
	if keys := filter.Keys(); len(keys) > 0 {
		for _, key := range keys {
			if e, ok := s.entries[s.entryKey(namespace, key)]; ok {
				results[key] = e.toValue()
			}
		}
		return config.NewPage(results, "", 0), nil
	}

	// Prefix mode: get all entries matching prefix
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
		results[m.e.key] = m.e.toValue()
		lastID = m.e.id
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

		// Close channel safely with mutex to prevent race with sendToWatcher
		we.closeOnce.Do(func() {
			we.mu.Lock()
			we.closed = true
			close(ch)
			we.mu.Unlock()
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
		stats.EntriesByType[e.value.Type()]++
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
		if config.MatchesWatchFilter(event, we.filter) {
			s.sendToWatcher(we, event)
		}
	}
}

// sendToWatcher safely sends an event to a watcher, handling closed channels.
func (s *Store) sendToWatcher(we *watchEntry, event config.ChangeEvent) {
	// Use mutex to coordinate with channel close
	we.mu.Lock()
	if we.closed {
		we.mu.Unlock()
		return
	}

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
	we.mu.Unlock()
}

// DroppedEvents returns the total number of watch events that were dropped
// due to full channel buffers since the store was created.
func (s *Store) DroppedEvents() int64 {
	return s.droppedEvents.Load()
}

// GetMany retrieves multiple values in a single operation.
func (s *Store) GetMany(ctx context.Context, namespace string, keys []string) (map[string]config.Value, error) {
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
	}
	if err := config.ValidateNamespace(namespace); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	results := make(map[string]config.Value, len(keys))
	for _, key := range keys {
		if e, ok := s.entries[s.entryKey(namespace, key)]; ok {
			results[key] = e.toValue()
		}
	}

	return results, nil
}

// SetMany creates or updates multiple values in a single operation.
// Returns a BulkWriteError if any individual set fails, indicating which keys
// succeeded and which failed. Successfully set values are persisted even if some fail.
func (s *Store) SetMany(ctx context.Context, namespace string, values map[string]config.Value) error {
	if s.closed.Load() {
		return config.ErrStoreClosed
	}
	if err := config.ValidateNamespace(namespace); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	events := make([]config.ChangeEvent, 0, len(values))
	keyErrors := make(map[string]error)
	succeeded := make([]string, 0, len(values))

	for key, value := range values {
		if err := config.ValidateKey(key); err != nil {
			keyErrors[key] = err
			continue
		}

		// Validate that the value can be marshaled.
		if _, err := value.Marshal(ctx); err != nil {
			keyErrors[key] = config.WrapStoreError("marshal", "memory", key, err)
			continue
		}

		ek := s.entryKey(namespace, key)
		existing, exists := s.entries[ek]

		newEntry := &entry{
			key:       key,
			namespace: namespace,
			value:     value,
		}

		if exists {
			newEntry.id = existing.id
			newEntry.version = existing.version + 1
			newEntry.createdAt = existing.createdAt
			newEntry.updatedAt = now
			newEntry.history = make([]historyEntry, len(existing.history), len(existing.history)+1)
			copy(newEntry.history, existing.history)
			newEntry.history = append(newEntry.history, historyEntry{
				value:     existing.value,
				version:   existing.version,
				updatedAt: existing.updatedAt,
			})
			s.trimHistory(newEntry)
		} else {
			s.nextID++
			newEntry.id = fmt.Sprintf("%d", s.nextID)
			newEntry.version = 1
			newEntry.createdAt = now
			newEntry.updatedAt = now
		}

		s.entries[ek] = newEntry
		succeeded = append(succeeded, key)

		newValue := newEntry.toValue()
		event := config.ChangeEvent{
			Type:      config.ChangeTypeSet,
			Namespace: namespace,
			Key:       key,
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

	if len(keyErrors) > 0 {
		return &config.BulkWriteError{
			Errors:    keyErrors,
			Succeeded: succeeded,
		}
	}
	return nil
}

// DeleteMany removes multiple values in a single operation.
func (s *Store) DeleteMany(ctx context.Context, namespace string, keys []string) (int64, error) {
	if s.closed.Load() {
		return 0, config.ErrStoreClosed
	}
	if err := config.ValidateNamespace(namespace); err != nil {
		return 0, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var deleted int64
	now := time.Now().UTC()
	events := make([]config.ChangeEvent, 0, len(keys))

	for _, key := range keys {
		ek := s.entryKey(namespace, key)
		if _, ok := s.entries[ek]; ok {
			delete(s.entries, ek)
			deleted++

			events = append(events, config.ChangeEvent{
				Type:      config.ChangeTypeDelete,
				Namespace: namespace,
				Key:       key,
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

// GetVersions retrieves version history for a configuration key.
func (s *Store) GetVersions(ctx context.Context, namespace, key string, filter config.VersionFilter) (config.VersionPage, error) {
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
	}
	if err := config.ValidateNamespace(namespace); err != nil {
		return nil, err
	}
	if err := config.ValidateKey(key); err != nil {
		return nil, err
	}
	if filter == nil {
		filter = config.NewVersionFilter().Build()
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.entries[s.entryKey(namespace, key)]
	if !ok {
		return nil, &config.KeyNotFoundError{Key: key, Namespace: namespace}
	}

	// Specific version requested
	if filter.Version() > 0 {
		v := s.findVersion(e, filter.Version())
		if v == nil {
			return nil, &config.VersionNotFoundError{Key: key, Namespace: namespace, Version: filter.Version()}
		}
		return config.NewVersionPage([]config.Value{v}, "", 1), nil
	}

	// Build all versions descending (current first, then history reversed)
	all := make([]config.Value, 0, len(e.history)+1)
	all = append(all, e.toValue()) // current version is newest
	for i := len(e.history) - 1; i >= 0; i-- {
		h := e.history[i]
		all = append(all, s.historyToValue(e, h))
	}

	// Apply cursor (cursor is a version number string; skip until we pass it)
	if filter.Cursor() != "" {
		cursorVer, err := strconv.ParseInt(filter.Cursor(), 10, 64)
		if err != nil {
			return nil, config.WrapStoreError("get_versions", "memory", key, fmt.Errorf("invalid cursor %q: %w", filter.Cursor(), err))
		}
		idx := 0
		for idx < len(all) && all[idx].Metadata().Version() >= cursorVer {
			idx++
		}
		all = all[idx:]
	}

	// Apply limit
	limit := filter.Limit()
	var nextCursor string
	if limit > 0 && len(all) > limit {
		nextCursor = strconv.FormatInt(all[limit-1].Metadata().Version(), 10)
		all = all[:limit]
	}

	return config.NewVersionPage(all, nextCursor, limit), nil
}

// findVersion returns the Value for a specific version, or nil if not found.
func (s *Store) findVersion(e *entry, version int64) config.Value {
	// Check current version
	if e.version == version {
		return e.toValue()
	}
	// Search history
	for _, h := range e.history {
		if h.version == version {
			return s.historyToValue(e, h)
		}
	}
	return nil
}

// trimHistory enforces the maxHistory limit by discarding the oldest entries.
// It copies the retained entries into a new slice to avoid pinning the old
// backing array in memory.
func (s *Store) trimHistory(e *entry) {
	if s.maxHistory > 0 && len(e.history) > s.maxHistory {
		keep := e.history[len(e.history)-s.maxHistory:]
		trimmed := make([]historyEntry, len(keep))
		copy(trimmed, keep)
		e.history = trimmed
	}
}

// historyToValue converts a historyEntry to a config.Value with metadata.
func (s *Store) historyToValue(e *entry, h historyEntry) config.Value {
	return &metadataValue{
		Value:     h.value,
		version:   h.version,
		createdAt: e.createdAt,
		updatedAt: h.updatedAt,
		entryID:   e.id,
	}
}

// SetAlias creates a new alias mapping from alias to target.
// Returns ErrAliasExists if the alias is already registered or if the alias key
// already exists as a configuration entry in any namespace.
func (s *Store) SetAlias(ctx context.Context, alias, target string) (config.Value, error) {
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
	}
	if err := config.ValidateKey(alias); err != nil {
		return nil, err
	}
	if err := config.ValidateKey(target); err != nil {
		return nil, err
	}

	// Lock both aliases and entries to enforce uniqueness across key spaces.
	s.aliasMu.Lock()
	defer s.aliasMu.Unlock()
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check alias doesn't already exist as an alias.
	if _, exists := s.aliases[alias]; exists {
		return nil, &config.AliasExistsError{Alias: alias, Reason: "alias already registered"}
	}

	// Check alias key doesn't exist as a config entry in any namespace.
	for _, e := range s.entries {
		if e.key == alias {
			return nil, &config.AliasExistsError{
				Alias:  alias,
				Reason: fmt.Sprintf("key exists in namespace %q", e.namespace),
			}
		}
	}

	now := time.Now().UTC()
	ae := &aliasEntry{
		alias:     alias,
		target:    target,
		version:   1,
		createdAt: now,
	}
	s.aliases[alias] = ae

	val := ae.toValue()
	event := config.ChangeEvent{
		Type:      config.ChangeTypeAliasSet,
		Key:       alias,
		Value:     val,
		Timestamp: now,
	}
	go s.notifyWatchers(event)

	return val, nil
}

// DeleteAlias removes an alias.
// Returns ErrNotFound if the alias does not exist.
func (s *Store) DeleteAlias(ctx context.Context, alias string) error {
	if s.closed.Load() {
		return config.ErrStoreClosed
	}

	s.aliasMu.Lock()
	defer s.aliasMu.Unlock()

	if _, exists := s.aliases[alias]; !exists {
		return &config.KeyNotFoundError{Key: alias}
	}

	delete(s.aliases, alias)

	event := config.ChangeEvent{
		Type:      config.ChangeTypeAliasDelete,
		Key:       alias,
		Timestamp: time.Now().UTC(),
	}
	go s.notifyWatchers(event)

	return nil
}

// GetAlias retrieves the target for a specific alias.
// Returns ErrNotFound if the alias does not exist.
func (s *Store) GetAlias(ctx context.Context, alias string) (config.Value, error) {
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
	}

	s.aliasMu.RLock()
	defer s.aliasMu.RUnlock()

	ae, exists := s.aliases[alias]
	if !exists {
		return nil, &config.KeyNotFoundError{Key: alias}
	}

	return ae.toValue(), nil
}

// ListAliases returns all registered aliases as a map of alias → Value.
func (s *Store) ListAliases(ctx context.Context) (map[string]config.Value, error) {
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
	}

	s.aliasMu.RLock()
	defer s.aliasMu.RUnlock()

	result := make(map[string]config.Value, len(s.aliases))
	for k, ae := range s.aliases {
		result[k] = ae.toValue()
	}
	return result, nil
}
