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
	cursorpkg "github.com/rbaliyan/config/internal/cursor"
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
	expiresAt time.Time      // zero = no TTL
	history   []historyEntry // all versions ordered by version ascending
}

// isExpired reports whether the entry has a non-zero expiry time in the past.
func (e *entry) isExpired(now time.Time) bool {
	return !e.expiresAt.IsZero() && now.After(e.expiresAt)
}

// toValue returns the stored Value wrapped with the entry's current metadata.
func (e *entry) toValue() config.Value {
	return &metadataValue{
		Value:     e.value,
		version:   e.version,
		createdAt: e.createdAt,
		updatedAt: e.updatedAt,
		expiresAt: e.expiresAt,
		entryID:   e.id,
	}
}

// metadataValue wraps a config.Value with store-level metadata (version, timestamps, ID).
type metadataValue struct {
	config.Value
	version   int64
	createdAt time.Time
	updatedAt time.Time
	expiresAt time.Time
	entryID   string
}

func (v *metadataValue) Metadata() config.Metadata {
	return &entryMetadata{
		version:   v.version,
		createdAt: v.createdAt,
		updatedAt: v.updatedAt,
		expiresAt: v.expiresAt,
		entryID:   v.entryID,
	}
}

// entryMetadata implements config.Metadata and the EntryID() method used by
// store internals via structural type assertion.
type entryMetadata struct {
	version   int64
	createdAt time.Time
	updatedAt time.Time
	expiresAt time.Time
	entryID   string
}

func (m *entryMetadata) Version() int64       { return m.version }
func (m *entryMetadata) CreatedAt() time.Time { return m.createdAt }
func (m *entryMetadata) UpdatedAt() time.Time { return m.updatedAt }
func (m *entryMetadata) ExpiresAt() time.Time { return m.expiresAt }
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

	bufferSize      int
	maxHistory      int                            // Max historical versions per key (0 = unlimited)
	cleanupInterval time.Duration                  // How often to sweep expired entries (0 = default 1m)
	onDropped       func(event config.ChangeEvent) // Optional callback when event is dropped

	// connectOnce ensures Connect's TTL sweeper goroutine is started at most
	// once even if Connect is called multiple times.
	connectOnce sync.Once
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

// WithCleanupInterval sets how often the store sweeps for expired entries and
// publishes ChangeTypeDelete events for them. Defaults to 1 minute.
// Use a short interval (e.g. 10ms) in tests to avoid slow assertions.
func WithCleanupInterval(d time.Duration) Option {
	return func(s *Store) {
		if d > 0 {
			s.cleanupInterval = d
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
	_ config.Store           = (*Store)(nil)
	_ config.HealthChecker   = (*Store)(nil)
	_ config.StatsProvider   = (*Store)(nil)
	_ config.BulkStore       = (*Store)(nil)
	_ config.VersionedStore  = (*Store)(nil)
	_ config.AliasStore      = (*Store)(nil)
	_ config.NamespaceLister = (*Store)(nil)
)

// keySeparator uses null byte to avoid collisions.
// Neither namespace nor key can contain null bytes (per validation rules),
// so "ns\x00key" is guaranteed unique for any (namespace, key) pair.
const keySeparator = "\x00"

func (s *Store) entryKey(namespace, key string) string {
	return namespace + keySeparator + key
}

// BackendName returns the stable backend identifier used in error messages.
func (s *Store) BackendName() string { return "memory" }

// Connect establishes connection and starts the TTL cleanup goroutine.
// Calling Connect more than once is safe — the sweeper is started exactly
// once for the lifetime of the store.
func (s *Store) Connect(ctx context.Context) error {
	if s.closed.Load() {
		return config.ErrStoreClosed
	}
	s.connectOnce.Do(func() {
		interval := s.cleanupInterval
		if interval <= 0 {
			interval = time.Minute
		}
		go s.runCleanup(interval)
	})
	return nil
}

// runCleanup periodically removes expired entries and notifies watchers.
func (s *Store) runCleanup(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopChan:
			return
		case now := <-ticker.C:
			s.sweepExpired(now)
		}
	}
}

// sweepExpired deletes all entries whose TTL has elapsed and fires Delete events.
func (s *Store) sweepExpired(now time.Time) {
	s.mu.Lock()
	var expired []*entry
	for _, e := range s.entries {
		if e.isExpired(now) {
			expired = append(expired, e)
		}
	}
	for _, e := range expired {
		delete(s.entries, s.entryKey(e.namespace, e.key))
	}
	s.mu.Unlock()

	for _, e := range expired {
		s.notifyWatchers(config.ChangeEvent{
			Type:      config.ChangeTypeDelete,
			Namespace: e.namespace,
			Key:       e.key,
			Timestamp: now,
		})
	}
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
// Returns ErrNotFound if the entry has expired.
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
	if e.isExpired(time.Now()) {
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
	now := time.Now().UTC()
	ek := s.entryKey(namespace, key)
	existing, exists := s.entries[ek]

	// An entry past its TTL must behave as absent for write-mode checks: a
	// Create on an expired key should succeed and an Update should fail. This
	// matches read-side semantics (Get returns ErrNotFound for expired entries)
	// and avoids depending on the sweep cadence for correctness.
	if exists && existing.isExpired(now) {
		exists = false
		existing = nil
	}

	writeMode := config.GetWriteMode(value)
	switch writeMode {
	case config.WriteModeCreate:
		if exists {
			s.mu.Unlock()
			return nil, &config.KeyExistsError{Key: key, Namespace: namespace}
		}
	case config.WriteModeUpdate:
		if !exists {
			s.mu.Unlock()
			return nil, &config.KeyNotFoundError{Key: key, Namespace: namespace}
		}
	}

	// Extract TTL from the incoming value. A non-zero expiresAt overrides any
	// existing TTL. A zero value preserves the existing TTL on updates.
	incomingExpiry := config.GetExpiresAt(value)

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
		// Preserve TTL unless caller explicitly sets a new one.
		if !incomingExpiry.IsZero() {
			newEntry.expiresAt = incomingExpiry
		} else {
			newEntry.expiresAt = existing.expiresAt
		}
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
		newEntry.expiresAt = incomingExpiry
	}

	s.entries[ek] = newEntry
	newValue := newEntry.toValue()
	s.mu.Unlock()

	// Notify watchers synchronously (outside the entry lock) so that the
	// caller sees events delivered before it returns. Ordering with
	// subsequent mutations is preserved because sendToWatcher is non-blocking.
	s.notifyWatchers(config.ChangeEvent{
		Type:      config.ChangeTypeSet,
		Namespace: namespace,
		Key:       key,
		Value:     newValue,
		Timestamp: now,
	})

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
	ek := s.entryKey(namespace, key)
	if _, ok := s.entries[ek]; !ok {
		s.mu.Unlock()
		return &config.KeyNotFoundError{Key: key, Namespace: namespace}
	}
	delete(s.entries, ek)
	s.mu.Unlock()

	s.notifyWatchers(config.ChangeEvent{
		Type:      config.ChangeTypeDelete,
		Namespace: namespace,
		Key:       key,
		Value:     nil,
		Timestamp: time.Now().UTC(),
	})
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

		// Extract TTL from the incoming value. A non-zero expiresAt overrides
		// any existing TTL. A zero value preserves the existing TTL on updates.
		incomingExpiry := config.GetExpiresAt(value)

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
			if !incomingExpiry.IsZero() {
				newEntry.expiresAt = incomingExpiry
			} else {
				newEntry.expiresAt = existing.expiresAt
			}
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
			newEntry.expiresAt = incomingExpiry
		}

		s.entries[ek] = newEntry
		succeeded = append(succeeded, key)

		events = append(events, config.ChangeEvent{
			Type:      config.ChangeTypeSet,
			Namespace: namespace,
			Key:       key,
			Value:     newEntry.toValue(),
			Timestamp: now,
		})
	}
	s.mu.Unlock()

	for _, event := range events {
		s.notifyWatchers(event)
	}

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
	s.mu.Unlock()

	for _, event := range events {
		s.notifyWatchers(event)
	}
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
		expiresAt: e.expiresAt,
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
	s.mu.RLock()

	if _, exists := s.aliases[alias]; exists {
		s.mu.RUnlock()
		s.aliasMu.Unlock()
		return nil, &config.AliasExistsError{Alias: alias, Reason: "alias already registered"}
	}
	for _, e := range s.entries {
		if e.key == alias {
			reason := fmt.Sprintf("key exists in namespace %q", e.namespace)
			s.mu.RUnlock()
			s.aliasMu.Unlock()
			return nil, &config.AliasExistsError{Alias: alias, Reason: reason}
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
	s.mu.RUnlock()
	s.aliasMu.Unlock()

	// Emit the event after releasing locks so watcher callbacks cannot
	// deadlock by calling back into the store; sendToWatcher is non-blocking
	// so delivery order matches the SetAlias call order.
	s.notifyWatchers(config.ChangeEvent{
		Type:      config.ChangeTypeAliasSet,
		Key:       alias,
		Value:     val,
		Timestamp: now,
	})
	return val, nil
}

// DeleteAlias removes an alias.
// Returns ErrNotFound if the alias does not exist.
func (s *Store) DeleteAlias(ctx context.Context, alias string) error {
	if s.closed.Load() {
		return config.ErrStoreClosed
	}

	s.aliasMu.Lock()
	if _, exists := s.aliases[alias]; !exists {
		s.aliasMu.Unlock()
		return &config.KeyNotFoundError{Key: alias}
	}
	delete(s.aliases, alias)
	s.aliasMu.Unlock()

	s.notifyWatchers(config.ChangeEvent{
		Type:      config.ChangeTypeAliasDelete,
		Key:       alias,
		Timestamp: time.Now().UTC(),
	})
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

// namespaceListerBackendTag is the [internal/cursor] backend identifier
// used by this store. It MUST differ from every other backend's tag so a
// caller that switches stores while holding a cursor sees ErrInvalidCursor
// instead of a silently misinterpreted token.
const namespaceListerBackendTag = "memory"

// ListNamespaces returns a page of namespace names currently containing at
// least one non-expired entry. See [config.NamespaceLister] for the full
// contract on ordering, prefix matching, and cursor opacity.
//
// Query strategy: the entries map is snapshot under RLock into a
// deduplicated namespace slice, then sort/filter/paginate happens outside
// the lock so a long page does not stall concurrent writers.
//
// Limit policy: limit <= 0 is treated as a caller error and returns
// [config.ErrInvalidValue]; the store does not silently fall back to a
// default. This matches the "limit > 0" SHOULD in the interface contract
// and surfaces the mistake loudly during development.
//
// Cursor policy: opaque, base64-url-encoded envelopes from
// [internal/cursor] carrying the last-emitted name. Foreign or malformed
// cursors return an error wrapping [config.ErrInvalidCursor]; check with
// [config.IsInvalidCursor].
//
// TTL: matches [Store.Get] and [Store.Find] read semantics — expired
// entries are filtered before the namespace set is built, so a namespace
// whose entries have all expired (but have not yet been Deleted) does not
// appear in the page.
func (s *Store) ListNamespaces(ctx context.Context, prefix string, limit int, cursor string) ([]string, string, error) {
	if s.closed.Load() {
		return nil, "", config.ErrStoreClosed
	}
	if limit <= 0 {
		return nil, "", fmt.Errorf("memory: ListNamespaces requires limit > 0: %w", config.ErrInvalidValue)
	}

	var after string
	if cursor != "" {
		decoded, err := cursorpkg.UnmarshalString(namespaceListerBackendTag, cursor)
		if err != nil {
			return nil, "", err
		}
		after = decoded
	}

	// Snapshot the distinct namespace set under RLock. The lock is held only
	// long enough to copy the keys; sort + filter + slice happen below in
	// uncontended local state.
	now := time.Now().UTC()
	seen := make(map[string]struct{}, 8)
	s.mu.RLock()
	for _, e := range s.entries {
		if e.isExpired(now) {
			continue
		}
		seen[e.namespace] = struct{}{}
	}
	s.mu.RUnlock()

	all := make([]string, 0, len(seen))
	for ns := range seen {
		if prefix != "" && !strings.HasPrefix(ns, prefix) {
			continue
		}
		if after != "" && ns <= after {
			continue
		}
		all = append(all, ns)
	}
	sort.Strings(all)

	if len(all) > limit {
		emitted := all[:limit]
		next, err := cursorpkg.MarshalString(namespaceListerBackendTag, emitted[len(emitted)-1])
		if err != nil {
			return nil, "", err
		}
		return emitted, next, nil
	}
	return all, "", nil
}
