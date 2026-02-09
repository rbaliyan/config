// Package sqlite provides a SQLite-backed configuration store with application-level change notifications.
package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rbaliyan/config"
)

// validIdentifier matches valid SQLite identifiers (alphanumeric and underscore, not starting with digit).
var validIdentifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// sqliteTimestampFormat is the format used by SQLite's datetime() function.
const sqliteTimestampFormat = "2006-01-02 15:04:05"

// Store is a SQLite configuration store implementation.
// Uses application-level notifications for watch support (SQLite has no LISTEN/NOTIFY).
// The store does not manage the database connection lifecycle - the integrating
// application is responsible for creating and closing the connection.
type Store struct {
	db     *sql.DB
	closed atomic.Bool

	// Watch management (same pattern as memory store)
	watchMu       sync.RWMutex
	watchers      map[*watchEntry]struct{}
	stopChan      chan struct{}
	droppedEvents atomic.Int64
	onDropped     func(event config.ChangeEvent)
	logger        *slog.Logger

	cfg Config
}

// Config holds SQLite store configuration.
type Config struct {
	// Table is the table name for config entries.
	Table string

	// WatchBufferSize is the channel buffer size for watch subscriptions.
	WatchBufferSize int

	// EnableWAL enables WAL journal mode for better concurrent read performance.
	EnableWAL bool
}

// DefaultConfig returns the default configuration.
func DefaultConfig() Config {
	return Config{
		Table:           "config_entries",
		WatchBufferSize: 100,
		EnableWAL:       true,
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

// Option configures the SQLite store.
type Option func(*Store)

// WithConfig sets the store configuration.
func WithConfig(cfg Config) Option {
	return func(s *Store) {
		if cfg.Table != "" {
			s.cfg.Table = cfg.Table
		}
		if cfg.WatchBufferSize > 0 {
			s.cfg.WatchBufferSize = cfg.WatchBufferSize
		}
		s.cfg.EnableWAL = cfg.EnableWAL
	}
}

// WithTable sets the table name.
func WithTable(name string) Option {
	return func(s *Store) {
		s.cfg.Table = name
	}
}

// WithWatchBufferSize sets the buffer size for watch channels.
func WithWatchBufferSize(size int) Option {
	return func(s *Store) {
		if size > 0 {
			s.cfg.WatchBufferSize = size
		}
	}
}

// WithWAL enables or disables WAL journal mode.
func WithWAL(enable bool) Option {
	return func(s *Store) {
		s.cfg.EnableWAL = enable
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

// WithLogger sets the logger for the store.
func WithLogger(logger *slog.Logger) Option {
	return func(s *Store) {
		s.logger = logger
	}
}

// NewStore creates a new SQLite store with the provided database connection.
// The db must be connected and ready to use.
// The store does not manage the connection lifecycle - the caller is responsible
// for closing the db when done.
func NewStore(db *sql.DB, opts ...Option) *Store {
	s := &Store{
		db:       db,
		cfg:      DefaultConfig(),
		watchers: make(map[*watchEntry]struct{}),
		stopChan: make(chan struct{}),
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

// Connect initializes the store (enables WAL mode and creates schema).
// The database connection must already be established.
func (s *Store) Connect(ctx context.Context) error {
	if s.db == nil {
		return config.WrapStoreError("connect", "sqlite", "", fmt.Errorf("db is nil"))
	}

	// Validate table name to prevent SQL injection
	if !validIdentifier.MatchString(s.cfg.Table) {
		return config.WrapStoreError("connect", "sqlite", "", fmt.Errorf("invalid table name: %q", s.cfg.Table))
	}

	// Enable WAL mode for better concurrent read performance
	if s.cfg.EnableWAL {
		if _, err := s.db.ExecContext(ctx, "PRAGMA journal_mode=WAL"); err != nil {
			return config.WrapStoreError("connect", "sqlite", "", err)
		}
	}

	// Create schema
	if err := s.createSchema(ctx); err != nil {
		return config.WrapStoreError("create_schema", "sqlite", "", err)
	}

	return nil
}

func (s *Store) createSchema(ctx context.Context) error {
	schema := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			key TEXT NOT NULL,
			namespace TEXT NOT NULL,
			value BLOB NOT NULL,
			codec TEXT NOT NULL DEFAULT 'json',
			type INTEGER NOT NULL DEFAULT 0,
			version INTEGER NOT NULL DEFAULT 1,
			created_at TEXT NOT NULL DEFAULT (datetime('now')),
			updated_at TEXT NOT NULL DEFAULT (datetime('now')),
			UNIQUE(namespace, key)
		)
	`, s.cfg.Table)

	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return err
	}

	indexes := []string{
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_namespace ON %s(namespace)`, s.cfg.Table, s.cfg.Table),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_key ON %s(key)`, s.cfg.Table, s.cfg.Table),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_namespace_key ON %s(namespace, key)`, s.cfg.Table, s.cfg.Table),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_type ON %s(type)`, s.cfg.Table, s.cfg.Table),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_updated_at ON %s(updated_at DESC)`, s.cfg.Table, s.cfg.Table),
	}

	for _, idx := range indexes {
		if _, err := s.db.ExecContext(ctx, idx); err != nil {
			return err
		}
	}

	return nil
}

// Close stops all watchers and releases resources.
// It does NOT close the database connection - that is the caller's responsibility.
func (s *Store) Close(ctx context.Context) error {
	if s.closed.Swap(true) {
		return nil // Already closed
	}

	close(s.stopChan)

	// Copy watchers and clear map while holding lock, then close channels outside lock
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

	query := fmt.Sprintf(`
		SELECT id, key, namespace, value, codec, type, version, created_at, updated_at
		FROM %s WHERE namespace = ? AND key = ?
	`, s.cfg.Table)

	var (
		id                         int64
		k, ns, codecName           string
		value                      []byte
		valueType                  config.Type
		version                    int64
		createdAtStr, updatedAtStr string
	)

	err := s.db.QueryRowContext(ctx, query, namespace, key).Scan(
		&id, &k, &ns, &value, &codecName, &valueType, &version, &createdAtStr, &updatedAtStr,
	)

	if err == sql.ErrNoRows {
		return nil, &config.KeyNotFoundError{Key: key, Namespace: namespace}
	}
	if err != nil {
		return nil, config.WrapStoreError("get", "sqlite", key, err)
	}

	createdAt, _ := time.Parse(sqliteTimestampFormat, createdAtStr)
	updatedAt, _ := time.Parse(sqliteTimestampFormat, updatedAtStr)

	return config.NewValueFromBytes(
		value,
		codecName,
		config.WithValueType(valueType),
		config.WithValueMetadata(version, createdAt, updatedAt),
		config.WithValueEntryID(fmt.Sprintf("%d", id)),
	)
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

	data, err := value.Marshal()
	if err != nil {
		return nil, config.WrapStoreError("marshal", "sqlite", key, err)
	}

	var query string
	var result sql.Result

	writeMode := config.GetWriteMode(value)
	switch writeMode {
	case config.WriteModeCreate:
		query = fmt.Sprintf(`
			INSERT OR IGNORE INTO %s (key, namespace, value, codec, type, version, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, 1, datetime('now'), datetime('now'))
		`, s.cfg.Table)

		result, err = s.db.ExecContext(ctx, query, key, namespace, data, value.Codec(), value.Type())
		if err != nil {
			return nil, config.WrapStoreError("set", "sqlite", key, err)
		}

		rowsAffected, _ := result.RowsAffected()
		if rowsAffected == 0 {
			return nil, &config.KeyExistsError{Key: key, Namespace: namespace}
		}

	case config.WriteModeUpdate:
		query = fmt.Sprintf(`
			UPDATE %s SET
				value = ?,
				codec = ?,
				type = ?,
				version = version + 1,
				updated_at = datetime('now')
			WHERE namespace = ? AND key = ?
		`, s.cfg.Table)

		result, err = s.db.ExecContext(ctx, query, data, value.Codec(), value.Type(), namespace, key)
		if err != nil {
			return nil, config.WrapStoreError("set", "sqlite", key, err)
		}

		rowsAffected, _ := result.RowsAffected()
		if rowsAffected == 0 {
			return nil, &config.KeyNotFoundError{Key: key, Namespace: namespace}
		}

	default:
		query = fmt.Sprintf(`
			INSERT INTO %s (key, namespace, value, codec, type, version, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, 1, datetime('now'), datetime('now'))
			ON CONFLICT (namespace, key) DO UPDATE SET
				value = excluded.value,
				codec = excluded.codec,
				type = excluded.type,
				version = %s.version + 1,
				updated_at = datetime('now')
		`, s.cfg.Table, s.cfg.Table)

		_, err = s.db.ExecContext(ctx, query, key, namespace, data, value.Codec(), value.Type())
		if err != nil {
			return nil, config.WrapStoreError("set", "sqlite", key, err)
		}
	}

	// Fetch the stored value with updated metadata
	stored, err := s.Get(ctx, namespace, key)
	if err != nil {
		return nil, err
	}

	// Notify watchers
	event := config.ChangeEvent{
		Type:      config.ChangeTypeSet,
		Namespace: namespace,
		Key:       key,
		Value:     stored,
		Timestamp: time.Now().UTC(),
	}
	go s.notifyWatchers(event)

	return stored, nil
}

// Delete removes a configuration value by namespace and key.
func (s *Store) Delete(ctx context.Context, namespace, key string) error {
	if s.closed.Load() {
		return config.ErrStoreClosed
	}
	if err := config.ValidateNamespace(namespace); err != nil {
		return err
	}

	query := fmt.Sprintf(`DELETE FROM %s WHERE namespace = ? AND key = ?`, s.cfg.Table)
	result, err := s.db.ExecContext(ctx, query, namespace, key)
	if err != nil {
		return config.WrapStoreError("delete", "sqlite", key, err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return config.WrapStoreError("delete", "sqlite", key, err)
	}
	if rows == 0 {
		return &config.KeyNotFoundError{Key: key, Namespace: namespace}
	}

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
// Uses INTEGER id-based pagination for consistent ordering.
func (s *Store) Find(ctx context.Context, namespace string, filter config.Filter) (config.Page, error) {
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
	}
	if err := config.ValidateNamespace(namespace); err != nil {
		return nil, err
	}

	limit := filter.Limit()

	// Keys mode: get specific keys (no pagination)
	if keys := filter.Keys(); len(keys) > 0 {
		placeholders := make([]string, len(keys))
		args := make([]any, len(keys)+1)
		for i, key := range keys {
			placeholders[i] = "?"
			args[i] = key
		}
		args[len(keys)] = namespace

		query := fmt.Sprintf(`
			SELECT id, key, namespace, value, codec, type, version, created_at, updated_at
			FROM %s WHERE namespace = ? AND key IN (%s)
			ORDER BY id
		`, s.cfg.Table, strings.Join(placeholders, ","))

		// Reorder args: namespace first, then keys
		reorderedArgs := make([]any, 0, len(args))
		reorderedArgs = append(reorderedArgs, namespace)
		for _, key := range keys {
			reorderedArgs = append(reorderedArgs, key)
		}

		results, nextCursor, err := s.executeListQuery(ctx, query, reorderedArgs)
		if err != nil {
			return nil, err
		}
		return config.NewPage(results, nextCursor, 0), nil
	}

	// Prefix mode: get all keys matching prefix
	args := []any{namespace}
	query := fmt.Sprintf(`
		SELECT id, key, namespace, value, codec, type, version, created_at, updated_at
		FROM %s WHERE namespace = ?
	`, s.cfg.Table)

	if prefix := filter.Prefix(); prefix != "" {
		query += " AND key LIKE ?"
		args = append(args, prefix+"%")
	}

	// Cursor-based pagination using id
	if cursor := filter.Cursor(); cursor != "" {
		query += " AND id > ?"
		args = append(args, cursor)
	}

	query += " ORDER BY id"

	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}

	results, nextCursor, err := s.executeListQuery(ctx, query, args)
	if err != nil {
		return nil, err
	}

	return config.NewPage(results, nextCursor, limit), nil
}

func (s *Store) executeListQuery(ctx context.Context, query string, args []any) (map[string]config.Value, string, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", config.WrapStoreError("list", "sqlite", "", err)
	}
	defer func() { _ = rows.Close() }()

	results := make(map[string]config.Value)
	var lastID int64
	for rows.Next() {
		var (
			id                         int64
			k, ns, codecName           string
			value                      []byte
			valueType                  config.Type
			version                    int64
			createdAtStr, updatedAtStr string
		)

		if err := rows.Scan(
			&id, &k, &ns, &value, &codecName, &valueType, &version, &createdAtStr, &updatedAtStr,
		); err != nil {
			return nil, "", config.WrapStoreError("list", "sqlite", "", err)
		}

		createdAt, _ := time.Parse(sqliteTimestampFormat, createdAtStr)
		updatedAt, _ := time.Parse(sqliteTimestampFormat, updatedAtStr)

		val, err := config.NewValueFromBytes(
			value,
			codecName,
			config.WithValueType(valueType),
			config.WithValueMetadata(version, createdAt, updatedAt),
			config.WithValueEntryID(fmt.Sprintf("%d", id)),
		)
		if err != nil {
			s.log().Warn("skipping corrupt entry in list query",
				"key", k, "namespace", ns, "error", err)
			lastID = id
			continue
		}

		results[k] = val
		lastID = id
	}

	if err := rows.Err(); err != nil {
		return nil, "", config.WrapStoreError("list", "sqlite", "", err)
	}

	nextCursor := ""
	if lastID > 0 {
		nextCursor = fmt.Sprintf("%d", lastID)
	}

	return results, nextCursor, nil
}

// Watch returns a channel that receives change events.
func (s *Store) Watch(ctx context.Context, filter config.WatchFilter) (<-chan config.ChangeEvent, error) {
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
	}

	ctx, cancel := context.WithCancel(ctx)
	ch := make(chan config.ChangeEvent, s.cfg.WatchBufferSize)

	entry := &watchEntry{
		filter: filter,
		ch:     ch,
		ctx:    ctx,
		cancel: cancel,
	}

	s.watchMu.Lock()
	s.watchers[entry] = struct{}{}
	s.watchMu.Unlock()

	// Cleanup when context is cancelled
	go func() {
		select {
		case <-ctx.Done():
		case <-s.stopChan:
		}

		s.watchMu.Lock()
		delete(s.watchers, entry)
		s.watchMu.Unlock()

		entry.closeOnce.Do(func() {
			entry.mu.Lock()
			entry.closed = true
			close(ch)
			entry.mu.Unlock()
		})
	}()

	return ch, nil
}

// Health performs a health check.
func (s *Store) Health(ctx context.Context) error {
	if s.closed.Load() {
		return config.ErrStoreClosed
	}
	return s.db.PingContext(ctx)
}

// Stats returns store statistics.
func (s *Store) Stats(ctx context.Context) (*config.StoreStats, error) {
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
	}

	stats := &config.StoreStats{
		EntriesByType:      make(map[config.Type]int64),
		EntriesByNamespace: make(map[string]int64),
	}

	// Count total entries
	countQuery := fmt.Sprintf(`SELECT COUNT(*) FROM %s`, s.cfg.Table)
	if err := s.db.QueryRowContext(ctx, countQuery).Scan(&stats.TotalEntries); err != nil {
		return nil, config.WrapStoreError("stats", "sqlite", "", err)
	}

	// Count by type
	typeQuery := fmt.Sprintf(`SELECT type, COUNT(*) FROM %s GROUP BY type`, s.cfg.Table)
	typeRows, err := s.db.QueryContext(ctx, typeQuery)
	if err == nil {
		defer func() { _ = typeRows.Close() }()
		for typeRows.Next() {
			var t config.Type
			var count int64
			if err := typeRows.Scan(&t, &count); err == nil {
				stats.EntriesByType[t] = count
			}
		}
	}

	// Count by namespace
	nsQuery := fmt.Sprintf(`SELECT namespace, COUNT(*) FROM %s GROUP BY namespace`, s.cfg.Table)
	nsRows, err := s.db.QueryContext(ctx, nsQuery)
	if err == nil {
		defer func() { _ = nsRows.Close() }()
		for nsRows.Next() {
			var ns string
			var count int64
			if err := nsRows.Scan(&ns, &count); err == nil {
				stats.EntriesByNamespace[ns] = count
			}
		}
	}

	return stats, nil
}

// GetMany retrieves multiple values in a single operation.
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

	placeholders := make([]string, len(keys))
	args := make([]any, len(keys)+1)
	args[0] = namespace
	for i, key := range keys {
		placeholders[i] = "?"
		args[i+1] = key
	}

	query := fmt.Sprintf(`
		SELECT id, key, namespace, value, codec, type, version, created_at, updated_at
		FROM %s WHERE namespace = ? AND key IN (%s)
	`, s.cfg.Table, strings.Join(placeholders, ","))

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, config.WrapStoreError("get_many", "sqlite", "", err)
	}
	defer func() { _ = rows.Close() }()

	results := make(map[string]config.Value, len(keys))
	for rows.Next() {
		var (
			id                         int64
			k, ns, codecName           string
			value                      []byte
			valueType                  config.Type
			version                    int64
			createdAtStr, updatedAtStr string
		)

		if err := rows.Scan(&id, &k, &ns, &value, &codecName, &valueType, &version, &createdAtStr, &updatedAtStr); err != nil {
			return nil, config.WrapStoreError("get_many", "sqlite", "", err)
		}

		createdAt, _ := time.Parse(sqliteTimestampFormat, createdAtStr)
		updatedAt, _ := time.Parse(sqliteTimestampFormat, updatedAtStr)

		val, err := config.NewValueFromBytes(
			value,
			codecName,
			config.WithValueType(valueType),
			config.WithValueMetadata(version, createdAt, updatedAt),
			config.WithValueEntryID(fmt.Sprintf("%d", id)),
		)
		if err != nil {
			s.log().Warn("skipping corrupt entry in get_many",
				"key", k, "namespace", ns, "error", err)
			continue
		}
		results[k] = val
	}

	if err := rows.Err(); err != nil {
		return nil, config.WrapStoreError("get_many", "sqlite", "", err)
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
	if len(values) == 0 {
		return nil
	}

	keyErrors := make(map[string]error)
	succeeded := make([]string, 0, len(values))
	events := make([]config.ChangeEvent, 0, len(values))

	query := fmt.Sprintf(`
		INSERT INTO %s (key, namespace, value, codec, type, version, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, 1, datetime('now'), datetime('now'))
		ON CONFLICT (namespace, key) DO UPDATE SET
			value = excluded.value,
			codec = excluded.codec,
			type = excluded.type,
			version = %s.version + 1,
			updated_at = datetime('now')
	`, s.cfg.Table, s.cfg.Table)

	for key, value := range values {
		if key == "" {
			keyErrors[key] = &config.InvalidKeyError{Key: key, Reason: "key cannot be empty"}
			continue
		}

		data, err := value.Marshal()
		if err != nil {
			keyErrors[key] = config.WrapStoreError("marshal", "sqlite", key, err)
			continue
		}

		_, err = s.db.ExecContext(ctx, query, key, namespace, data, value.Codec(), value.Type())
		if err != nil {
			keyErrors[key] = config.WrapStoreError("set_many", "sqlite", key, err)
		} else {
			succeeded = append(succeeded, key)
			events = append(events, config.ChangeEvent{
				Type:      config.ChangeTypeSet,
				Namespace: namespace,
				Key:       key,
				Timestamp: time.Now().UTC(),
			})
		}
	}

	// Notify watchers for all successful changes
	if len(events) > 0 {
		go func() {
			for _, event := range events {
				s.notifyWatchers(event)
			}
		}()
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
// Returns the number of entries actually deleted.
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

	placeholders := make([]string, len(keys))
	args := make([]any, len(keys)+1)
	args[0] = namespace
	for i, key := range keys {
		placeholders[i] = "?"
		args[i+1] = key
	}

	query := fmt.Sprintf(`DELETE FROM %s WHERE namespace = ? AND key IN (%s)`,
		s.cfg.Table, strings.Join(placeholders, ","))

	result, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, config.WrapStoreError("delete_many", "sqlite", "", err)
	}

	deleted, err := result.RowsAffected()
	if err != nil {
		return 0, config.WrapStoreError("delete_many", "sqlite", "", err)
	}

	// Notify watchers for deleted keys
	if deleted > 0 {
		now := time.Now().UTC()
		events := make([]config.ChangeEvent, 0, len(keys))
		for _, key := range keys {
			events = append(events, config.ChangeEvent{
				Type:      config.ChangeTypeDelete,
				Namespace: namespace,
				Key:       key,
				Timestamp: now,
			})
		}
		go func() {
			for _, event := range events {
				s.notifyWatchers(event)
			}
		}()
	}

	return deleted, nil
}

// notifyWatchers sends an event to all matching watchers.
func (s *Store) notifyWatchers(event config.ChangeEvent) {
	if s.closed.Load() {
		return
	}

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

// log returns the configured logger or the default logger.
func (s *Store) log() *slog.Logger {
	if s.logger != nil {
		return s.logger
	}
	return slog.Default()
}

func (s *Store) matchesFilter(event config.ChangeEvent, filter config.WatchFilter) bool {
	if len(filter.Namespaces) > 0 && !slices.Contains(filter.Namespaces, event.Namespace) {
		return false
	}

	if len(filter.Prefixes) > 0 {
		found := false
		for _, prefix := range filter.Prefixes {
			if strings.HasPrefix(event.Key, prefix) {
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
