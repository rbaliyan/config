// Package postgres provides a PostgreSQL-backed configuration store with LISTEN/NOTIFY change notifications.
package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lib/pq"

	"github.com/rbaliyan/config"
)

// validIdentifier matches valid PostgreSQL identifiers (alphanumeric and underscore, not starting with digit)
var validIdentifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// Store is a PostgreSQL configuration store implementation.
// Uses LISTEN/NOTIFY for real-time watch notifications.
// The store does not manage the database connection lifecycle - the integrating
// application is responsible for creating and closing the connection.
type Store struct {
	db       *sql.DB
	listener *pq.Listener
	closed   atomic.Bool

	// Watch management
	watchMu       sync.RWMutex
	watchers      map[*watchEntry]struct{}
	stopWatch     chan struct{}
	watchWg       sync.WaitGroup
	droppedEvents atomic.Int64                   // Counter for dropped events due to full channels
	onDropped     func(event config.ChangeEvent) // Optional callback when event is dropped
	logger        *slog.Logger

	cfg Config
}

// Config holds PostgreSQL store configuration.
type Config struct {
	// Table is the table name for config entries.
	Table string

	// NotifyChannel is the PostgreSQL NOTIFY channel name.
	NotifyChannel string

	// WatchBufferSize is the channel buffer size for watch subscriptions.
	WatchBufferSize int

	// ReconnectBackoff is the wait time before reconnecting listener.
	ReconnectBackoff time.Duration
}

// DefaultConfig returns the default configuration.
func DefaultConfig() Config {
	return Config{
		Table:            "config_entries",
		NotifyChannel:    "config_changes",
		WatchBufferSize:  100,
		ReconnectBackoff: 5 * time.Second,
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

// notifyPayload is the JSON structure sent via NOTIFY.
// For insert/update, includes the full entry data for cache invalidation.
// For delete, Value/Codec/Type are omitted.
type notifyPayload struct {
	Operation string `json:"op"` // "insert", "update", "delete"
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	Version   int64  `json:"version"`
	// Entry data (only present for insert/update)
	Value     []byte      `json:"value,omitempty"`      // Base64-encoded by json.Marshal
	Codec     string      `json:"codec,omitempty"`      // Codec name
	Type      config.Type `json:"type,omitempty"`       // Value type
	CreatedAt *time.Time  `json:"created_at,omitempty"` // Creation timestamp
	UpdatedAt *time.Time  `json:"updated_at,omitempty"` // Update timestamp
}

// Option configures the PostgreSQL store.
type Option func(*Store)

// WithConfig sets the store configuration.
func WithConfig(cfg Config) Option {
	return func(s *Store) {
		if cfg.Table != "" {
			s.cfg.Table = cfg.Table
		}
		if cfg.NotifyChannel != "" {
			s.cfg.NotifyChannel = cfg.NotifyChannel
		}
		if cfg.WatchBufferSize > 0 {
			s.cfg.WatchBufferSize = cfg.WatchBufferSize
		}
		if cfg.ReconnectBackoff > 0 {
			s.cfg.ReconnectBackoff = cfg.ReconnectBackoff
		}
	}
}

// WithTable sets the table name.
func WithTable(name string) Option {
	return func(s *Store) {
		s.cfg.Table = name
	}
}

// WithNotifyChannel sets the NOTIFY channel name.
func WithNotifyChannel(name string) Option {
	return func(s *Store) {
		s.cfg.NotifyChannel = name
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

// NewStore creates a new PostgreSQL store with the provided database connection and listener.
// The db must be connected and ready to use.
// The listener is used for LISTEN/NOTIFY functionality - pass nil to disable watch support.
// The store does not manage the connection lifecycle - the caller is responsible
// for closing the db and listener when done.
func NewStore(db *sql.DB, listener *pq.Listener, opts ...Option) *Store {
	s := &Store{
		db:        db,
		listener:  listener,
		cfg:       DefaultConfig(),
		watchers:  make(map[*watchEntry]struct{}),
		stopWatch: make(chan struct{}),
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

// Connect initializes the store (creates schema and starts notification listener).
// The database connection must already be established.
func (s *Store) Connect(ctx context.Context) error {
	if s.db == nil {
		return config.WrapStoreError("connect", "postgres", "", fmt.Errorf("db is nil"))
	}

	// Validate table name and notify channel to prevent SQL injection
	if !validIdentifier.MatchString(s.cfg.Table) {
		return config.WrapStoreError("connect", "postgres", "", fmt.Errorf("invalid table name: %q", s.cfg.Table))
	}
	if !validIdentifier.MatchString(s.cfg.NotifyChannel) {
		return config.WrapStoreError("connect", "postgres", "", fmt.Errorf("invalid notify channel: %q", s.cfg.NotifyChannel))
	}

	// Create schema
	if err := s.createSchema(ctx); err != nil {
		return config.WrapStoreError("create_schema", "postgres", "", err)
	}

	// Start notification listener if listener is provided
	if s.listener != nil {
		if err := s.listener.Listen(s.cfg.NotifyChannel); err != nil {
			return config.WrapStoreError("listen", "postgres", "", err)
		}
		s.watchWg.Add(1)
		go s.listenNotifications()
	}

	return nil
}

func (s *Store) createSchema(ctx context.Context) error {
	schema := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id BIGSERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			namespace TEXT NOT NULL,
			value BYTEA NOT NULL,
			codec TEXT NOT NULL DEFAULT 'json',
			type INTEGER NOT NULL DEFAULT 0,
			version BIGINT NOT NULL DEFAULT 1,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			UNIQUE(namespace, key)
		);

		CREATE INDEX IF NOT EXISTS idx_%s_namespace ON %s(namespace);
		CREATE INDEX IF NOT EXISTS idx_%s_key ON %s(key);
		CREATE INDEX IF NOT EXISTS idx_%s_namespace_key ON %s(namespace, key);
		CREATE INDEX IF NOT EXISTS idx_%s_type ON %s(type);
		CREATE INDEX IF NOT EXISTS idx_%s_updated_at ON %s(updated_at DESC);
		CREATE INDEX IF NOT EXISTS idx_%s_key_prefix ON %s(key text_pattern_ops);
	`, s.cfg.Table,
		s.cfg.Table, s.cfg.Table,
		s.cfg.Table, s.cfg.Table,
		s.cfg.Table, s.cfg.Table,
		s.cfg.Table, s.cfg.Table,
		s.cfg.Table, s.cfg.Table,
		s.cfg.Table, s.cfg.Table)

	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return err
	}

	// Create trigger function for NOTIFY
	// Includes full entry data for insert/update to enable cache updates without refetching.
	// Note: PostgreSQL NOTIFY has 8KB payload limit. Large values may be truncated.
	triggerFunc := fmt.Sprintf(`
		CREATE OR REPLACE FUNCTION notify_%s_changes()
		RETURNS TRIGGER AS $$
		DECLARE
			payload TEXT;
		BEGIN
			IF TG_OP = 'DELETE' THEN
				payload := json_build_object(
					'op', 'delete',
					'namespace', OLD.namespace,
					'key', OLD.key,
					'version', OLD.version
				)::text;
				PERFORM pg_notify('%s', payload);
				RETURN OLD;
			ELSE
				-- Include full entry data for cache invalidation
				payload := json_build_object(
					'op', lower(TG_OP),
					'namespace', NEW.namespace,
					'key', NEW.key,
					'version', NEW.version,
					'value', encode(NEW.value, 'base64'),
					'codec', NEW.codec,
					'type', NEW.type,
					'created_at', NEW.created_at,
					'updated_at', NEW.updated_at
				)::text;
				PERFORM pg_notify('%s', payload);
				RETURN NEW;
			END IF;
		END;
		$$ LANGUAGE plpgsql;
	`, s.cfg.Table, s.cfg.NotifyChannel, s.cfg.NotifyChannel)

	if _, err := s.db.ExecContext(ctx, triggerFunc); err != nil {
		return err
	}

	// Create trigger
	trigger := fmt.Sprintf(`
		DROP TRIGGER IF EXISTS %s_notify ON %s;
		CREATE TRIGGER %s_notify
			AFTER INSERT OR UPDATE OR DELETE ON %s
			FOR EACH ROW EXECUTE FUNCTION notify_%s_changes();
	`, s.cfg.Table, s.cfg.Table, s.cfg.Table, s.cfg.Table, s.cfg.Table)

	_, err := s.db.ExecContext(ctx, trigger)
	return err
}

// Close stops the notification listener and closes all watchers.
// It does NOT close the database connection or listener - that is the caller's responsibility.
func (s *Store) Close(ctx context.Context) error {
	if s.closed.Swap(true) {
		return nil // Already closed
	}

	close(s.stopWatch)
	s.watchWg.Wait()

	// Close all watchers
	s.watchMu.Lock()
	for entry := range s.watchers {
		entry.cancel()
		entry.closeOnce.Do(func() {
			entry.mu.Lock()
			entry.closed = true
			close(entry.ch)
			entry.mu.Unlock()
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

	query := fmt.Sprintf(`
		SELECT id, key, namespace, value, codec, type, version, created_at, updated_at
		FROM %s WHERE namespace = $1 AND key = $2
	`, s.cfg.Table)

	var (
		id                   int64
		k, ns, codecName     string
		value                []byte
		valueType            config.Type
		version              int64
		createdAt, updatedAt time.Time
	)

	err := s.db.QueryRowContext(ctx, query, namespace, key).Scan(
		&id, &k, &ns, &value, &codecName, &valueType, &version, &createdAt, &updatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, &config.KeyNotFoundError{Key: key, Namespace: namespace}
	}
	if err != nil {
		return nil, config.WrapStoreError("get", "postgres", key, err)
	}

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

	// Marshal the value
	data, err := value.Marshal()
	if err != nil {
		return nil, config.WrapStoreError("marshal", "postgres", key, err)
	}

	var query string
	var result sql.Result

	writeMode := config.GetWriteMode(value)
	switch writeMode {
	case config.WriteModeCreate:
		// Insert only if not exists - use ON CONFLICT DO NOTHING
		query = fmt.Sprintf(`
			INSERT INTO %s (key, namespace, value, codec, type, version, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, 1, NOW(), NOW())
			ON CONFLICT (namespace, key) DO NOTHING
		`, s.cfg.Table)

		result, err = s.db.ExecContext(ctx, query,
			key,
			namespace,
			data,
			value.Codec(),
			value.Type(),
		)
		if err != nil {
			return nil, config.WrapStoreError("set", "postgres", key, err)
		}

		// Check if insert happened
		rowsAffected, _ := result.RowsAffected()
		if rowsAffected == 0 {
			return nil, &config.KeyExistsError{Key: key, Namespace: namespace}
		}

	case config.WriteModeUpdate:
		// Update only if exists
		query = fmt.Sprintf(`
			UPDATE %s SET
				value = $3,
				codec = $4,
				type = $5,
				version = version + 1,
				updated_at = NOW()
			WHERE namespace = $2 AND key = $1
		`, s.cfg.Table)

		result, err = s.db.ExecContext(ctx, query,
			key,
			namespace,
			data,
			value.Codec(),
			value.Type(),
		)
		if err != nil {
			return nil, config.WrapStoreError("set", "postgres", key, err)
		}

		// Check if update happened
		rowsAffected, _ := result.RowsAffected()
		if rowsAffected == 0 {
			return nil, &config.KeyNotFoundError{Key: key, Namespace: namespace}
		}

	default:
		// Upsert with version increment using unique constraint on (namespace, key)
		query = fmt.Sprintf(`
			INSERT INTO %s (key, namespace, value, codec, type, version, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, 1, NOW(), NOW())
			ON CONFLICT (namespace, key) DO UPDATE SET
				value = EXCLUDED.value,
				codec = EXCLUDED.codec,
				type = EXCLUDED.type,
				version = %s.version + 1,
				updated_at = NOW()
		`, s.cfg.Table, s.cfg.Table)

		_, err = s.db.ExecContext(ctx, query,
			key,
			namespace,
			data,
			value.Codec(),
			value.Type(),
		)
		if err != nil {
			return nil, config.WrapStoreError("set", "postgres", key, err)
		}
	}

	// Fetch the stored value with updated metadata
	return s.Get(ctx, namespace, key)
}

// Delete removes a configuration value by namespace and key.
func (s *Store) Delete(ctx context.Context, namespace, key string) error {
	if s.closed.Load() {
		return config.ErrStoreClosed
	}
	if err := config.ValidateNamespace(namespace); err != nil {
		return err
	}

	query := fmt.Sprintf(`DELETE FROM %s WHERE namespace = $1 AND key = $2`, s.cfg.Table)
	result, err := s.db.ExecContext(ctx, query, namespace, key)
	if err != nil {
		return config.WrapStoreError("delete", "postgres", key, err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return config.WrapStoreError("delete", "postgres", key, err)
	}
	if rows == 0 {
		return &config.KeyNotFoundError{Key: key, Namespace: namespace}
	}

	return nil
}

// Find returns a page of keys and values matching the filter within a namespace.
// Uses BIGSERIAL id-based pagination for consistent ordering.
func (s *Store) Find(ctx context.Context, namespace string, filter config.Filter) (config.Page, error) {
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
	}
	if err := config.ValidateNamespace(namespace); err != nil {
		return nil, err
	}

	args := []any{}
	argNum := 1
	limit := filter.Limit()

	// Keys mode: get specific keys (no pagination)
	if keys := filter.Keys(); len(keys) > 0 {
		placeholders := make([]string, len(keys))
		for i, key := range keys {
			placeholders[i] = fmt.Sprintf("$%d", argNum)
			args = append(args, key)
			argNum++
		}
		query := fmt.Sprintf(`
			SELECT id, key, namespace, value, codec, type, version, created_at, updated_at
			FROM %s WHERE namespace = $%d AND key IN (%s)
		`, s.cfg.Table, argNum, strings.Join(placeholders, ","))
		args = append(args, namespace)

		query += " ORDER BY id"

		results, nextCursor, err := s.executeListQuery(ctx, query, args)
		if err != nil {
			return nil, err
		}
		return config.NewPage(results, nextCursor, 0), nil
	}

	// Prefix mode: get all keys matching prefix
	query := fmt.Sprintf(`
		SELECT id, key, namespace, value, codec, type, version, created_at, updated_at
		FROM %s WHERE namespace = $%d
	`, s.cfg.Table, argNum)
	args = append(args, namespace)
	argNum++

	if prefix := filter.Prefix(); prefix != "" {
		query += fmt.Sprintf(" AND key LIKE $%d", argNum)
		args = append(args, prefix+"%")
		argNum++
	}

	// Cursor-based pagination using BIGSERIAL id
	if cursor := filter.Cursor(); cursor != "" {
		query += fmt.Sprintf(" AND id > $%d", argNum)
		args = append(args, cursor)
		argNum++
	}

	query += " ORDER BY id"

	// Apply limit
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT $%d", argNum)
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
		return nil, "", config.WrapStoreError("list", "postgres", "", err)
	}
	defer rows.Close()

	results := make(map[string]config.Value)
	var lastID int64
	for rows.Next() {
		var (
			id                   int64
			k, ns, codecName     string
			value                []byte
			valueType            config.Type
			version              int64
			createdAt, updatedAt time.Time
		)

		if err := rows.Scan(
			&id, &k, &ns, &value, &codecName, &valueType, &version, &createdAt, &updatedAt,
		); err != nil {
			return nil, "", config.WrapStoreError("list", "postgres", "", err)
		}

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
		return nil, "", config.WrapStoreError("list", "postgres", "", err)
	}

	// Return the last ID as the cursor for next page
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
		case <-s.stopWatch:
		}

		s.watchMu.Lock()
		delete(s.watchers, entry)
		s.watchMu.Unlock()

		// Close channel safely with mutex to prevent race with dispatchEvent
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
		return nil, config.WrapStoreError("stats", "postgres", "", err)
	}

	// Count by type
	typeQuery := fmt.Sprintf(`SELECT type, COUNT(*) FROM %s GROUP BY type`, s.cfg.Table)
	typeRows, err := s.db.QueryContext(ctx, typeQuery)
	if err == nil {
		defer typeRows.Close()
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
		defer nsRows.Close()
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

	// Build query with placeholders
	placeholders := make([]string, len(keys))
	args := make([]any, len(keys)+1)
	args[0] = namespace
	for i, key := range keys {
		placeholders[i] = fmt.Sprintf("$%d", i+2)
		args[i+1] = key
	}

	query := fmt.Sprintf(`
		SELECT id, key, namespace, value, codec, type, version, created_at, updated_at
		FROM %s WHERE namespace = $1 AND key IN (%s)
	`, s.cfg.Table, strings.Join(placeholders, ","))

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, config.WrapStoreError("get_many", "postgres", "", err)
	}
	defer rows.Close()

	results := make(map[string]config.Value, len(keys))
	for rows.Next() {
		var (
			id                   int64
			k, ns, codecName     string
			value                []byte
			valueType            config.Type
			version              int64
			createdAt, updatedAt time.Time
		)

		if err := rows.Scan(&id, &k, &ns, &value, &codecName, &valueType, &version, &createdAt, &updatedAt); err != nil {
			return nil, config.WrapStoreError("get_many", "postgres", "", err)
		}

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
		return nil, config.WrapStoreError("get_many", "postgres", "", err)
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

	// Individual upserts — partial success is acceptable, so we don't use a
	// transaction. Each key is written independently; failures on one key
	// do not prevent writes to other keys.
	query := fmt.Sprintf(`
		INSERT INTO %s (key, namespace, value, codec, type, version, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, 1, NOW(), NOW())
		ON CONFLICT (namespace, key) DO UPDATE SET
			value = EXCLUDED.value,
			codec = EXCLUDED.codec,
			type = EXCLUDED.type,
			version = %s.version + 1,
			updated_at = NOW()
	`, s.cfg.Table, s.cfg.Table)

	for key, value := range values {
		if key == "" {
			keyErrors[key] = &config.InvalidKeyError{Key: key, Reason: "key cannot be empty"}
			continue
		}

		data, err := value.Marshal()
		if err != nil {
			keyErrors[key] = config.WrapStoreError("marshal", "postgres", key, err)
			continue
		}

		_, err = s.db.ExecContext(ctx, query, key, namespace, data, value.Codec(), value.Type())
		if err != nil {
			keyErrors[key] = config.WrapStoreError("set_many", "postgres", key, err)
		} else {
			succeeded = append(succeeded, key)
		}
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

	// Build query with placeholders
	placeholders := make([]string, len(keys))
	args := make([]any, len(keys)+1)
	args[0] = namespace
	for i, key := range keys {
		placeholders[i] = fmt.Sprintf("$%d", i+2)
		args[i+1] = key
	}

	query := fmt.Sprintf(`DELETE FROM %s WHERE namespace = $1 AND key IN (%s)`,
		s.cfg.Table, strings.Join(placeholders, ","))

	result, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, config.WrapStoreError("delete_many", "postgres", "", err)
	}

	deleted, err := result.RowsAffected()
	if err != nil {
		return 0, config.WrapStoreError("delete_many", "postgres", "", err)
	}

	return deleted, nil
}

// listenNotifications processes LISTEN/NOTIFY events.
func (s *Store) listenNotifications() {
	defer s.watchWg.Done()

	for {
		select {
		case <-s.stopWatch:
			return
		case notification := <-s.listener.Notify:
			if notification == nil {
				// Reconnect event - pq.Listener sends nil on reconnection
				s.log().Info("postgres: listener reconnected to database")
				continue
			}

			var payload notifyPayload
			if err := json.Unmarshal([]byte(notification.Extra), &payload); err != nil {
				s.log().Warn("failed to decode notification payload", "error", err)
				continue
			}

			var eventType config.ChangeType
			switch payload.Operation {
			case "insert":
				eventType = config.ChangeTypeSet
			case "update":
				eventType = config.ChangeTypeSet
			case "delete":
				eventType = config.ChangeTypeDelete
			default:
				continue
			}

			event := config.ChangeEvent{
				Type:      eventType,
				Namespace: payload.Namespace,
				Key:       payload.Key,
				Timestamp: time.Now().UTC(),
			}

			// For insert/update, create Value from the payload data
			// Note: json.Unmarshal automatically base64-decodes []byte fields
			if eventType == config.ChangeTypeSet && len(payload.Value) > 0 {
				var createdAt, updatedAt time.Time
				if payload.CreatedAt != nil {
					createdAt = *payload.CreatedAt
				}
				if payload.UpdatedAt != nil {
					updatedAt = *payload.UpdatedAt
				}
				val, err := config.NewValueFromBytes(
					payload.Value,
					payload.Codec,
					config.WithValueType(payload.Type),
					config.WithValueMetadata(payload.Version, createdAt, updatedAt),
				)
				if err == nil {
					event.Value = val
				} else {
					s.log().Warn("failed to create value from notification", "error", err)
				}
			}

			s.dispatchEvent(event)
		}
	}
}

func (s *Store) dispatchEvent(event config.ChangeEvent) {
	s.watchMu.RLock()
	entries := make([]*watchEntry, 0, len(s.watchers))
	for entry := range s.watchers {
		entries = append(entries, entry)
	}
	s.watchMu.RUnlock()

	for _, entry := range entries {
		if s.matchesFilter(event, entry.filter) {
			s.sendToWatcher(entry, event)
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

// log returns the configured logger or the default logger.
func (s *Store) log() *slog.Logger {
	if s.logger != nil {
		return s.logger
	}
	return slog.Default()
}

func (s *Store) matchesFilter(event config.ChangeEvent, filter config.WatchFilter) bool {
	// Check namespace filter
	if len(filter.Namespaces) > 0 && !slices.Contains(filter.Namespaces, event.Namespace) {
		return false
	}

	// Check prefix filter
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
