package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
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
	watchMu   sync.RWMutex
	watchers  map[*watchEntry]struct{}
	stopWatch chan struct{}
	watchWg   sync.WaitGroup

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

	// NotificationFetchTimeout is the timeout for fetching values during notification processing.
	NotificationFetchTimeout time.Duration
}

// DefaultConfig returns the default configuration.
func DefaultConfig() Config {
	return Config{
		Table:                    "config_entries",
		NotifyChannel:            "config_changes",
		WatchBufferSize:          100,
		ReconnectBackoff:         5 * time.Second,
		NotificationFetchTimeout: 5 * time.Second,
	}
}

type watchEntry struct {
	filter    config.WatchFilter
	ch        chan config.ChangeEvent
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
}

// notifyPayload is the JSON structure sent via NOTIFY.
type notifyPayload struct {
	Operation string `json:"op"` // "insert", "update", "delete"
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	Tags      string `json:"tags"` // Sorted "key1=value1,key2=value2"
	Version   int64  `json:"version"`
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
		if cfg.NotificationFetchTimeout > 0 {
			s.cfg.NotificationFetchTimeout = cfg.NotificationFetchTimeout
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
			tags TEXT NOT NULL DEFAULT '',
			value BYTEA NOT NULL,
			codec TEXT NOT NULL DEFAULT 'json',
			type INTEGER NOT NULL DEFAULT 0,
			version BIGINT NOT NULL DEFAULT 1,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			UNIQUE(namespace, key, tags)
		);

		CREATE INDEX IF NOT EXISTS idx_%s_namespace ON %s(namespace);
		CREATE INDEX IF NOT EXISTS idx_%s_key ON %s(key);
		CREATE INDEX IF NOT EXISTS idx_%s_namespace_key ON %s(namespace, key);
		CREATE INDEX IF NOT EXISTS idx_%s_tags ON %s(tags);
		CREATE INDEX IF NOT EXISTS idx_%s_type ON %s(type);
		CREATE INDEX IF NOT EXISTS idx_%s_updated_at ON %s(updated_at DESC);
		CREATE INDEX IF NOT EXISTS idx_%s_key_prefix ON %s(key text_pattern_ops);
	`, s.cfg.Table,
		s.cfg.Table, s.cfg.Table,
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
					'tags', OLD.tags,
					'version', OLD.version
				)::text;
				PERFORM pg_notify('%s', payload);
				RETURN OLD;
			ELSE
				payload := json_build_object(
					'op', lower(TG_OP),
					'namespace', NEW.namespace,
					'key', NEW.key,
					'tags', NEW.tags,
					'version', NEW.version
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
			close(entry.ch)
		})
	}
	s.watchers = make(map[*watchEntry]struct{})
	s.watchMu.Unlock()

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

	tagsStr := config.FormatTags(tags)
	query := fmt.Sprintf(`
		SELECT key, namespace, tags, value, codec, type, version, created_at, updated_at
		FROM %s WHERE namespace = $1 AND key = $2 AND tags = $3
	`, s.cfg.Table)

	var (
		k, ns, tagsResult, codecName string
		value                        []byte
		valueType                    config.Type
		version                      int64
		createdAt, updatedAt         time.Time
	)

	err := s.db.QueryRowContext(ctx, query, namespace, key, tagsStr).Scan(
		&k, &ns, &tagsResult, &value, &codecName, &valueType, &version, &createdAt, &updatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, &config.KeyNotFoundError{Key: key, Namespace: namespace}
	}
	if err != nil {
		return nil, config.WrapStoreError("get", "postgres", key, err)
	}

	// Parse tags from result
	parsedTags, _ := config.ParseTags(tagsResult)

	return config.NewValueFromBytes(
		value,
		codecName,
		config.WithValueType(valueType),
		config.WithValueMetadata(version, createdAt, updatedAt),
		config.WithValueTags(parsedTags),
	)
}

// Set creates or updates a configuration value.
// Tags are extracted from value.Metadata().Tags().
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
		return config.WrapStoreError("marshal", "postgres", key, err)
	}

	// Get tags from value metadata
	var tagsStr string
	if value.Metadata() != nil {
		tagsStr = config.FormatTags(value.Metadata().Tags())
	}

	// Upsert with version increment using unique constraint on (namespace, key, tags)
	query := fmt.Sprintf(`
		INSERT INTO %s (key, namespace, tags, value, codec, type, version, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, 1, NOW(), NOW())
		ON CONFLICT (namespace, key, tags) DO UPDATE SET
			value = EXCLUDED.value,
			codec = EXCLUDED.codec,
			type = EXCLUDED.type,
			version = %s.version + 1,
			updated_at = NOW()
	`, s.cfg.Table, s.cfg.Table)

	_, err = s.db.ExecContext(ctx, query,
		key,
		namespace,
		tagsStr,
		data,
		value.Codec(),
		value.Type(),
	)

	if err != nil {
		return config.WrapStoreError("set", "postgres", key, err)
	}

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

	tagsStr := config.FormatTags(tags)
	query := fmt.Sprintf(`DELETE FROM %s WHERE namespace = $1 AND key = $2 AND tags = $3`, s.cfg.Table)
	result, err := s.db.ExecContext(ctx, query, namespace, key, tagsStr)
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

	// Add tag filter if specified (AND logic - all must match)
	var tagsFilter string
	if filterTags := filter.Tags(); len(filterTags) > 0 {
		tagsFilter = config.FormatTags(filterTags)
	}

	// Keys mode: get specific keys (no pagination)
	if keys := filter.Keys(); len(keys) > 0 {
		placeholders := make([]string, len(keys))
		for i, key := range keys {
			placeholders[i] = fmt.Sprintf("$%d", argNum)
			args = append(args, key)
			argNum++
		}
		query := fmt.Sprintf(`
			SELECT id, key, namespace, tags, value, codec, type, version, created_at, updated_at
			FROM %s WHERE namespace = $%d AND key IN (%s)
		`, s.cfg.Table, argNum, strings.Join(placeholders, ","))
		args = append(args, namespace)
		argNum++

		if tagsFilter != "" {
			query += fmt.Sprintf(" AND tags = $%d", argNum)
			args = append(args, tagsFilter)
			argNum++
		}

		query += " ORDER BY id"

		results, nextCursor, err := s.executeListQuery(ctx, query, args)
		if err != nil {
			return nil, err
		}
		return config.NewPage(results, nextCursor, 0), nil
	}

	// Prefix mode: get all keys matching prefix
	query := fmt.Sprintf(`
		SELECT id, key, namespace, tags, value, codec, type, version, created_at, updated_at
		FROM %s WHERE namespace = $%d
	`, s.cfg.Table, argNum)
	args = append(args, namespace)
	argNum++

	if tagsFilter != "" {
		query += fmt.Sprintf(" AND tags = $%d", argNum)
		args = append(args, tagsFilter)
		argNum++
	}

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
			id                           int64
			k, ns, tagsResult, codecName string
			value                        []byte
			valueType                    config.Type
			version                      int64
			createdAt, updatedAt         time.Time
		)

		if err := rows.Scan(
			&id, &k, &ns, &tagsResult, &value, &codecName, &valueType, &version, &createdAt, &updatedAt,
		); err != nil {
			return nil, "", config.WrapStoreError("list", "postgres", "", err)
		}

		// Parse tags from result
		parsedTags, _ := config.ParseTags(tagsResult)

		val, err := config.NewValueFromBytes(
			value,
			codecName,
			config.WithValueType(valueType),
			config.WithValueMetadata(version, createdAt, updatedAt),
			config.WithValueTags(parsedTags),
		)
		if err != nil {
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

		// Close channel safely using sync.Once
		entry.closeOnce.Do(func() {
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

// listenNotifications processes LISTEN/NOTIFY events.
func (s *Store) listenNotifications() {
	defer s.watchWg.Done()

	for {
		select {
		case <-s.stopWatch:
			return
		case notification := <-s.listener.Notify:
			if notification == nil {
				// Reconnect event
				continue
			}

			var payload notifyPayload
			if err := json.Unmarshal([]byte(notification.Extra), &payload); err != nil {
				slog.Warn("postgres: failed to decode notification payload", "error", err)
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

			// Parse tags from payload
			tags, _ := config.ParseTags(payload.Tags)

			event := config.ChangeEvent{
				Type:      eventType,
				Namespace: payload.Namespace,
				Key:       payload.Key,
				Tags:      tags,
				Timestamp: time.Now().UTC(),
			}

			// Fetch full record for non-delete events
			if eventType != config.ChangeTypeDelete {
				ctx, cancel := context.WithTimeout(context.Background(), s.cfg.NotificationFetchTimeout)
				if val, err := s.Get(ctx, payload.Namespace, payload.Key, tags...); err == nil {
					event.Value = val
				}
				cancel()
			} else {
				event.Value = nil
			}

			s.dispatchEvent(event)
		}
	}
}

func (s *Store) dispatchEvent(event config.ChangeEvent) {
	s.watchMu.RLock()
	defer s.watchMu.RUnlock()

	for entry := range s.watchers {
		if s.matchesFilter(event, entry.filter) {
			select {
			case entry.ch <- event:
			case <-entry.ctx.Done():
			default:
				// Channel full, skip
			}
		}
	}
}

func (s *Store) matchesFilter(event config.ChangeEvent, filter config.WatchFilter) bool {
	// Check namespace filter
	if len(filter.Namespaces) > 0 {
		found := false
		for _, ns := range filter.Namespaces {
			if event.Namespace == ns {
				found = true
				break
			}
		}
		if !found {
			return false
		}
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
