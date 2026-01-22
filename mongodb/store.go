package mongodb

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/rbaliyan/config"
)

// Store is a MongoDB configuration store implementation.
// Uses MongoDB change streams for real-time watch notifications.
// The store does not manage the MongoDB client lifecycle - the integrating
// application is responsible for creating and closing the client.
type Store struct {
	client     *mongo.Client
	database   *mongo.Database
	collection *mongo.Collection
	closed     atomic.Bool

	// Watch management
	watchMu   sync.RWMutex
	watchers  map[*watchEntry]struct{}
	stopWatch chan struct{}
	watchWg   sync.WaitGroup

	cfg Config
}

// Config holds MongoDB store configuration.
type Config struct {
	// Database is the database name.
	Database string

	// Collection is the collection name for config entries.
	Collection string

	// WatchBufferSize is the channel buffer size for watch subscriptions.
	WatchBufferSize int

	// ReconnectBackoff is the wait time before reconnecting change stream.
	ReconnectBackoff time.Duration
}

// DefaultConfig returns the default configuration.
func DefaultConfig() Config {
	return Config{
		Database:         "config",
		Collection:       "entries",
		WatchBufferSize:  100,
		ReconnectBackoff: 5 * time.Second,
	}
}

type watchEntry struct {
	filter    config.WatchFilter
	ch        chan config.ChangeEvent
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
}

// mongoEntry is the MongoDB document structure.
type mongoEntry struct {
	ID        primitive.ObjectID `bson:"_id,omitempty"`
	Key       string             `bson:"key"`
	Namespace string             `bson:"namespace"`
	Tags      string             `bson:"tags"` // Sorted "key1=value1,key2=value2"
	Value     []byte             `bson:"value"`
	Codec     string             `bson:"codec"`
	Type      config.Type        `bson:"type"`
	Version   int64              `bson:"version"`
	CreatedAt time.Time          `bson:"created_at"`
	UpdatedAt time.Time          `bson:"updated_at"`
}

func (e *mongoEntry) toValue() (config.Value, error) {
	// Parse tags from stored string
	tags, _ := config.ParseTags(e.Tags)

	return config.NewValueFromBytes(
		e.Value,
		e.Codec,
		config.WithValueType(e.Type),
		config.WithValueMetadata(e.Version, e.CreatedAt, e.UpdatedAt),
		config.WithValueTags(tags),
	)
}

// Option configures the MongoDB store.
type Option func(*Store)

// WithConfig sets the store configuration.
func WithConfig(cfg Config) Option {
	return func(s *Store) {
		if cfg.Database != "" {
			s.cfg.Database = cfg.Database
		}
		if cfg.Collection != "" {
			s.cfg.Collection = cfg.Collection
		}
		if cfg.WatchBufferSize > 0 {
			s.cfg.WatchBufferSize = cfg.WatchBufferSize
		}
		if cfg.ReconnectBackoff > 0 {
			s.cfg.ReconnectBackoff = cfg.ReconnectBackoff
		}
	}
}

// WithDatabase sets the database name.
func WithDatabase(name string) Option {
	return func(s *Store) {
		s.cfg.Database = name
	}
}

// WithCollection sets the collection name.
func WithCollection(name string) Option {
	return func(s *Store) {
		s.cfg.Collection = name
	}
}

// NewStore creates a new MongoDB store with the provided client.
// The client must be connected and ready to use.
// The store does not manage the client lifecycle - the caller is responsible
// for closing the client when done.
func NewStore(client *mongo.Client, opts ...Option) *Store {
	s := &Store{
		client:    client,
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

// Connect initializes the store (creates indexes and starts change stream listener).
// The MongoDB client must already be connected.
func (s *Store) Connect(ctx context.Context) error {
	if s.client == nil {
		return config.WrapStoreError("connect", "mongodb", "", fmt.Errorf("client is nil"))
	}

	s.database = s.client.Database(s.cfg.Database)
	s.collection = s.database.Collection(s.cfg.Collection)

	// Create indexes
	if err := s.createIndexes(ctx); err != nil {
		return config.WrapStoreError("create_indexes", "mongodb", "", err)
	}

	// Start change stream listener
	s.watchWg.Add(1)
	go s.watchChangeStream()

	return nil
}

func (s *Store) createIndexes(ctx context.Context) error {
	indexes := []mongo.IndexModel{
		{
			Keys: bson.D{{Key: "namespace", Value: 1}},
		},
		{
			Keys: bson.D{{Key: "key", Value: 1}},
		},
		{
			// Unique index on (namespace, key, tags) - this is the entry identity
			Keys: bson.D{
				{Key: "namespace", Value: 1},
				{Key: "key", Value: 1},
				{Key: "tags", Value: 1},
			},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys: bson.D{{Key: "updated_at", Value: -1}},
		},
		{
			Keys: bson.D{{Key: "type", Value: 1}},
		},
		{
			Keys: bson.D{{Key: "tags", Value: 1}},
		},
	}

	_, err := s.collection.Indexes().CreateMany(ctx, indexes)
	return err
}

// Close stops the change stream listener and closes all watchers.
// It does NOT close the MongoDB client - that is the caller's responsibility.
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
	filter := bson.M{
		"namespace": namespace,
		"key":       key,
		"tags":      tagsStr,
	}

	var doc mongoEntry
	err := s.collection.FindOne(ctx, filter).Decode(&doc)
	if err == mongo.ErrNoDocuments {
		return nil, &config.KeyNotFoundError{Key: key, Namespace: namespace}
	}
	if err != nil {
		return nil, config.WrapStoreError("get", "mongodb", key, err)
	}

	return doc.toValue()
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
		return config.WrapStoreError("marshal", "mongodb", key, err)
	}

	// Get tags from value metadata
	var tagsStr string
	if value.Metadata() != nil {
		tagsStr = config.FormatTags(value.Metadata().Tags())
	}

	now := time.Now().UTC()

	// Use findOneAndUpdate with upsert for atomic version increment
	// Filter by (namespace, key, tags) - the unique identity
	filter := bson.M{
		"namespace": namespace,
		"key":       key,
		"tags":      tagsStr,
	}
	update := bson.M{
		"$set": bson.M{
			"key":        key,
			"namespace":  namespace,
			"tags":       tagsStr,
			"value":      data,
			"codec":      value.Codec(),
			"type":       value.Type(),
			"updated_at": now,
		},
		"$inc": bson.M{"version": int64(1)},
		"$setOnInsert": bson.M{
			"created_at": now,
		},
	}

	opts := options.FindOneAndUpdate().
		SetUpsert(true).
		SetReturnDocument(options.After)

	var doc mongoEntry
	err = s.collection.FindOneAndUpdate(ctx, filter, update, opts).Decode(&doc)
	if err != nil {
		return config.WrapStoreError("set", "mongodb", key, err)
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
	filter := bson.M{
		"namespace": namespace,
		"key":       key,
		"tags":      tagsStr,
	}

	result, err := s.collection.DeleteOne(ctx, filter)
	if err != nil {
		return config.WrapStoreError("delete", "mongodb", key, err)
	}
	if result.DeletedCount == 0 {
		return &config.KeyNotFoundError{Key: key, Namespace: namespace}
	}

	return nil
}

// Find returns a page of keys and values matching the filter within a namespace.
// Uses ObjectID-based pagination for consistent ordering.
func (s *Store) Find(ctx context.Context, namespace string, filter config.Filter) (config.Page, error) {
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
	}
	if err := config.ValidateNamespace(namespace); err != nil {
		return nil, err
	}

	limit := filter.Limit()
	mongoFilter := bson.M{"namespace": namespace}

	// Add tag filter if specified (AND logic - all must match)
	if filterTags := filter.Tags(); len(filterTags) > 0 {
		// For exact tag match, we compare the formatted tags string
		mongoFilter["tags"] = config.FormatTags(filterTags)
	}

	// Keys mode: get specific keys (no pagination)
	// Note: In keys mode with tags, we look for entries matching both key and tags
	if keys := filter.Keys(); len(keys) > 0 {
		mongoFilter["key"] = bson.M{"$in": keys}

		findOpts := options.Find().SetSort(bson.D{{Key: "_id", Value: 1}})
		results, nextCursor, err := s.executeListQuery(ctx, mongoFilter, findOpts)
		if err != nil {
			return nil, err
		}
		return config.NewPage(results, nextCursor, 0), nil
	}

	// Prefix mode
	if prefix := filter.Prefix(); prefix != "" {
		mongoFilter["key"] = bson.M{"$regex": primitive.Regex{Pattern: "^" + escapeRegex(prefix)}}
	}

	// Cursor-based pagination using ObjectID
	if cursor := filter.Cursor(); cursor != "" {
		oid, err := primitive.ObjectIDFromHex(cursor)
		if err == nil {
			mongoFilter["_id"] = bson.M{"$gt": oid}
		}
	}

	// Sort by _id for consistent pagination
	findOpts := options.Find().SetSort(bson.D{{Key: "_id", Value: 1}})

	// Apply limit
	if limit > 0 {
		findOpts.SetLimit(int64(limit))
	}

	results, nextCursor, err := s.executeListQuery(ctx, mongoFilter, findOpts)
	if err != nil {
		return nil, err
	}

	return config.NewPage(results, nextCursor, limit), nil
}

func (s *Store) executeListQuery(ctx context.Context, filter bson.M, opts *options.FindOptions) (map[string]config.Value, string, error) {
	cursor, err := s.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, "", config.WrapStoreError("list", "mongodb", "", err)
	}
	defer cursor.Close(ctx)

	results := make(map[string]config.Value)
	var lastID primitive.ObjectID
	for cursor.Next(ctx) {
		var doc mongoEntry
		if err := cursor.Decode(&doc); err != nil {
			return nil, "", config.WrapStoreError("list", "mongodb", "", err)
		}
		val, err := doc.toValue()
		if err != nil {
			continue
		}
		results[doc.Key] = val
		lastID = doc.ID
	}

	if err := cursor.Err(); err != nil {
		return nil, "", config.WrapStoreError("list", "mongodb", "", err)
	}

	// Return the last ObjectID as the cursor for next page
	nextCursor := ""
	if !lastID.IsZero() {
		nextCursor = lastID.Hex()
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
	return s.client.Ping(ctx, nil)
}

// Stats returns store statistics.
func (s *Store) Stats(ctx context.Context) (*config.StoreStats, error) {
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
	}

	// Count total entries
	total, err := s.collection.CountDocuments(ctx, bson.M{})
	if err != nil {
		return nil, config.WrapStoreError("stats", "mongodb", "", err)
	}

	stats := &config.StoreStats{
		TotalEntries:       total,
		EntriesByType:      make(map[config.Type]int64),
		EntriesByNamespace: make(map[string]int64),
	}

	// Aggregate by type
	typePipeline := mongo.Pipeline{
		{{Key: "$group", Value: bson.M{"_id": "$type", "count": bson.M{"$sum": 1}}}},
	}
	typeCursor, err := s.collection.Aggregate(ctx, typePipeline)
	if err == nil {
		defer typeCursor.Close(ctx)
		for typeCursor.Next(ctx) {
			var result struct {
				ID    config.Type `bson:"_id"`
				Count int64       `bson:"count"`
			}
			if err := typeCursor.Decode(&result); err == nil {
				stats.EntriesByType[result.ID] = result.Count
			}
		}
	}

	// Aggregate by namespace
	nsPipeline := mongo.Pipeline{
		{{Key: "$group", Value: bson.M{"_id": "$namespace", "count": bson.M{"$sum": 1}}}},
	}
	nsCursor, err := s.collection.Aggregate(ctx, nsPipeline)
	if err == nil {
		defer nsCursor.Close(ctx)
		for nsCursor.Next(ctx) {
			var result struct {
				ID    string `bson:"_id"`
				Count int64  `bson:"count"`
			}
			if err := nsCursor.Decode(&result); err == nil {
				stats.EntriesByNamespace[result.ID] = result.Count
			}
		}
	}

	return stats, nil
}

// watchChangeStream listens to MongoDB change stream and dispatches events.
func (s *Store) watchChangeStream() {
	defer s.watchWg.Done()

	pipeline := mongo.Pipeline{}
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)

	for {
		select {
		case <-s.stopWatch:
			return
		default:
		}

		// Create a cancellable context for this watch iteration
		watchCtx, watchCancel := context.WithCancel(context.Background())

		// Start a goroutine to cancel the context when stopWatch is signaled
		go func() {
			select {
			case <-s.stopWatch:
				watchCancel()
			case <-watchCtx.Done():
				// Context already cancelled, nothing to do
			}
		}()

		stream, err := s.collection.Watch(watchCtx, pipeline, opts)
		if err != nil {
			watchCancel() // Clean up the context
			// Backoff and retry
			select {
			case <-s.stopWatch:
				return
			case <-time.After(s.cfg.ReconnectBackoff):
				continue
			}
		}

		s.processChangeStream(stream, watchCtx)
		stream.Close(watchCtx)
		watchCancel() // Ensure context is cancelled after processing
	}
}

func (s *Store) processChangeStream(stream *mongo.ChangeStream, ctx context.Context) {
	for stream.Next(ctx) {
		select {
		case <-s.stopWatch:
			return
		case <-ctx.Done():
			return
		default:
		}

		var change struct {
			OperationType      string     `bson:"operationType"`
			DocumentKey        bson.M     `bson:"documentKey"`
			FullDocument       mongoEntry `bson:"fullDocument"`
			FullDocumentBefore mongoEntry `bson:"fullDocumentBeforeChange"` // Requires MongoDB 6.0+ with pre-image
		}
		if err := stream.Decode(&change); err != nil {
			slog.Warn("mongodb: failed to decode change stream event", "error", err)
			continue
		}

		var eventType config.ChangeType
		switch change.OperationType {
		case "insert":
			eventType = config.ChangeTypeSet
		case "update", "replace":
			eventType = config.ChangeTypeSet
		case "delete":
			eventType = config.ChangeTypeDelete
		default:
			continue
		}

		event := config.ChangeEvent{
			Type:      eventType,
			Timestamp: time.Now().UTC(),
		}

		if eventType != config.ChangeTypeDelete {
			event.Namespace = change.FullDocument.Namespace
			event.Key = change.FullDocument.Key
			event.Tags, _ = config.ParseTags(change.FullDocument.Tags)
			event.Value, _ = change.FullDocument.toValue()
		} else {
			// For deletes, we can use FullDocumentBeforeChange if available (MongoDB 6.0+)
			// Otherwise, we skip delete events as we can't determine the key from ObjectID
			if change.FullDocumentBefore.Key != "" {
				event.Namespace = change.FullDocumentBefore.Namespace
				event.Key = change.FullDocumentBefore.Key
				event.Tags, _ = config.ParseTags(change.FullDocumentBefore.Tags)
			} else {
				// Skip delete events when we can't determine the key
				// Consider enabling pre-image on collection for full delete support
				slog.Debug("mongodb: skipping delete event - unable to determine key from ObjectID")
				continue
			}
			event.Value = nil
		}

		s.dispatchEvent(event)
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

func escapeRegex(s string) string {
	// Escape special regex characters
	special := []string{".", "+", "*", "?", "^", "$", "(", ")", "[", "]", "{", "}", "|", "\\"}
	result := s
	for _, char := range special {
		result = strings.ReplaceAll(result, char, "\\"+char)
	}
	return result
}
