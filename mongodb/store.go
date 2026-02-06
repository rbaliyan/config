// Package mongodb provides a MongoDB-backed configuration store with change stream notifications.
package mongodb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

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
	watchMu       sync.RWMutex
	watchers      map[*watchEntry]struct{}
	stopWatch     chan struct{}
	watchWg       sync.WaitGroup
	droppedEvents atomic.Int64                   // Counter for dropped events due to full channels
	onDropped     func(event config.ChangeEvent) // Optional callback when event is dropped

	// ID-to-key mapping for delete events (populated from insert/update events)
	idMapMu sync.RWMutex
	idToKey map[bson.ObjectID]docIdentity

	cfg Config
}

// docIdentity holds the namespace and key for a document, used for delete event lookups.
type docIdentity struct {
	namespace string
	key       string
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

	// AutoCreateIndexes controls whether indexes are created automatically on Connect.
	// Default is true. Set to false if you want to manage indexes manually.
	AutoCreateIndexes bool

	// Capped enables capped collection mode.
	// Capped collections have a fixed size and automatically remove oldest documents.
	// Note: Capped collections cannot have unique indexes, so lookups
	// may return multiple entries. Use with caution.
	Capped bool

	// CappedSizeBytes is the maximum size in bytes for a capped collection.
	// Only used when Capped is true. Default is 100MB.
	CappedSizeBytes int64

	// CappedMaxDocuments is the maximum number of documents in a capped collection.
	// Only used when Capped is true. Default is 0 (no document limit, only size limit).
	CappedMaxDocuments int64

	// EnablePreImages enables change stream pre-images for the collection.
	// This is required for delete events to include the deleted document's data.
	// Requires MongoDB 6.0+ and creates the collection with changeStreamPreAndPostImages enabled.
	// Default is false.
	EnablePreImages bool

	// MaxIDCacheSize is the maximum number of entries in the ID-to-key cache.
	// This cache is used to resolve delete events when pre-images are unavailable.
	// When the limit is reached, the cache is cleared. Default is 10000.
	MaxIDCacheSize int

	// Logger is the logger for the store. If nil, uses slog.Default().
	Logger *slog.Logger
}

// DefaultConfig returns the default configuration.
func DefaultConfig() Config {
	return Config{
		Database:           "config",
		Collection:         "entries",
		WatchBufferSize:    100,
		ReconnectBackoff:   5 * time.Second,
		AutoCreateIndexes:  true,
		Capped:             false,
		CappedSizeBytes:    100 * 1024 * 1024, // 100MB
		CappedMaxDocuments: 0,                 // No document limit
		MaxIDCacheSize:     10000,             // 10K entries
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

// mongoEntry is the MongoDB document structure.
type mongoEntry struct {
	ID        bson.ObjectID `bson:"_id,omitempty"`
	Key       string        `bson:"key"`
	Namespace string        `bson:"namespace"`
	Value     []byte        `bson:"value"`
	Codec     string        `bson:"codec"`
	Type      config.Type   `bson:"type"`
	Version   int64         `bson:"version"`
	CreatedAt time.Time     `bson:"created_at"`
	UpdatedAt time.Time     `bson:"updated_at"`
}

func (e *mongoEntry) toValue() (config.Value, error) {
	return config.NewValueFromBytes(
		e.Value,
		e.Codec,
		config.WithValueType(e.Type),
		config.WithValueMetadata(e.Version, e.CreatedAt, e.UpdatedAt),
		config.WithValueEntryID(e.ID.Hex()),
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
		// Boolean fields need explicit handling
		s.cfg.AutoCreateIndexes = cfg.AutoCreateIndexes
		s.cfg.Capped = cfg.Capped
		if cfg.CappedSizeBytes > 0 {
			s.cfg.CappedSizeBytes = cfg.CappedSizeBytes
		}
		if cfg.CappedMaxDocuments > 0 {
			s.cfg.CappedMaxDocuments = cfg.CappedMaxDocuments
		}
		if cfg.Logger != nil {
			s.cfg.Logger = cfg.Logger
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

// WithAutoCreateIndexes controls whether indexes are created automatically on Connect.
func WithAutoCreateIndexes(enabled bool) Option {
	return func(s *Store) {
		s.cfg.AutoCreateIndexes = enabled
	}
}

// WithCapped enables capped collection mode with the specified size.
// Capped collections have a fixed size and automatically remove oldest documents.
// sizeBytes is the maximum size in bytes (must be > 0).
// maxDocuments is the optional maximum number of documents (0 = no limit).
//
// Note: Capped collections cannot have unique indexes, so the unique constraint
// on (namespace, key) will not be enforced. Consider using regular collections
// for production use where uniqueness is important.
func WithCapped(sizeBytes int64, maxDocuments int64) Option {
	return func(s *Store) {
		s.cfg.Capped = true
		if sizeBytes > 0 {
			s.cfg.CappedSizeBytes = sizeBytes
		}
		if maxDocuments > 0 {
			s.cfg.CappedMaxDocuments = maxDocuments
		}
	}
}

// WithLogger sets the logger for the store.
func WithLogger(logger *slog.Logger) Option {
	return func(s *Store) {
		s.cfg.Logger = logger
	}
}

// WithPreImages enables change stream pre-images for the collection.
// This allows delete events to include the deleted document's namespace and key.
// Requires MongoDB 6.0+ and will create the collection with changeStreamPreAndPostImages enabled.
// Without pre-images, delete events cannot determine the key and will be skipped.
func WithPreImages(enabled bool) Option {
	return func(s *Store) {
		s.cfg.EnablePreImages = enabled
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

// WithMaxIDCacheSize sets the maximum size of the ID-to-key cache.
// This cache maps MongoDB ObjectIDs to (namespace, key) pairs for resolving
// delete events when pre-images are unavailable. Default is 10000.
// Set to 0 to disable the size limit (not recommended for large collections).
func WithMaxIDCacheSize(size int) Option {
	return func(s *Store) {
		s.cfg.MaxIDCacheSize = size
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
		idToKey:   make(map[bson.ObjectID]docIdentity),
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

// Connect initializes the store (optionally creates collection/indexes and starts change stream listener).
// The MongoDB client must already be connected.
func (s *Store) Connect(ctx context.Context) error {
	if s.client == nil {
		return config.WrapStoreError("connect", "mongodb", "", fmt.Errorf("client is nil"))
	}

	s.database = s.client.Database(s.cfg.Database)

	// Create capped collection if configured
	if s.cfg.Capped {
		if err := s.ensureCappedCollection(ctx); err != nil {
			return config.WrapStoreError("create_collection", "mongodb", "", err)
		}
	}

	// Enable pre-images if configured (for delete event support)
	if s.cfg.EnablePreImages {
		if err := s.ensurePreImagesEnabled(ctx); err != nil {
			return config.WrapStoreError("enable_pre_images", "mongodb", "", err)
		}
	}

	s.collection = s.database.Collection(s.cfg.Collection)

	// Create indexes if auto-creation is enabled
	if s.cfg.AutoCreateIndexes {
		if err := s.EnsureIndexes(ctx); err != nil {
			return config.WrapStoreError("create_indexes", "mongodb", "", err)
		}
	}

	// Start change stream listener
	s.watchWg.Add(1)
	go s.watchChangeStream()

	s.logger().Info("mongodb store connected",
		"database", s.cfg.Database,
		"collection", s.cfg.Collection,
		"capped", s.cfg.Capped,
		"preImages", s.cfg.EnablePreImages,
	)

	return nil
}

// ensureCappedCollection creates the collection as a capped collection if it doesn't exist.
func (s *Store) ensureCappedCollection(ctx context.Context) error {
	// Check if collection already exists
	collections, err := s.database.ListCollectionNames(ctx, bson.M{"name": s.cfg.Collection})
	if err != nil {
		return fmt.Errorf("failed to list collections: %w", err)
	}

	if len(collections) > 0 {
		// Collection exists, check if it's capped
		// Note: We can't convert a regular collection to capped, so just log a warning
		s.logger().Debug("collection already exists, skipping capped collection creation",
			"collection", s.cfg.Collection)
		return nil
	}

	// Create capped collection
	opts := options.CreateCollection().
		SetCapped(true).
		SetSizeInBytes(s.cfg.CappedSizeBytes)

	if s.cfg.CappedMaxDocuments > 0 {
		opts.SetMaxDocuments(s.cfg.CappedMaxDocuments)
	}

	if err := s.database.CreateCollection(ctx, s.cfg.Collection, opts); err != nil {
		return fmt.Errorf("failed to create capped collection: %w", err)
	}

	s.logger().Info("created capped collection",
		"collection", s.cfg.Collection,
		"sizeBytes", s.cfg.CappedSizeBytes,
		"maxDocuments", s.cfg.CappedMaxDocuments,
	)

	return nil
}

// ensurePreImagesEnabled enables change stream pre-images on the collection.
// This requires MongoDB 6.0+ and allows delete events to include the deleted document.
func (s *Store) ensurePreImagesEnabled(ctx context.Context) error {
	// Check if collection already exists
	collections, err := s.database.ListCollectionNames(ctx, bson.M{"name": s.cfg.Collection})
	if err != nil {
		return fmt.Errorf("failed to list collections: %w", err)
	}

	if len(collections) == 0 {
		// Collection doesn't exist, create it with pre-images enabled
		opts := options.CreateCollection().
			SetChangeStreamPreAndPostImages(bson.M{"enabled": true})

		if err := s.database.CreateCollection(ctx, s.cfg.Collection, opts); err != nil {
			return fmt.Errorf("failed to create collection with pre-images: %w", err)
		}

		s.logger().Info("created collection with pre-images enabled",
			"collection", s.cfg.Collection,
		)
	} else {
		// Collection exists, try to enable pre-images via collMod
		cmd := bson.D{
			{Key: "collMod", Value: s.cfg.Collection},
			{Key: "changeStreamPreAndPostImages", Value: bson.M{"enabled": true}},
		}

		if err := s.database.RunCommand(ctx, cmd).Err(); err != nil {
			// Log warning but don't fail - collection might already have it enabled
			// or MongoDB version might not support it
			s.logger().Warn("failed to enable pre-images on existing collection",
				"collection", s.cfg.Collection,
				"error", err,
			)
		} else {
			s.logger().Info("enabled pre-images on existing collection",
				"collection", s.cfg.Collection,
			)
		}
	}

	return nil
}

// logger returns the configured logger or the default logger.
func (s *Store) logger() *slog.Logger {
	if s.cfg.Logger != nil {
		return s.cfg.Logger
	}
	return slog.Default()
}

// EnsureIndexes creates the required indexes on the collection.
// This is called automatically during Connect if AutoCreateIndexes is true (default).
// You can also call this manually if you disabled auto-creation.
//
// For regular collections, creates:
//   - Unique composite index on (namespace, key)
//   - Index on namespace
//   - Index on key
//   - Index on type
//   - Index on updated_at (descending)
//
// For capped collections, the unique index is skipped as capped collections
// don't support unique indexes.
func (s *Store) EnsureIndexes(ctx context.Context) error {
	if s.collection == nil {
		return fmt.Errorf("collection not initialized, call Connect first")
	}

	indexes := []mongo.IndexModel{
		{
			Keys: bson.D{{Key: "namespace", Value: 1}},
		},
		{
			Keys: bson.D{{Key: "key", Value: 1}},
		},
		{
			Keys: bson.D{{Key: "updated_at", Value: -1}},
		},
		{
			Keys: bson.D{{Key: "type", Value: 1}},
		},
	}

	// Add unique composite index only for non-capped collections
	// Capped collections don't support unique indexes
	if !s.cfg.Capped {
		indexes = append(indexes, mongo.IndexModel{
			// Unique index on (namespace, key) - this is the entry identity
			Keys: bson.D{
				{Key: "namespace", Value: 1},
				{Key: "key", Value: 1},
			},
			Options: options.Index().SetUnique(true),
		})
	} else {
		// For capped collections, add a non-unique composite index for lookups
		indexes = append(indexes, mongo.IndexModel{
			Keys: bson.D{
				{Key: "namespace", Value: 1},
				{Key: "key", Value: 1},
			},
		})
	}

	created, err := s.collection.Indexes().CreateMany(ctx, indexes)
	if err != nil {
		return err
	}

	s.logger().Debug("indexes ensured",
		"collection", s.cfg.Collection,
		"indexCount", len(created),
		"capped", s.cfg.Capped,
	)

	return nil
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
			entry.mu.Lock()
			entry.closed = true
			close(entry.ch)
			entry.mu.Unlock()
		})
	}
	s.watchers = make(map[*watchEntry]struct{})
	s.watchMu.Unlock()

	// Clear ID-to-key mapping
	s.idMapMu.Lock()
	s.idToKey = make(map[bson.ObjectID]docIdentity)
	s.idMapMu.Unlock()

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

	filter := bson.M{
		"namespace": namespace,
		"key":       key,
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
		return nil, config.WrapStoreError("marshal", "mongodb", key, err)
	}

	now := time.Now().UTC()

	writeMode := config.GetWriteMode(value)
	switch writeMode {
	case config.WriteModeCreate:
		// Insert only - will fail if document exists due to unique index
		doc := mongoEntry{
			Key:       key,
			Namespace: namespace,
			Value:     data,
			Codec:     value.Codec(),
			Type:      value.Type(),
			Version:   1,
			CreatedAt: now,
			UpdatedAt: now,
		}

		result, err := s.collection.InsertOne(ctx, doc)
		if err != nil {
			// Check if it's a duplicate key error
			if mongo.IsDuplicateKeyError(err) {
				return nil, &config.KeyExistsError{Key: key, Namespace: namespace}
			}
			return nil, config.WrapStoreError("set", "mongodb", key, err)
		}

		// Set the ID from the insert result
		if oid, ok := result.InsertedID.(bson.ObjectID); ok {
			doc.ID = oid
		}
		return doc.toValue()

	case config.WriteModeUpdate:
		// Update only - fail if document doesn't exist
		filter := bson.M{
			"namespace": namespace,
			"key":       key,
		}
		update := bson.M{
			"$set": bson.M{
				"value":      data,
				"codec":      value.Codec(),
				"type":       value.Type(),
				"updated_at": now,
			},
			"$inc": bson.M{"version": int64(1)},
		}

		opts := options.FindOneAndUpdate().
			SetUpsert(false).
			SetReturnDocument(options.After)

		var doc mongoEntry
		err = s.collection.FindOneAndUpdate(ctx, filter, update, opts).Decode(&doc)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				return nil, &config.KeyNotFoundError{Key: key, Namespace: namespace}
			}
			return nil, config.WrapStoreError("set", "mongodb", key, err)
		}
		return doc.toValue()

	default:
		// Upsert (default) - create or update
		filter := bson.M{
			"namespace": namespace,
			"key":       key,
		}
		update := bson.M{
			"$set": bson.M{
				"key":        key,
				"namespace":  namespace,
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
			return nil, config.WrapStoreError("set", "mongodb", key, err)
		}
		return doc.toValue()
	}
}

// Delete removes a configuration value by namespace and key.
func (s *Store) Delete(ctx context.Context, namespace, key string) error {
	if s.closed.Load() {
		return config.ErrStoreClosed
	}
	if err := config.ValidateNamespace(namespace); err != nil {
		return err
	}

	filter := bson.M{
		"namespace": namespace,
		"key":       key,
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

	// Keys mode: get specific keys (no pagination)
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
		mongoFilter["key"] = bson.M{"$regex": bson.Regex{Pattern: "^" + escapeRegex(prefix)}}
	}

	// Cursor-based pagination using ObjectID
	if cursor := filter.Cursor(); cursor != "" {
		oid, err := bson.ObjectIDFromHex(cursor)
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

func (s *Store) executeListQuery(ctx context.Context, filter bson.M, opts *options.FindOptionsBuilder) (map[string]config.Value, string, error) {
	cursor, err := s.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, "", config.WrapStoreError("list", "mongodb", "", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	results := make(map[string]config.Value)
	var lastID bson.ObjectID
	for cursor.Next(ctx) {
		var doc mongoEntry
		if err := cursor.Decode(&doc); err != nil {
			return nil, "", config.WrapStoreError("list", "mongodb", "", err)
		}
		val, err := doc.toValue()
		if err != nil {
			s.logger().Warn("skipping corrupt entry in list query",
				"key", doc.Key, "namespace", doc.Namespace, "error", err)
			lastID = doc.ID
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
		defer func() { _ = typeCursor.Close(ctx) }()
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
		defer func() { _ = nsCursor.Close(ctx) }()
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

	filter := bson.M{
		"namespace": namespace,
		"key":       bson.M{"$in": keys},
	}

	cursor, err := s.collection.Find(ctx, filter)
	if err != nil {
		return nil, config.WrapStoreError("get_many", "mongodb", "", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	results := make(map[string]config.Value, len(keys))
	for cursor.Next(ctx) {
		var doc mongoEntry
		if err := cursor.Decode(&doc); err != nil {
			return nil, config.WrapStoreError("get_many", "mongodb", "", err)
		}
		val, err := doc.toValue()
		if err != nil {
			s.logger().Warn("skipping corrupt entry in get_many",
				"key", doc.Key, "namespace", doc.Namespace, "error", err)
			continue
		}
		results[doc.Key] = val
	}

	if err := cursor.Err(); err != nil {
		return nil, config.WrapStoreError("get_many", "mongodb", "", err)
	}

	return results, nil
}

// SetMany creates or updates multiple values in a single operation.
// Returns an error if any individual set fails. Successfully set values
// are still persisted even if some fail.
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
	now := time.Now().UTC()

	// Use bulk write for efficiency
	var models []mongo.WriteModel
	indexToKey := make([]string, 0, len(values)) // Track which key is at which index

	for key, value := range values {
		if key == "" {
			keyErrors[key] = &config.InvalidKeyError{Key: key, Reason: "key cannot be empty"}
			continue
		}

		data, err := value.Marshal()
		if err != nil {
			keyErrors[key] = config.WrapStoreError("marshal", "mongodb", key, err)
			continue
		}

		filter := bson.M{
			"namespace": namespace,
			"key":       key,
		}
		update := bson.M{
			"$set": bson.M{
				"key":        key,
				"namespace":  namespace,
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

		model := mongo.NewUpdateOneModel().
			SetFilter(filter).
			SetUpdate(update).
			SetUpsert(true)

		models = append(models, model)
		indexToKey = append(indexToKey, key)
	}

	// Execute bulk write if we have valid operations
	if len(models) > 0 {
		opts := options.BulkWrite().SetOrdered(false) // Continue on error
		_, err := s.collection.BulkWrite(ctx, models, opts)
		if err != nil {
			// Check for bulk write exception with partial failures
			var bulkErr mongo.BulkWriteException
			if errors.As(err, &bulkErr) {
				// Mark failed keys based on WriteError index
				failedIndices := make(map[int]bool)
				for _, writeErr := range bulkErr.WriteErrors {
					if writeErr.Index < len(indexToKey) {
						key := indexToKey[writeErr.Index]
						keyErrors[key] = config.WrapStoreError("set_many", "mongodb", key, writeErr)
						failedIndices[writeErr.Index] = true
					}
				}
				// Mark succeeded keys (those not in failedIndices)
				for i, key := range indexToKey {
					if !failedIndices[i] {
						succeeded = append(succeeded, key)
					}
				}
			} else {
				// Complete failure - all keys failed
				return config.WrapStoreError("set_many", "mongodb", "", err)
			}
		} else {
			// All operations succeeded
			succeeded = append(succeeded, indexToKey...)
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

	filter := bson.M{
		"namespace": namespace,
		"key":       bson.M{"$in": keys},
	}

	result, err := s.collection.DeleteMany(ctx, filter)
	if err != nil {
		return 0, config.WrapStoreError("delete_many", "mongodb", "", err)
	}

	return result.DeletedCount, nil
}

// watchChangeStream listens to MongoDB change stream and dispatches events.
func (s *Store) watchChangeStream() {
	defer s.watchWg.Done()

	pipeline := mongo.Pipeline{}
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)

	// Request pre-images for delete events if enabled
	if s.cfg.EnablePreImages {
		opts.SetFullDocumentBeforeChange(options.WhenAvailable)
	}

	// Create a single done context cancelled when stopWatch fires.
	// All per-iteration watch contexts derive from this, avoiding
	// a monitoring goroutine per reconnect iteration.
	doneCtx, doneCancel := context.WithCancel(context.Background())
	go func() {
		<-s.stopWatch
		doneCancel()
	}()

	for {
		select {
		case <-doneCtx.Done():
			return
		default:
		}

		// Create a cancellable context for this watch iteration
		watchCtx, watchCancel := context.WithCancel(doneCtx)

		stream, err := s.collection.Watch(watchCtx, pipeline, opts)
		if err != nil {
			watchCancel()
			// Backoff and retry
			select {
			case <-doneCtx.Done():
				return
			case <-time.After(s.cfg.ReconnectBackoff):
				continue
			}
		}

		s.processChangeStream(stream, watchCtx)
		_ = stream.Close(watchCtx)
		watchCancel()
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
			s.logger().Warn("failed to decode change stream event", "error", err)
			continue
		}

		// Extract ObjectID from documentKey
		var docID bson.ObjectID
		if id, ok := change.DocumentKey["_id"].(bson.ObjectID); ok {
			docID = id
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
			val, err := change.FullDocument.toValue()
			if err != nil {
				s.logger().Warn("failed to convert change stream document to value",
					"key", change.FullDocument.Key, "namespace", change.FullDocument.Namespace, "error", err)
			}
			event.Value = val

			// Cache the ID -> (namespace, key) mapping for future delete events
			if !docID.IsZero() && event.Key != "" {
				s.idMapMu.Lock()
				// Clear cache if it exceeds the size limit
				if s.cfg.MaxIDCacheSize > 0 && len(s.idToKey) >= s.cfg.MaxIDCacheSize {
					s.idToKey = make(map[bson.ObjectID]docIdentity)
					s.logger().Debug("mongodb: ID cache cleared due to size limit", "limit", s.cfg.MaxIDCacheSize)
				}
				s.idToKey[docID] = docIdentity{namespace: event.Namespace, key: event.Key}
				s.idMapMu.Unlock()
			}
		} else {
			// For deletes, try multiple sources to determine the key:
			// 1. Pre-image (MongoDB 6.0+ with changeStreamPreAndPostImages enabled)
			// 2. Cached ID-to-key mapping (populated from previous insert/update events)
			if change.FullDocumentBefore.Key != "" {
				event.Namespace = change.FullDocumentBefore.Namespace
				event.Key = change.FullDocumentBefore.Key
			} else if !docID.IsZero() {
				// Lookup from our cached mapping
				s.idMapMu.RLock()
				identity, found := s.idToKey[docID]
				s.idMapMu.RUnlock()

				if found {
					event.Namespace = identity.namespace
					event.Key = identity.key
				} else {
					// Skip delete events when we can't determine the key
					s.logger().Debug("mongodb: skipping delete event - document ID not in cache (event occurred before watch started)")
					continue
				}
			} else {
				s.logger().Warn("mongodb: skipping delete event - no document ID available")
				continue
			}

			// Remove from cache after processing delete
			if !docID.IsZero() {
				s.idMapMu.Lock()
				delete(s.idToKey, docID)
				s.idMapMu.Unlock()
			}

			event.Value = nil
		}

		s.dispatchEvent(event)
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

func escapeRegex(s string) string {
	// Escape special regex characters
	special := []string{".", "+", "*", "?", "^", "$", "(", ")", "[", "]", "{", "}", "|", "\\"}
	result := s
	for _, char := range special {
		result = strings.ReplaceAll(result, char, "\\"+char)
	}
	return result
}
