package mongodb

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/codec"
)

// mongoVersion is the document layout of a single snapshot in the versions
// collection. There is one document per (namespace, key, version).
type mongoVersion struct {
	ID        bson.ObjectID `bson:"_id,omitempty"`
	Key       string        `bson:"key"`
	Namespace string        `bson:"namespace"`
	Value     bson.RawValue `bson:"value"`
	Codec     string        `bson:"codec"`
	Type      config.Type   `bson:"type"`
	Version   int64         `bson:"version"`
	CreatedAt time.Time     `bson:"created_at"`
	UpdatedAt time.Time     `bson:"updated_at"`
	ExpiresAt *time.Time    `bson:"expires_at,omitempty"`
}

func (v *mongoVersion) toValue(ctx context.Context) (config.Value, error) {
	c := codec.Get(v.Codec)
	if c == nil {
		return nil, fmt.Errorf("read version for key %q: codec %q not registered", v.Key, v.Codec)
	}
	data, err := fromBSONValue(c, v.Value)
	if err != nil {
		return nil, fmt.Errorf("read version for key %q: %w", v.Key, err)
	}
	var expiresAt time.Time
	if v.ExpiresAt != nil {
		expiresAt = *v.ExpiresAt
	}
	return config.NewValueFromBytes(
		ctx,
		data,
		v.Codec,
		config.WithValueType(v.Type),
		config.WithValueMetadata(v.Version, v.CreatedAt, v.UpdatedAt),
		config.WithValueEntryID(v.ID.Hex()),
		config.WithValueExpiresAt(expiresAt),
	)
}

// versionsCollectionName returns the configured versions collection name,
// falling back to "{Collection}_versions" when not set.
func (s *Store) versionsCollectionName() string {
	if s.cfg.VersionsCollection != "" {
		return s.cfg.VersionsCollection
	}
	return s.cfg.Collection + "_versions"
}

// versionsCollection returns the mongo.Collection used for version snapshots,
// or nil when versioning is disabled or the store has not been connected.
func (s *Store) versionsCollection() *mongo.Collection {
	if !s.cfg.EnableVersioning || s.database == nil {
		return nil
	}
	return s.database.Collection(s.versionsCollectionName())
}

// ensureVersionsIndexes creates the indexes required by the versions
// collection. Called from Connect when versioning is enabled.
//
// Indexes:
//   - unique compound (namespace, key, version desc): identity + listing path
//   - TTL on expires_at (sparse): auto-clean snapshots tied to TTL-bound entries
func (s *Store) ensureVersionsIndexes(ctx context.Context) error {
	coll := s.versionsCollection()
	if coll == nil {
		return nil
	}
	indexes := []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "namespace", Value: 1},
				{Key: "key", Value: 1},
				{Key: "version", Value: -1},
			},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys:    bson.D{{Key: "expires_at", Value: 1}},
			Options: options.Index().SetExpireAfterSeconds(0).SetSparse(true),
		},
	}
	if _, err := coll.Indexes().CreateMany(ctx, indexes); err != nil {
		return err
	}
	return nil
}

// writeVersionSnapshot inserts a snapshot of doc into the versions collection
// and trims the history to MaxHistory when configured. Best-effort: failures
// are logged but do not propagate to the caller, since the source-of-truth
// entry has already been written successfully.
func (s *Store) writeVersionSnapshot(ctx context.Context, doc *mongoEntry) {
	coll := s.versionsCollection()
	if coll == nil {
		return
	}
	snap := mongoVersion{
		Key:       doc.Key,
		Namespace: doc.Namespace,
		Value:     doc.Value,
		Codec:     doc.Codec,
		Type:      doc.Type,
		Version:   doc.Version,
		CreatedAt: doc.CreatedAt,
		UpdatedAt: doc.UpdatedAt,
		ExpiresAt: doc.ExpiresAt,
	}
	if _, err := coll.InsertOne(ctx, snap); err != nil {
		// A duplicate-key error can happen if SetMany's read-back races with
		// another writer and reads back a version already snapshotted. Treat
		// duplicates as a no-op; surface all other errors as warnings.
		if !mongo.IsDuplicateKeyError(err) {
			s.logger().Warn("mongodb: failed to write version snapshot",
				"namespace", doc.Namespace,
				"key", doc.Key,
				"version", doc.Version,
				"error", err,
			)
			return
		}
	}
	if s.cfg.MaxHistory > 0 {
		s.trimVersions(ctx, doc.Namespace, doc.Key)
	}
}

// snapshotKeys reads back the live entries for the given keys and writes a
// version snapshot for each. Used by SetMany after a successful bulk write,
// where the per-key new version is not returned by the driver.
//
// Concurrency note: between BulkWrite and this read-back, another writer
// could bump the version. In that case we snapshot the newer version instead
// of the one we wrote — still monotonic and consistent, but a rare snapshot
// gap is possible under high contention. This is acceptable for an opt-in
// history feature.
func (s *Store) snapshotKeys(ctx context.Context, namespace string, keys []string) {
	coll := s.versionsCollection()
	if coll == nil || len(keys) == 0 {
		return
	}
	filter := bson.M{
		"namespace": namespace,
		"key":       bson.M{"$in": keys},
	}
	cur, err := s.collection.Find(ctx, filter)
	if err != nil {
		s.logger().Warn("mongodb: snapshot read-back failed",
			"namespace", namespace, "error", err)
		return
	}
	defer func() { _ = cur.Close(ctx) }()
	for cur.Next(ctx) {
		var doc mongoEntry
		if err := cur.Decode(&doc); err != nil {
			s.logger().Warn("mongodb: snapshot decode failed",
				"namespace", namespace, "error", err)
			continue
		}
		s.writeVersionSnapshot(ctx, &doc)
	}
}

// trimVersions deletes snapshots older than the MaxHistory+1 most recent
// versions for the given key. Best-effort.
func (s *Store) trimVersions(ctx context.Context, namespace, key string) {
	coll := s.versionsCollection()
	if coll == nil || s.cfg.MaxHistory <= 0 {
		return
	}
	keep := int64(s.cfg.MaxHistory + 1)

	findOpts := options.Find().
		SetSort(bson.D{{Key: "version", Value: -1}}).
		SetSkip(keep).
		SetProjection(bson.M{"version": 1})

	cur, err := coll.Find(ctx, bson.M{"namespace": namespace, "key": key}, findOpts)
	if err != nil {
		s.logger().Warn("mongodb: trim versions find failed",
			"namespace", namespace, "key", key, "error", err)
		return
	}
	defer func() { _ = cur.Close(ctx) }()

	var toDelete []int64
	for cur.Next(ctx) {
		var d struct {
			Version int64 `bson:"version"`
		}
		if err := cur.Decode(&d); err != nil {
			continue
		}
		toDelete = append(toDelete, d.Version)
	}
	if len(toDelete) == 0 {
		return
	}
	delFilter := bson.M{
		"namespace": namespace,
		"key":       key,
		"version":   bson.M{"$in": toDelete},
	}
	if _, err := coll.DeleteMany(ctx, delFilter); err != nil {
		s.logger().Warn("mongodb: trim versions delete failed",
			"namespace", namespace, "key", key, "error", err)
	}
}

// deleteAllVersions removes every snapshot for the given keys. Best-effort:
// a failure here only leaves orphan snapshots — GetVersions filters them out
// by checking the live entry first.
func (s *Store) deleteAllVersions(ctx context.Context, namespace string, keys ...string) {
	coll := s.versionsCollection()
	if coll == nil || len(keys) == 0 {
		return
	}
	filter := bson.M{
		"namespace": namespace,
		"key":       bson.M{"$in": keys},
	}
	if _, err := coll.DeleteMany(ctx, filter); err != nil {
		s.logger().Warn("mongodb: failed to delete version snapshots",
			"namespace", namespace, "keys", keys, "error", err)
	}
}

// GetVersions retrieves the version history for a key.
//
// Returns ErrVersioningNotSupported when versioning is disabled on this store.
// Returns ErrNotFound when the key does not exist (Delete drops all history
// alongside the live entry, so the absence of a live entry implies no history).
// Returns ErrVersionNotFound when a specific version is requested but missing.
//
// Listing mode returns versions ordered by version descending. The cursor is
// the version number of the last entry on the previous page; the next page
// returns versions strictly less than the cursor.
//
// History note: snapshots are written only for Set/SetMany operations made
// while versioning is enabled. Entries written before versioning was enabled
// — or under MaxHistory pressure — may have an incomplete history.
func (s *Store) GetVersions(ctx context.Context, namespace, key string, filter config.VersionFilter) (config.VersionPage, error) {
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
	}
	if !s.cfg.EnableVersioning {
		return nil, config.ErrVersioningNotSupported
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

	coll := s.versionsCollection()
	if coll == nil {
		return nil, config.ErrVersioningNotSupported
	}

	// Confirm the live entry exists. Delete drops both the entry and its
	// snapshots, so a missing entry means "no such key", not "empty history".
	// Using Get also honours TTL expiry semantics for free.
	if _, err := s.Get(ctx, namespace, key); err != nil {
		return nil, err
	}

	// Specific version request
	if v := filter.Version(); v > 0 {
		var doc mongoVersion
		err := coll.FindOne(ctx, bson.M{
			"namespace": namespace,
			"key":       key,
			"version":   v,
		}).Decode(&doc)
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, &config.VersionNotFoundError{Key: key, Namespace: namespace, Version: v}
		}
		if err != nil {
			return nil, config.WrapStoreError("get_versions", "mongodb", key, err)
		}
		val, err := doc.toValue(ctx)
		if err != nil {
			return nil, config.WrapStoreError("get_versions", "mongodb", key, err)
		}
		return config.NewVersionPage([]config.Value{val}, "", 1), nil
	}

	// Listing mode with cursor + limit
	mongoFilter := bson.M{"namespace": namespace, "key": key}
	if cursor := filter.Cursor(); cursor != "" {
		cv, err := strconv.ParseInt(cursor, 10, 64)
		if err != nil {
			return nil, config.WrapStoreError("get_versions", "mongodb", key,
				fmt.Errorf("invalid cursor %q: %w", cursor, err))
		}
		mongoFilter["version"] = bson.M{"$lt": cv}
	}

	findOpts := options.Find().SetSort(bson.D{{Key: "version", Value: -1}})
	limit := filter.Limit()
	if limit > 0 {
		findOpts.SetLimit(int64(limit + 1)) // +1 to detect a next page
	}

	cur, err := coll.Find(ctx, mongoFilter, findOpts)
	if err != nil {
		return nil, config.WrapStoreError("get_versions", "mongodb", key, err)
	}
	defer func() { _ = cur.Close(ctx) }()

	versions := make([]config.Value, 0)
	for cur.Next(ctx) {
		var v mongoVersion
		if err := cur.Decode(&v); err != nil {
			return nil, config.WrapStoreError("get_versions", "mongodb", key, err)
		}
		val, err := v.toValue(ctx)
		if err != nil {
			s.logger().Warn("mongodb: skipping corrupt version snapshot",
				"key", v.Key, "namespace", v.Namespace, "version", v.Version, "error", err)
			continue
		}
		versions = append(versions, val)
	}
	if err := cur.Err(); err != nil {
		return nil, config.WrapStoreError("get_versions", "mongodb", key, err)
	}

	var nextCursor string
	if limit > 0 && len(versions) > limit {
		nextCursor = strconv.FormatInt(versions[limit-1].Metadata().Version(), 10)
		versions = versions[:limit]
	}

	return config.NewVersionPage(versions, nextCursor, limit), nil
}
