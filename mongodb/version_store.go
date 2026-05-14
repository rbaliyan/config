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

// reportVersionError logs a versioning failure and invokes the configured
// callback when present. Centralizing this keeps the slog payload identical
// to what the hook sees.
func (s *Store) reportVersionError(op, namespace, key string, version int64, err error) {
	s.logger().Warn("mongodb: version operation failed",
		"op", op,
		"namespace", namespace,
		"key", key,
		"version", version,
		"error", err,
	)
	if cb := s.cfg.OnVersionError; cb != nil {
		cb(op, namespace, key, version, err)
	}
}

// writeVersionSnapshot inserts a snapshot of doc into the versions collection
// and trims the history to MaxHistory when configured. Best-effort: failures
// are reported via [Store.reportVersionError] but do not propagate to the
// caller, since the source-of-truth entry has already been written.
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
		// Duplicate-key on (ns, key, version) means the snapshot was already
		// written by a concurrent path (e.g. SetMany's per-key fallback).
		// Idempotent: not an error, but skip the trim since the other writer
		// will run it.
		if mongo.IsDuplicateKeyError(err) {
			return
		}
		s.reportVersionError("snapshot", doc.Namespace, doc.Key, doc.Version, err)
		return
	}
	if s.cfg.MaxHistory > 0 {
		s.trimVersions(ctx, doc.Namespace, doc.Key)
	}
}

// trimVersions deletes snapshots older than the MaxHistory+1 most recent
// versions for the given key. Two round-trips: one findOne to locate the
// cutoff version (the (MaxHistory+1)-th newest), one DeleteMany($lte) to
// drop everything at or below it. Best-effort.
func (s *Store) trimVersions(ctx context.Context, namespace, key string) {
	coll := s.versionsCollection()
	if coll == nil || s.cfg.MaxHistory <= 0 {
		return
	}

	// Find the cutoff: the first document we want to *drop* in descending
	// version order, after skipping the (MaxHistory+1) we keep.
	findOpts := options.FindOne().
		SetSort(bson.D{{Key: "version", Value: -1}}).
		SetSkip(int64(s.cfg.MaxHistory + 1)).
		SetProjection(bson.M{"version": 1})

	var cutoff struct {
		Version int64 `bson:"version"`
	}
	err := coll.FindOne(ctx, bson.M{"namespace": namespace, "key": key}, findOpts).Decode(&cutoff)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return // history already within limit
	}
	if err != nil {
		s.reportVersionError("trim", namespace, key, 0, err)
		return
	}
	delFilter := bson.M{
		"namespace": namespace,
		"key":       key,
		"version":   bson.M{"$lte": cutoff.Version},
	}
	if _, err := coll.DeleteMany(ctx, delFilter); err != nil {
		s.reportVersionError("trim", namespace, key, cutoff.Version, err)
	}
}

// deleteAllVersions removes every snapshot for the given keys. Best-effort:
// a failure here only leaves orphan snapshots — GetVersions filters them out
// by checking the live entry first. Use [Store.CleanupOrphans] to reclaim
// the storage.
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
		// Report the failure once per call (not once per key) — the keys
		// are visible via the hook's key argument only when there is a
		// single one, which is the Delete-path case worth tracking.
		k := ""
		if len(keys) == 1 {
			k = keys[0]
		}
		s.reportVersionError("delete", namespace, k, 0, err)
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

// CleanupOrphans deletes version snapshots whose live entry no longer
// exists. Orphans accumulate when [Store.Delete] or [Store.DeleteMany]
// succeeds on the entries collection but the matching snapshot cleanup
// fails (network blip, write concern violation). Snapshots tied to a TTL
// expiry are reclaimed automatically by the sparse TTL index; this method
// covers the non-TTL case.
//
// The implementation walks unique (namespace, key) pairs in the versions
// collection and emits a $lookup against the entries collection to
// identify orphans, then drops them with a single DeleteMany. Returns the
// number of snapshot documents removed.
//
// Returns ErrVersioningNotSupported when versioning is disabled.
func (s *Store) CleanupOrphans(ctx context.Context) (int64, error) {
	if s.closed.Load() {
		return 0, config.ErrStoreClosed
	}
	if !s.cfg.EnableVersioning {
		return 0, config.ErrVersioningNotSupported
	}
	coll := s.versionsCollection()
	if coll == nil {
		return 0, config.ErrVersioningNotSupported
	}

	pipeline := mongo.Pipeline{
		{{Key: "$group", Value: bson.M{
			"_id": bson.M{"namespace": "$namespace", "key": "$key"},
		}}},
		{{Key: "$lookup", Value: bson.M{
			"from": s.cfg.Collection,
			"let":  bson.M{"ns": "$_id.namespace", "k": "$_id.key"},
			"pipeline": bson.A{
				bson.M{"$match": bson.M{"$expr": bson.M{"$and": bson.A{
					bson.M{"$eq": bson.A{"$namespace", "$$ns"}},
					bson.M{"$eq": bson.A{"$key", "$$k"}},
				}}}},
				bson.M{"$limit": 1},
				bson.M{"$project": bson.M{"_id": 1}},
			},
			"as": "entry",
		}}},
		{{Key: "$match", Value: bson.M{"entry": bson.M{"$size": 0}}}},
	}

	cur, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		s.reportVersionError("cleanup", "", "", 0, err)
		return 0, config.WrapStoreError("cleanup_orphans", "mongodb", "", err)
	}
	defer func() { _ = cur.Close(ctx) }()

	// Collect orphan (namespace, key) tuples and delete in batches to keep
	// any single DeleteMany filter bounded.
	type orphan struct {
		Namespace string
		Key       string
	}
	var batch []orphan
	const batchSize = 500
	var deleted int64

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		ors := make(bson.A, 0, len(batch))
		for _, o := range batch {
			ors = append(ors, bson.M{"namespace": o.Namespace, "key": o.Key})
		}
		res, err := coll.DeleteMany(ctx, bson.M{"$or": ors})
		if err != nil {
			s.reportVersionError("cleanup", "", "", 0, err)
			return err
		}
		deleted += res.DeletedCount
		batch = batch[:0]
		return nil
	}

	for cur.Next(ctx) {
		var doc struct {
			ID struct {
				Namespace string `bson:"namespace"`
				Key       string `bson:"key"`
			} `bson:"_id"`
		}
		if err := cur.Decode(&doc); err != nil {
			s.reportVersionError("cleanup", "", "", 0, err)
			return deleted, config.WrapStoreError("cleanup_orphans", "mongodb", "", err)
		}
		batch = append(batch, orphan{Namespace: doc.ID.Namespace, Key: doc.ID.Key})
		if len(batch) >= batchSize {
			if err := flush(); err != nil {
				return deleted, config.WrapStoreError("cleanup_orphans", "mongodb", "", err)
			}
		}
	}
	if err := cur.Err(); err != nil {
		s.reportVersionError("cleanup", "", "", 0, err)
		return deleted, config.WrapStoreError("cleanup_orphans", "mongodb", "", err)
	}
	if err := flush(); err != nil {
		return deleted, config.WrapStoreError("cleanup_orphans", "mongodb", "", err)
	}
	return deleted, nil
}
