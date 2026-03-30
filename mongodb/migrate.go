package mongodb

import (
	"context"
	"fmt"
	"math"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/codec"
)

// MigrateResult contains the results of a migration operation.
type MigrateResult struct {
	Total    int64 // documents examined
	Migrated int64 // documents updated
	Skipped  int64 // already in target format
	Errors   int64 // failed documents
}

// MigrateOption configures the migration.
type MigrateOption func(*migrateOptions)

type migrateOptions struct {
	targetCodec string // "" = keep existing codec, just re-encode for new storage format
	namespace   string // "" = all namespaces
	prefix      string // "" = all keys
	dryRun      bool
	batchSize   int // default 100
}

// WithTargetCodec sets the codec to convert values to.
// If empty (default), values are re-encoded with their existing codec
// but stored in the new native format.
func WithTargetCodec(name string) MigrateOption {
	return func(o *migrateOptions) {
		o.targetCodec = name
	}
}

// WithMigrateNamespace filters migration to a specific namespace.
func WithMigrateNamespace(ns string) MigrateOption {
	return func(o *migrateOptions) {
		o.namespace = ns
	}
}

// WithMigratePrefix filters migration to keys with a specific prefix.
func WithMigratePrefix(p string) MigrateOption {
	return func(o *migrateOptions) {
		o.prefix = p
	}
}

// WithDryRun reports what would be migrated without making changes.
func WithDryRun() MigrateOption {
	return func(o *migrateOptions) {
		o.dryRun = true
	}
}

// WithBatchSize sets the cursor batch size for the migration query.
func WithBatchSize(n int) MigrateOption {
	return func(o *migrateOptions) {
		if n > 0 {
			o.batchSize = n
		}
	}
}

// Migrate re-encodes documents in the collection to the native BSON storage format.
//
// Existing MongoDB documents store values as BinData (legacy format). This method
// reads each document, decodes the value using its stored codec, optionally
// re-encodes with a target codec, and writes back as a native bson.RawValue.
//
// Migration is not required for the store to function — the read path handles
// both legacy and new formats. Migration is an optimization to make values
// readable in MongoDB shell/Compass.
//
// Safe to run on a live system — uses FindOneAndUpdate per document.
func (s *Store) Migrate(ctx context.Context, opts ...MigrateOption) (*MigrateResult, error) {
	if s.closed.Load() {
		return nil, config.ErrStoreClosed
	}

	o := &migrateOptions{batchSize: 100}
	for _, opt := range opts {
		opt(o)
	}

	// Build filter
	filter := bson.M{}
	if o.namespace != "" {
		filter["namespace"] = o.namespace
	}
	if o.prefix != "" {
		filter["key"] = bson.M{"$regex": bson.Regex{Pattern: "^" + escapeRegex(o.prefix)}}
	}

	batchSize := o.batchSize
	if batchSize > math.MaxInt32 {
		batchSize = math.MaxInt32
	}
	findOpts := options.Find().
		SetBatchSize(int32(batchSize)).
		SetSort(bson.D{{Key: "_id", Value: 1}})

	cursor, err := s.collection.Find(ctx, filter, findOpts)
	if err != nil {
		return nil, fmt.Errorf("migrate find: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	result := &MigrateResult{}

	for cursor.Next(ctx) {
		result.Total++

		var doc mongoEntry
		if err := cursor.Decode(&doc); err != nil {
			s.logger().Warn("migrate: failed to decode document", "error", err)
			result.Errors++
			continue
		}

		// Only migrate legacy BinData format
		if doc.Value.Type != bson.TypeBinary {
			result.Skipped++
			continue
		}

		if o.dryRun {
			result.Migrated++
			continue
		}

		if err := s.migrateDocument(ctx, doc, o); err != nil {
			s.logger().Warn("migrate: failed to update document",
				"key", doc.Key, "namespace", doc.Namespace, "error", err)
			result.Errors++
			continue
		}

		result.Migrated++
	}

	if err := cursor.Err(); err != nil {
		return result, fmt.Errorf("migrate cursor: %w", err)
	}

	return result, nil
}

func (s *Store) migrateDocument(ctx context.Context, doc mongoEntry, o *migrateOptions) error {
	// Extract raw bytes from BinData
	var rawBytes []byte
	if err := doc.Value.Unmarshal(&rawBytes); err != nil {
		return fmt.Errorf("unmarshal bindata: %w", err)
	}

	// Determine source and target codecs
	sourceCodecName := doc.Codec
	targetCodecName := sourceCodecName
	if o.targetCodec != "" {
		targetCodecName = o.targetCodec
	}

	sourceCodec := codec.Get(sourceCodecName)
	if sourceCodec == nil {
		return fmt.Errorf("source codec %q not found", sourceCodecName)
	}

	// Decode the value using the source codec
	var decoded any
	if err := sourceCodec.Decode(rawBytes, &decoded); err != nil {
		return fmt.Errorf("decode with %q: %w", sourceCodecName, err)
	}

	// Re-encode with the target codec
	targetCodec := codec.Get(targetCodecName)
	if targetCodec == nil {
		return fmt.Errorf("target codec %q not found", targetCodecName)
	}

	encoded, err := targetCodec.Encode(decoded)
	if err != nil {
		return fmt.Errorf("encode with %q: %w", targetCodecName, err)
	}

	// Convert to native bson.RawValue
	bsonVal, err := toBSONValue(targetCodec, encoded)
	if err != nil {
		return fmt.Errorf("convert to BSON for %q: %w", targetCodecName, err)
	}

	// Update document
	updateFilter := bson.M{"_id": doc.ID}
	update := bson.M{
		"$set": bson.M{
			"value": bsonVal,
			"codec": targetCodecName,
		},
		"$inc": bson.M{"version": int64(1)},
	}

	updateResult, err := s.collection.UpdateOne(ctx, updateFilter, update)
	if err != nil {
		return fmt.Errorf("update: %w", err)
	}
	if updateResult.MatchedCount == 0 {
		return fmt.Errorf("document not found (deleted during migration?)")
	}

	return nil
}
