package mongodb

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// VersionsCollectionName exposes the resolved versions collection name for
// tests that need to seed snapshots directly (e.g. orphan cleanup).
func (s *Store) VersionsCollectionName() string {
	return s.versionsCollectionName()
}

// InsertLegacyDocument inserts a document with []byte Value (legacy BinData format)
// for testing migration from old format. This bypasses the native BSON conversion.
func (s *Store) InsertLegacyDocument(ctx context.Context, namespace, key string, value []byte, codecName string) error {
	now := time.Now().UTC()

	// Delete any existing document first
	_, _ = s.collection.DeleteOne(ctx, bson.M{"namespace": namespace, "key": key})

	// Marshal value as BinData (the old format)
	t, val, err := bson.MarshalValue(value)
	if err != nil {
		return err
	}

	doc := mongoEntry{
		Key:       key,
		Namespace: namespace,
		Value:     bson.RawValue{Type: t, Value: val},
		Codec:     codecName,
		Type:      0, // TypeUnknown
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}

	_, err = s.collection.InsertOne(ctx, doc)
	return err
}
