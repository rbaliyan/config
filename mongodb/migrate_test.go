package mongodb_test

import (
	"context"
	"testing"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/mongodb"
)

func TestMigrate_DryRun(t *testing.T) {
	store, client := skipIfNoMongo(t)
	defer func() {
		ctx := context.Background()
		_ = store.Close(ctx)
		_ = client.Disconnect(ctx)
	}()

	ctx := context.Background()
	cleanCollection(t, store, ctx)

	// Insert a legacy BinData document directly
	if err := store.InsertLegacyDocument(ctx, "test-ns", "dry-run-key", []byte(`"hello"`), "json"); err != nil {
		t.Fatalf("InsertLegacyDocument: %v", err)
	}

	result, err := store.Migrate(ctx, mongodb.WithDryRun())
	if err != nil {
		t.Fatalf("Migrate dry run: %v", err)
	}

	if result.Total != 1 {
		t.Errorf("expected Total=1, got %d", result.Total)
	}
	if result.Migrated != 1 {
		t.Errorf("expected Migrated=1, got %d", result.Migrated)
	}

	// Verify the value is still readable after dry run (not modified)
	val, err := store.Get(ctx, "test-ns", "dry-run-key")
	if err != nil {
		t.Fatalf("Get after dry run: %v", err)
	}
	s, err := val.String()
	if err != nil {
		t.Fatalf("String: %v", err)
	}
	if s != "hello" {
		t.Errorf("expected 'hello', got %q", s)
	}
}

func TestMigrate_JSONToNative(t *testing.T) {
	store, client := skipIfNoMongo(t)
	defer func() {
		ctx := context.Background()
		_ = store.Close(ctx)
		_ = client.Disconnect(ctx)
	}()

	ctx := context.Background()
	cleanCollection(t, store, ctx)

	// Insert a legacy BinData document
	if err := store.InsertLegacyDocument(ctx, "test-ns", "json-key", []byte(`42`), "json"); err != nil {
		t.Fatalf("InsertLegacyDocument: %v", err)
	}

	// Migrate
	result, err := store.Migrate(ctx)
	if err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	if result.Migrated != 1 {
		t.Errorf("expected Migrated=1, got %d", result.Migrated)
	}

	// Verify the value is still readable
	val, err := store.Get(ctx, "test-ns", "json-key")
	if err != nil {
		t.Fatalf("Get after migration: %v", err)
	}
	i, err := val.Int64()
	if err != nil {
		t.Fatalf("Int64: %v", err)
	}
	if i != 42 {
		t.Errorf("expected 42, got %d", i)
	}

	// Run migration again — should skip already-migrated docs
	result2, err := store.Migrate(ctx)
	if err != nil {
		t.Fatalf("Migrate second run: %v", err)
	}
	if result2.Skipped != 1 {
		t.Errorf("expected Skipped=1 on second run, got %d", result2.Skipped)
	}
	if result2.Migrated != 0 {
		t.Errorf("expected Migrated=0 on second run, got %d", result2.Migrated)
	}
}

func TestMigrate_WithNamespaceFilter(t *testing.T) {
	store, client := skipIfNoMongo(t)
	defer func() {
		ctx := context.Background()
		_ = store.Close(ctx)
		_ = client.Disconnect(ctx)
	}()

	ctx := context.Background()
	cleanCollection(t, store, ctx)

	// Insert legacy docs in two namespaces
	if err := store.InsertLegacyDocument(ctx, "ns1", "key1", []byte(`"a"`), "json"); err != nil {
		t.Fatalf("InsertLegacyDocument: %v", err)
	}
	if err := store.InsertLegacyDocument(ctx, "ns2", "key2", []byte(`"b"`), "json"); err != nil {
		t.Fatalf("InsertLegacyDocument: %v", err)
	}

	// Migrate only ns1
	result, err := store.Migrate(ctx, mongodb.WithMigrateNamespace("ns1"))
	if err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	if result.Total != 1 {
		t.Errorf("expected Total=1, got %d", result.Total)
	}
	if result.Migrated != 1 {
		t.Errorf("expected Migrated=1, got %d", result.Migrated)
	}
}

func TestMigrate_JSONToBSON(t *testing.T) {
	store, client := skipIfNoMongo(t)
	defer func() {
		ctx := context.Background()
		_ = store.Close(ctx)
		_ = client.Disconnect(ctx)
	}()

	ctx := context.Background()
	cleanCollection(t, store, ctx)

	// Insert a legacy JSON document
	if err := store.InsertLegacyDocument(ctx, "test-ns", "convert-key", []byte(`99`), "json"); err != nil {
		t.Fatalf("InsertLegacyDocument: %v", err)
	}

	// Register bson codec by referencing it
	_ = mongodb.BSON()

	// Migrate to BSON codec
	result, err := store.Migrate(ctx, mongodb.WithTargetCodec("bson"))
	if err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	if result.Migrated != 1 {
		t.Errorf("expected Migrated=1, got %d", result.Migrated)
	}

	// Verify the value is still readable
	val, err := store.Get(ctx, "test-ns", "convert-key")
	if err != nil {
		t.Fatalf("Get after migration: %v", err)
	}
	i, err := val.Int64()
	if err != nil {
		t.Fatalf("Int64: %v", err)
	}
	if i != 99 {
		t.Errorf("expected 99, got %d", i)
	}
}

// cleanCollection removes all documents by finding and deleting each key.
func cleanCollection(t *testing.T, store *mongodb.Store, ctx context.Context) {
	t.Helper()
	for _, ns := range []string{"test-ns", "ns1", "ns2"} {
		page, _ := store.Find(ctx, ns, config.NewFilter().WithPrefix("").Build())
		if page != nil {
			for key := range page.Results() {
				_ = store.Delete(ctx, ns, key)
			}
		}
	}
}
