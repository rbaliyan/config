package mongodb_test

import (
	"context"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/mongodb"
)

// The universal [config.Store] / [config.BulkStore] contract is exercised by
// the shared suites in store_conformance_test.go; the
// [config.VersionedStore] contract by the same suite against a
// WithVersioning(true) factory. This file keeps the MongoDB-specific
// tests: Watch (change-stream timing) and SecretValue (deeper than the
// conformance suite: SecretFrom recovery via BSON BinData, version
// increment).

func getMongoURI() string {
	uri := os.Getenv("MONGO_URI")
	if uri == "" {
		uri = "mongodb://localhost:27017"
	}
	return uri
}

func skipIfNoMongo(t *testing.T) (*mongodb.Store, *mongo.Client) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create MongoDB client (app's responsibility)
	client, err := mongo.Connect(options.Client().ApplyURI(getMongoURI()))
	if err != nil {
		t.Skipf("MongoDB not available: %v", err)
	}

	// Verify connection
	if err := client.Ping(ctx, nil); err != nil {
		t.Skipf("MongoDB not available: %v", err)
	}

	// Create store with the client
	store := mongodb.NewStore(client,
		mongodb.WithDatabase("config_test"),
		mongodb.WithCollection("entries_test"),
	)

	if err := store.Connect(ctx); err != nil {
		_ = client.Disconnect(ctx)
		t.Skipf("Store connect failed: %v", err)
	}

	return store, client
}

func TestMongoDBStore_Watch(t *testing.T) {
	store, client := skipIfNoMongo(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	defer func() { _ = client.Disconnect(ctx) }()
	defer func() { _ = store.Close(ctx) }()

	// Start watching
	changes, err := store.Watch(ctx, config.WatchFilter{
		Namespaces: []string{"watchtest"},
	})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}

	// Wait until the change stream is actually live by probing with
	// throwaway writes and draining their events, rather than sleeping a
	// fixed amount and hoping the stream registered in time.
	if !mongoWatchReady(ctx, t, store, changes, "watchtest") {
		t.Fatal("change stream never became ready")
	}

	// Set a value
	value := config.NewValue("test", config.WithValueType(config.TypeString))
	if _, err := store.Set(ctx, "watchtest", "watched/key", value); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Wait for change event
	select {
	case event := <-changes:
		if event.Type != config.ChangeTypeSet {
			t.Errorf("Expected ChangeTypeSet, got %v", event.Type)
		}
		if event.Key != "watched/key" {
			t.Errorf("Expected key 'watched/key', got %s", event.Key)
		}
	case <-ctx.Done():
		t.Fatal("Timeout waiting for change event")
	}

	// Cleanup
	_ = store.Delete(ctx, "watchtest", "watched/key")
}

// TestMongoDBStore_SecretValue goes deeper than the shared conformance
// suite's SecretValue subtest: MongoDB's Get returns a *val type (unlike
// memory's metadataValue wrapper), so SecretFrom can recover the original
// bytes after a round-trip through BSON BinData. This test pins that
// round-trip AND the version-increment behavior on rotation.
func TestMongoDBStore_SecretValue(t *testing.T) {
	store, client := skipIfNoMongo(t)
	ctx := context.Background()
	defer func() { _ = client.Disconnect(ctx) }()
	defer func() { _ = store.Close(ctx) }()

	const ns, key = "secrettest", "creds/db_password"
	defer func() { _ = store.Delete(ctx, ns, key) }()

	original := config.NewSecret("super-secret-pw")
	written, err := store.Set(ctx, ns, key, config.NewSecretValue(original))
	if err != nil {
		t.Fatalf("Set: %v", err)
	}
	if written.Type() != config.TypeSecret {
		t.Errorf("Set returned type %v, want TypeSecret", written.Type())
	}

	retrieved, err := store.Get(ctx, ns, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	// Type must survive the round-trip through BSON BinData.
	if retrieved.Type() != config.TypeSecret {
		t.Errorf("retrieved type = %v, want TypeSecret", retrieved.Type())
	}

	// String() must always mask — plaintext must never leak.
	str, err := retrieved.String()
	if err != nil {
		t.Fatalf("String(): %v", err)
	}
	if str != "******" {
		t.Errorf("String() = %q, want \"******\" (plaintext leaked)", str)
	}

	// SecretFrom must recover the original bytes.
	got, err := config.SecretFrom(ctx, retrieved)
	if err != nil {
		t.Fatalf("SecretFrom: %v", err)
	}
	defer got.Wipe()
	if !original.Equal(got) {
		t.Errorf("SecretFrom bytes mismatch after round-trip: got %q", got.Bytes())
	}

	// Version increments on update.
	if _, err := store.Set(ctx, ns, key, config.NewSecretValue(config.NewSecret("rotated-secret"))); err != nil {
		t.Fatalf("update Set: %v", err)
	}
	updated, err := store.Get(ctx, ns, key)
	if err != nil {
		t.Fatalf("Get after update: %v", err)
	}
	if updated.Metadata().Version() != 2 {
		t.Errorf("version after update = %d, want 2", updated.Metadata().Version())
	}
	// Updated value must also be masked.
	if s, _ := updated.String(); s != "******" {
		t.Errorf("updated String() = %q, want \"******\"", s)
	}
}

// Compile-time interface checks
var (
	_ config.Store          = (*mongodb.Store)(nil)
	_ config.HealthChecker  = (*mongodb.Store)(nil)
	_ config.StatsProvider  = (*mongodb.Store)(nil)
	_ config.BulkStore      = (*mongodb.Store)(nil)
	_ config.VersionedStore = (*mongodb.Store)(nil)
)
