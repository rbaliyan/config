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
		client.Disconnect(ctx)
		t.Skipf("Store connect failed: %v", err)
	}

	return store, client
}

func TestMongoDBStore_BasicOperations(t *testing.T) {
	store, client := skipIfNoMongo(t)
	ctx := context.Background()
	defer client.Disconnect(ctx)
	defer store.Close(ctx)

	// Set a value
	value := config.NewValue(42, config.WithValueType(config.TypeInt))

	_, err := store.Set(ctx, "test", "test/key", value)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Get the value
	retrieved, err := store.Get(ctx, "test", "test/key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Check the value
	var intVal int
	if err := retrieved.Unmarshal(&intVal); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if intVal != 42 {
		t.Errorf("Expected value 42, got %d", intVal)
	}

	// Check version
	meta := retrieved.Metadata()
	if meta == nil {
		t.Fatal("Expected metadata, got nil")
	}
	if meta.Version() != 1 {
		t.Errorf("Expected version 1, got %d", meta.Version())
	}

	// Update the value
	value2 := config.NewValue(100, config.WithValueType(config.TypeInt))
	_, err = store.Set(ctx, "test", "test/key", value2)
	if err != nil {
		t.Fatalf("Set (update) failed: %v", err)
	}

	// Verify updated version
	retrieved2, _ := store.Get(ctx, "test", "test/key")
	meta2 := retrieved2.Metadata()
	if meta2.Version() != 2 {
		t.Errorf("Expected version 2, got %d", meta2.Version())
	}

	// Delete the value
	if err := store.Delete(ctx, "test", "test/key"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify deletion
	_, err = store.Get(ctx, "test", "test/key")
	if !config.IsNotFound(err) {
		t.Errorf("Expected ErrNotFound, got %v", err)
	}
}

func TestMongoDBStore_Find(t *testing.T) {
	store, client := skipIfNoMongo(t)
	ctx := context.Background()
	defer client.Disconnect(ctx)
	defer store.Close(ctx)

	// Clean up any existing test data
	store.Delete(ctx, "listtest", "app/db/host")
	store.Delete(ctx, "listtest", "app/db/port")
	store.Delete(ctx, "listtest", "app/cache/ttl")

	// Set multiple values
	testData := []struct {
		key   string
		value any
		typ   config.Type
	}{
		{"app/db/host", "localhost", config.TypeString},
		{"app/db/port", 5432, config.TypeInt},
		{"app/cache/ttl", 300, config.TypeInt},
	}

	for _, d := range testData {
		val := config.NewValue(d.value, config.WithValueType(d.typ))
		if _, err := store.Set(ctx, "listtest", d.key, val); err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	// Find with prefix filter
	page, err := store.Find(ctx, "listtest", config.NewFilter().WithPrefix("app/db").Build())
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}

	results := page.Results()
	if len(results) != 2 {
		t.Errorf("Expected 2 entries with prefix 'app/db', got %d", len(results))
	}

	// Cleanup
	for _, d := range testData {
		store.Delete(ctx, "listtest", d.key)
	}
}

func TestMongoDBStore_Watch(t *testing.T) {
	store, client := skipIfNoMongo(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	defer client.Disconnect(ctx)
	defer store.Close(ctx)

	// Start watching
	changes, err := store.Watch(ctx, config.WatchFilter{
		Namespaces: []string{"watchtest"},
	})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}

	// Give the change stream time to initialize
	time.Sleep(100 * time.Millisecond)

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
	store.Delete(ctx, "watchtest", "watched/key")
}

func TestMongoDBStore_Health(t *testing.T) {
	store, client := skipIfNoMongo(t)
	ctx := context.Background()
	defer client.Disconnect(ctx)
	defer store.Close(ctx)

	if err := store.Health(ctx); err != nil {
		t.Errorf("Health check failed: %v", err)
	}
}

func TestMongoDBStore_Stats(t *testing.T) {
	store, client := skipIfNoMongo(t)
	ctx := context.Background()
	defer client.Disconnect(ctx)
	defer store.Close(ctx)

	// Set a value
	value := config.NewValue(1, config.WithValueType(config.TypeInt))
	_, _ = store.Set(ctx, "statstest", "stats/key", value)
	defer store.Delete(ctx, "statstest", "stats/key")

	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}

	if stats.TotalEntries < 1 {
		t.Errorf("Expected at least 1 entry, got %d", stats.TotalEntries)
	}
}

func TestMongoDBStore_TypesAndCodec(t *testing.T) {
	store, client := skipIfNoMongo(t)
	ctx := context.Background()
	defer client.Disconnect(ctx)
	defer store.Close(ctx)

	// Test various types
	tests := []struct {
		key   string
		value any
		typ   config.Type
	}{
		{"types/string", "hello", config.TypeString},
		{"types/int", 42, config.TypeInt},
		{"types/float", 3.14, config.TypeFloat},
		{"types/bool", true, config.TypeBool},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			val := config.NewValue(tt.value, config.WithValueType(tt.typ))
			if _, err := store.Set(ctx, "typetest", tt.key, val); err != nil {
				t.Fatalf("Set failed: %v", err)
			}
			defer store.Delete(ctx, "typetest", tt.key)

			retrieved, err := store.Get(ctx, "typetest", tt.key)
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}

			if retrieved.Type() != tt.typ {
				t.Errorf("Expected type %v, got %v", tt.typ, retrieved.Type())
			}
		})
	}
}

func TestMongoDBStore_GetMany(t *testing.T) {
	store, client := skipIfNoMongo(t)
	ctx := context.Background()
	defer client.Disconnect(ctx)
	defer store.Close(ctx)

	// Set values
	_, _ = store.Set(ctx, "bulktest", "key1", config.NewValue("value1"))
	_, _ = store.Set(ctx, "bulktest", "key2", config.NewValue("value2"))
	defer store.Delete(ctx, "bulktest", "key1")
	defer store.Delete(ctx, "bulktest", "key2")

	// GetMany
	results, err := store.GetMany(ctx, "bulktest", []string{"key1", "key2", "nonexistent"})
	if err != nil {
		t.Fatalf("GetMany failed: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}
}

func TestMongoDBStore_SetMany(t *testing.T) {
	store, client := skipIfNoMongo(t)
	ctx := context.Background()
	defer client.Disconnect(ctx)
	defer store.Close(ctx)

	values := map[string]config.Value{
		"bulk/key1": config.NewValue("value1"),
		"bulk/key2": config.NewValue("value2"),
	}

	if err := store.SetMany(ctx, "bulktest", values); err != nil {
		t.Fatalf("SetMany failed: %v", err)
	}
	defer store.Delete(ctx, "bulktest", "bulk/key1")
	defer store.Delete(ctx, "bulktest", "bulk/key2")

	// Verify
	val, err := store.Get(ctx, "bulktest", "bulk/key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	str, _ := val.String()
	if str != "value1" {
		t.Errorf("bulk/key1 = %q, want %q", str, "value1")
	}
}

func TestMongoDBStore_SetMany_WithErrors(t *testing.T) {
	store, client := skipIfNoMongo(t)
	ctx := context.Background()
	defer client.Disconnect(ctx)
	defer store.Close(ctx)

	values := map[string]config.Value{
		"":          config.NewValue("invalid"), // Invalid key
		"valid/key": config.NewValue("valid"),
	}

	err := store.SetMany(ctx, "bulktest", values)
	if err == nil {
		t.Error("Expected error for empty key, got nil")
	}
	defer store.Delete(ctx, "bulktest", "valid/key")

	// Valid key should still be set
	val, getErr := store.Get(ctx, "bulktest", "valid/key")
	if getErr != nil {
		t.Fatalf("Get failed for valid key: %v", getErr)
	}
	str, _ := val.String()
	if str != "valid" {
		t.Errorf("valid/key = %q, want %q", str, "valid")
	}
}

func TestMongoDBStore_DeleteMany(t *testing.T) {
	store, client := skipIfNoMongo(t)
	ctx := context.Background()
	defer client.Disconnect(ctx)
	defer store.Close(ctx)

	// Set values
	_, _ = store.Set(ctx, "bulktest", "del/key1", config.NewValue("value1"))
	_, _ = store.Set(ctx, "bulktest", "del/key2", config.NewValue("value2"))

	// DeleteMany
	deleted, err := store.DeleteMany(ctx, "bulktest", []string{"del/key1", "del/key2", "nonexistent"})
	if err != nil {
		t.Fatalf("DeleteMany failed: %v", err)
	}

	if deleted != 2 {
		t.Errorf("Deleted %d, want 2", deleted)
	}

	// Verify deleted
	_, err = store.Get(ctx, "bulktest", "del/key1")
	if !config.IsNotFound(err) {
		t.Error("del/key1 should be deleted")
	}
}

// Compile-time interface checks
var (
	_ config.Store         = (*mongodb.Store)(nil)
	_ config.HealthChecker = (*mongodb.Store)(nil)
	_ config.StatsProvider = (*mongodb.Store)(nil)
	_ config.BulkStore     = (*mongodb.Store)(nil)
)
