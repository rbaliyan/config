package postgres_test

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/lib/pq"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/postgres"
)

func getPostgresDSN() string {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://localhost:5432/config_test?sslmode=disable"
	}
	return dsn
}

func skipIfNoPostgres(t *testing.T) (*postgres.Store, *sql.DB, *pq.Listener) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dsn := getPostgresDSN()

	// Create database connection (app's responsibility)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Skipf("PostgreSQL not available: %v", err)
	}

	// Verify connection
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		t.Skipf("PostgreSQL not available: %v", err)
	}

	// Create listener for LISTEN/NOTIFY (app's responsibility)
	listener := pq.NewListener(dsn, 10*time.Second, time.Minute, func(ev pq.ListenerEventType, err error) {
		// Log errors but don't fail
	})

	// Create store with the db and listener
	store := postgres.NewStore(db, listener,
		postgres.WithTable("config_entries_test"),
		postgres.WithNotifyChannel("config_changes_test"),
	)

	if err := store.Connect(ctx); err != nil {
		listener.Close()
		db.Close()
		t.Skipf("Store connect failed: %v", err)
	}

	return store, db, listener
}

func TestPostgresStore_BasicOperations(t *testing.T) {
	store, db, listener := skipIfNoPostgres(t)
	ctx := context.Background()
	defer listener.Close()
	defer db.Close()
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

func TestPostgresStore_Find(t *testing.T) {
	store, db, listener := skipIfNoPostgres(t)
	ctx := context.Background()
	defer listener.Close()
	defer db.Close()
	defer store.Close(ctx)

	// Clean up any existing test data
	_ = store.Delete(ctx, "listtest", "app/db/host")
	_ = store.Delete(ctx, "listtest", "app/db/port")
	_ = store.Delete(ctx, "listtest", "app/cache/ttl")

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
		_ = store.Delete(ctx, "listtest", d.key)
	}
}

func TestPostgresStore_Watch(t *testing.T) {
	store, db, listener := skipIfNoPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	defer listener.Close()
	defer db.Close()
	defer store.Close(ctx)

	// Start watching
	changes, err := store.Watch(ctx, config.WatchFilter{
		Namespaces: []string{"watchtest"},
	})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}

	// Give the listener time to initialize
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
	_ = store.Delete(ctx, "watchtest", "watched/key")
}

func TestPostgresStore_Health(t *testing.T) {
	store, db, listener := skipIfNoPostgres(t)
	ctx := context.Background()
	defer listener.Close()
	defer db.Close()
	defer store.Close(ctx)

	if err := store.Health(ctx); err != nil {
		t.Errorf("Health check failed: %v", err)
	}
}

func TestPostgresStore_Stats(t *testing.T) {
	store, db, listener := skipIfNoPostgres(t)
	ctx := context.Background()
	defer listener.Close()
	defer db.Close()
	defer store.Close(ctx)

	// Set a value
	value := config.NewValue(1, config.WithValueType(config.TypeInt))
	_, _ = store.Set(ctx, "statstest", "stats/key", value)
	defer func() { _ = store.Delete(ctx, "statstest", "stats/key") }()

	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}

	if stats.TotalEntries < 1 {
		t.Errorf("Expected at least 1 entry, got %d", stats.TotalEntries)
	}
}

func TestPostgresStore_TypesAndCodec(t *testing.T) {
	store, db, listener := skipIfNoPostgres(t)
	ctx := context.Background()
	defer listener.Close()
	defer db.Close()
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
			defer func() { _ = store.Delete(ctx, "typetest", tt.key) }()

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

func TestPostgresStore_GetMany(t *testing.T) {
	store, db, listener := skipIfNoPostgres(t)
	ctx := context.Background()
	defer listener.Close()
	defer db.Close()
	defer store.Close(ctx)

	// Set values
	_, _ = store.Set(ctx, "bulktest", "key1", config.NewValue("value1"))
	_, _ = store.Set(ctx, "bulktest", "key2", config.NewValue("value2"))
	defer func() { _ = store.Delete(ctx, "bulktest", "key1") }()
	defer func() { _ = store.Delete(ctx, "bulktest", "key2") }()

	// GetMany
	results, err := store.GetMany(ctx, "bulktest", []string{"key1", "key2", "nonexistent"})
	if err != nil {
		t.Fatalf("GetMany failed: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}
}

func TestPostgresStore_SetMany(t *testing.T) {
	store, db, listener := skipIfNoPostgres(t)
	ctx := context.Background()
	defer listener.Close()
	defer db.Close()
	defer store.Close(ctx)

	values := map[string]config.Value{
		"bulk/key1": config.NewValue("value1"),
		"bulk/key2": config.NewValue("value2"),
	}

	if err := store.SetMany(ctx, "bulktest", values); err != nil {
		t.Fatalf("SetMany failed: %v", err)
	}
	defer func() { _ = store.Delete(ctx, "bulktest", "bulk/key1") }()
	defer func() { _ = store.Delete(ctx, "bulktest", "bulk/key2") }()

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

func TestPostgresStore_SetMany_WithErrors(t *testing.T) {
	store, db, listener := skipIfNoPostgres(t)
	ctx := context.Background()
	defer listener.Close()
	defer db.Close()
	defer store.Close(ctx)

	values := map[string]config.Value{
		"":          config.NewValue("invalid"), // Invalid key
		"valid/key": config.NewValue("valid"),
	}

	err := store.SetMany(ctx, "bulktest", values)
	if err == nil {
		t.Error("Expected error for empty key, got nil")
	}
	defer func() { _ = store.Delete(ctx, "bulktest", "valid/key") }()

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

func TestPostgresStore_DeleteMany(t *testing.T) {
	store, db, listener := skipIfNoPostgres(t)
	ctx := context.Background()
	defer listener.Close()
	defer db.Close()
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
	_ config.Store         = (*postgres.Store)(nil)
	_ config.HealthChecker = (*postgres.Store)(nil)
	_ config.StatsProvider = (*postgres.Store)(nil)
	_ config.BulkStore     = (*postgres.Store)(nil)
)
