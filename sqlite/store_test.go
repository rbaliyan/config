package sqlite_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/sqlite"
)

func newTestStore(t *testing.T) (*sqlite.Store, *sql.DB) {
	t.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open SQLite: %v", err)
	}

	store := sqlite.NewStore(db, sqlite.WithTable("config_entries_test"))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := store.Connect(ctx); err != nil {
		db.Close()
		t.Fatalf("Store connect failed: %v", err)
	}

	t.Cleanup(func() {
		store.Close(context.Background())
		db.Close()
	})

	return store, db
}

func TestSQLiteStore_BasicOperations(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

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
	if err := retrieved.Unmarshal(ctx, &intVal); err != nil {
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

func TestSQLiteStore_Find(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

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

	// Find with pagination
	page1, err := store.Find(ctx, "listtest", config.NewFilter().WithPrefix("app/").WithLimit(2).Build())
	if err != nil {
		t.Fatalf("Find (page 1) failed: %v", err)
	}
	if len(page1.Results()) != 2 {
		t.Errorf("Expected 2 results on page 1, got %d", len(page1.Results()))
	}
	if page1.NextCursor() == "" {
		t.Error("Expected non-empty cursor for page 1")
	}

	page2, err := store.Find(ctx, "listtest", config.NewFilter().WithPrefix("app/").WithLimit(2).WithCursor(page1.NextCursor()).Build())
	if err != nil {
		t.Fatalf("Find (page 2) failed: %v", err)
	}
	if len(page2.Results()) != 1 {
		t.Errorf("Expected 1 result on page 2, got %d", len(page2.Results()))
	}
}

func TestSQLiteStore_Watch(t *testing.T) {
	store, _ := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Start watching
	changes, err := store.Watch(ctx, config.WatchFilter{
		Namespaces: []string{"watchtest"},
	})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
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
}

func TestSQLiteStore_Health(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	if err := store.Health(ctx); err != nil {
		t.Errorf("Health check failed: %v", err)
	}
}

func TestSQLiteStore_Stats(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	// Set some values
	_, _ = store.Set(ctx, "statstest", "stats/key1", config.NewValue(1, config.WithValueType(config.TypeInt)))
	_, _ = store.Set(ctx, "statstest", "stats/key2", config.NewValue("hello", config.WithValueType(config.TypeString)))

	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}

	if stats.TotalEntries() != 2 {
		t.Errorf("Expected 2 entries, got %d", stats.TotalEntries())
	}

	if stats.CountForType(config.TypeInt) != 1 {
		t.Errorf("Expected 1 int entry, got %d", stats.CountForType(config.TypeInt))
	}

	if stats.CountForType(config.TypeString) != 1 {
		t.Errorf("Expected 1 string entry, got %d", stats.CountForType(config.TypeString))
	}

	if stats.CountForNamespace("statstest") != 2 {
		t.Errorf("Expected 2 entries in 'statstest' namespace, got %d", stats.CountForNamespace("statstest"))
	}
}

func TestSQLiteStore_TypesAndCodec(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

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

func TestSQLiteStore_GetMany(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	// Set values
	_, _ = store.Set(ctx, "bulktest", "key1", config.NewValue("value1"))
	_, _ = store.Set(ctx, "bulktest", "key2", config.NewValue("value2"))

	// GetMany
	results, err := store.GetMany(ctx, "bulktest", []string{"key1", "key2", "nonexistent"})
	if err != nil {
		t.Fatalf("GetMany failed: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}
}

func TestSQLiteStore_SetMany(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	values := map[string]config.Value{
		"bulk/key1": config.NewValue("value1"),
		"bulk/key2": config.NewValue("value2"),
	}

	if err := store.SetMany(ctx, "bulktest", values); err != nil {
		t.Fatalf("SetMany failed: %v", err)
	}

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

func TestSQLiteStore_SetMany_WithErrors(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	values := map[string]config.Value{
		"":          config.NewValue("invalid"), // Invalid key
		"valid/key": config.NewValue("valid"),
	}

	err := store.SetMany(ctx, "bulktest", values)
	if err == nil {
		t.Error("Expected error for empty key, got nil")
	}

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

func TestSQLiteStore_DeleteMany(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

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

func TestSQLiteStore_SecretValue(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	const ns, key = "secrettest", "creds/db_password"

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

	// Type must survive the round-trip.
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

// TestSQLiteStore_ExpiredEntryTreatedAsAbsent verifies write semantics
// align with read semantics for entries past their TTL.
func TestSQLiteStore_ExpiredEntryTreatedAsAbsent(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	// Seed with a TTL just in the future, then wait past it. SQLite's
	// datetime() function is second-resolution, so use a buffer of >1s.
	soonExpiry := time.Now().UTC().Add(1 * time.Second)
	val := config.NewValue("stale", config.WithValueExpiresAt(soonExpiry))
	if _, err := store.Set(ctx, "ns", "k", val); err != nil {
		t.Fatalf("seed Set: %v", err)
	}
	time.Sleep(2200 * time.Millisecond)

	if _, err := store.Get(ctx, "ns", "k"); !config.IsNotFound(err) {
		t.Errorf("Get on expired entry: want NotFound, got %v", err)
	}

	upd := config.NewValue("late", config.WithValueWriteMode(config.WriteModeUpdate))
	if _, err := store.Set(ctx, "ns", "k", upd); !config.IsNotFound(err) {
		t.Errorf("Update on expired entry: want NotFound, got %v", err)
	}

	create := config.NewValue("fresh", config.WithValueWriteMode(config.WriteModeCreate))
	if _, err := store.Set(ctx, "ns", "k", create); err != nil {
		t.Fatalf("Create over expired entry: %v", err)
	}

	got, err := store.Get(ctx, "ns", "k")
	if err != nil {
		t.Fatalf("Get after takeover: %v", err)
	}
	gs, _ := got.String()
	if gs != "fresh" {
		t.Errorf("post-takeover value = %q, want %q", gs, "fresh")
	}
}

// Compile-time interface checks
var (
	_ config.Store         = (*sqlite.Store)(nil)
	_ config.HealthChecker = (*sqlite.Store)(nil)
	_ config.StatsProvider = (*sqlite.Store)(nil)
	_ config.BulkStore     = (*sqlite.Store)(nil)
)
