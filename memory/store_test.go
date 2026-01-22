package memory

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/rbaliyan/config"
)

func TestNewStore(t *testing.T) {
	store := NewStore()
	if store == nil {
		t.Fatal("NewStore returned nil")
	}
}

func TestStore_ConnectClose(t *testing.T) {
	store := NewStore()
	ctx := context.Background()

	if err := store.Connect(ctx); err != nil {
		t.Errorf("Connect failed: %v", err)
	}

	if err := store.Close(ctx); err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestStore_SetGet(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	store.Connect(ctx)
	defer store.Close(ctx)

	// Set a value
	val := config.NewValue("test-value")
	if err := store.Set(ctx, "ns", "key", val); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Get the value
	got, err := store.Get(ctx, "ns", "key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	str, err := got.String()
	if err != nil {
		t.Fatalf("String conversion failed: %v", err)
	}
	if str != "test-value" {
		t.Errorf("Got %q, want %q", str, "test-value")
	}
}

func TestStore_GetNotFound(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	store.Connect(ctx)
	defer store.Close(ctx)

	_, err := store.Get(ctx, "ns", "nonexistent")
	if !config.IsNotFound(err) {
		t.Errorf("Expected NotFound error, got: %v", err)
	}
}

func TestStore_Delete(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	store.Connect(ctx)
	defer store.Close(ctx)

	// Set a value
	val := config.NewValue("to-delete")
	store.Set(ctx, "ns", "key", val)

	// Delete it
	if err := store.Delete(ctx, "ns", "key"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify it's gone
	_, err := store.Get(ctx, "ns", "key")
	if !config.IsNotFound(err) {
		t.Error("Expected NotFound after delete")
	}
}

func TestStore_DeleteNotFound(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	store.Connect(ctx)
	defer store.Close(ctx)

	err := store.Delete(ctx, "ns", "nonexistent")
	if !config.IsNotFound(err) {
		t.Errorf("Expected NotFound error, got: %v", err)
	}
}

func TestStore_SetWithTags(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	store.Connect(ctx)
	defer store.Close(ctx)

	tag1 := config.MustTag("env", "prod")
	tag2 := config.MustTag("region", "us")

	// Set with tags
	val := config.NewValue("tagged-value", config.WithValueTags([]config.Tag{tag1, tag2}))
	if err := store.Set(ctx, "ns", "key", val); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Get with same tags
	got, err := store.Get(ctx, "ns", "key", tag1, tag2)
	if err != nil {
		t.Fatalf("Get with tags failed: %v", err)
	}

	str, _ := got.String()
	if str != "tagged-value" {
		t.Errorf("Got %q, want %q", str, "tagged-value")
	}

	// Get without tags should fail
	_, err = store.Get(ctx, "ns", "key")
	if !config.IsNotFound(err) {
		t.Error("Expected NotFound when getting without tags")
	}
}

func TestStore_SameKeyDifferentTags(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	store.Connect(ctx)
	defer store.Close(ctx)

	prodTag := config.MustTag("env", "prod")
	devTag := config.MustTag("env", "dev")

	// Set same key with different tags
	store.Set(ctx, "ns", "db/host", config.NewValue("prod-db.example.com", config.WithValueTags([]config.Tag{prodTag})))
	store.Set(ctx, "ns", "db/host", config.NewValue("dev-db.example.com", config.WithValueTags([]config.Tag{devTag})))

	// Get prod value
	prodVal, err := store.Get(ctx, "ns", "db/host", prodTag)
	if err != nil {
		t.Fatalf("Get prod failed: %v", err)
	}
	prodStr, _ := prodVal.String()
	if prodStr != "prod-db.example.com" {
		t.Errorf("Prod value = %q, want %q", prodStr, "prod-db.example.com")
	}

	// Get dev value
	devVal, err := store.Get(ctx, "ns", "db/host", devTag)
	if err != nil {
		t.Fatalf("Get dev failed: %v", err)
	}
	devStr, _ := devVal.String()
	if devStr != "dev-db.example.com" {
		t.Errorf("Dev value = %q, want %q", devStr, "dev-db.example.com")
	}
}

func TestStore_Find(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	store.Connect(ctx)
	defer store.Close(ctx)

	// Set multiple values
	store.Set(ctx, "ns", "app/db/host", config.NewValue("localhost"))
	store.Set(ctx, "ns", "app/db/port", config.NewValue(5432))
	store.Set(ctx, "ns", "app/cache/ttl", config.NewValue(300))
	store.Set(ctx, "ns", "other/key", config.NewValue("other"))

	// Find with prefix
	page, err := store.Find(ctx, "ns", config.NewFilter().WithPrefix("app/db").Build())
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}

	results := page.Results()
	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}
}

func TestStore_FindWithLimit(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	store.Connect(ctx)
	defer store.Close(ctx)

	// Set 5 values
	for i := range 5 {
		store.Set(ctx, "ns", "key"+string(rune('a'+i)), config.NewValue(i))
	}

	// Find with limit
	page, err := store.Find(ctx, "ns", config.NewFilter().WithLimit(2).Build())
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}

	if len(page.Results()) != 2 {
		t.Errorf("Expected 2 results, got %d", len(page.Results()))
	}
	if page.NextCursor() == "" {
		t.Error("Expected non-empty cursor for pagination")
	}
}

func TestStore_FindWithTags(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	store.Connect(ctx)
	defer store.Close(ctx)

	prodTag := config.MustTag("env", "prod")
	devTag := config.MustTag("env", "dev")

	// Set values with different tags
	store.Set(ctx, "ns", "key1", config.NewValue("prod1", config.WithValueTags([]config.Tag{prodTag})))
	store.Set(ctx, "ns", "key2", config.NewValue("prod2", config.WithValueTags([]config.Tag{prodTag})))
	store.Set(ctx, "ns", "key3", config.NewValue("dev1", config.WithValueTags([]config.Tag{devTag})))

	// Find with prod tag
	page, err := store.Find(ctx, "ns", config.NewFilter().WithTags(prodTag).Build())
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}

	if len(page.Results()) != 2 {
		t.Errorf("Expected 2 prod results, got %d", len(page.Results()))
	}
}

func TestStore_Watch(t *testing.T) {
	store := NewStore()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	store.Connect(ctx)
	defer store.Close(ctx)

	// Start watching
	ch, err := store.Watch(ctx, config.WatchFilter{Namespaces: []string{"ns"}})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}

	// Set a value (triggers event)
	go func() {
		time.Sleep(50 * time.Millisecond)
		store.Set(ctx, "ns", "watch-key", config.NewValue("watch-value"))
	}()

	// Wait for event
	select {
	case event := <-ch:
		if event.Type != config.ChangeTypeSet {
			t.Errorf("Expected ChangeTypeSet, got %v", event.Type)
		}
		if event.Key != "watch-key" {
			t.Errorf("Expected key 'watch-key', got %s", event.Key)
		}
	case <-ctx.Done():
		t.Fatal("Timeout waiting for watch event")
	}
}

func TestStore_WatchDelete(t *testing.T) {
	store := NewStore()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	store.Connect(ctx)
	defer store.Close(ctx)

	// Set a value first
	store.Set(ctx, "ns", "delete-key", config.NewValue("to-delete"))

	// Wait for the Set notification to complete (it's async via goroutine)
	time.Sleep(10 * time.Millisecond)

	// Start watching
	ch, err := store.Watch(ctx, config.WatchFilter{Namespaces: []string{"ns"}})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}

	// Delete the value
	go func() {
		time.Sleep(50 * time.Millisecond)
		store.Delete(ctx, "ns", "delete-key")
	}()

	// Wait for event
	select {
	case event := <-ch:
		if event.Type != config.ChangeTypeDelete {
			t.Errorf("Expected ChangeTypeDelete, got %v", event.Type)
		}
	case <-ctx.Done():
		t.Fatal("Timeout waiting for delete event")
	}
}

func TestStore_NamespaceIsolation(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	store.Connect(ctx)
	defer store.Close(ctx)

	// Set same key in different namespaces
	store.Set(ctx, "ns1", "key", config.NewValue("value1"))
	store.Set(ctx, "ns2", "key", config.NewValue("value2"))

	// Get from ns1
	val1, err := store.Get(ctx, "ns1", "key")
	if err != nil {
		t.Fatalf("Get ns1 failed: %v", err)
	}
	str1, _ := val1.String()
	if str1 != "value1" {
		t.Errorf("ns1 value = %q, want %q", str1, "value1")
	}

	// Get from ns2
	val2, err := store.Get(ctx, "ns2", "key")
	if err != nil {
		t.Fatalf("Get ns2 failed: %v", err)
	}
	str2, _ := val2.String()
	if str2 != "value2" {
		t.Errorf("ns2 value = %q, want %q", str2, "value2")
	}
}

func TestStore_Health(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	store.Connect(ctx)
	defer store.Close(ctx)

	if err := store.Health(ctx); err != nil {
		t.Errorf("Health check failed: %v", err)
	}
}

func TestStore_Stats(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	store.Connect(ctx)
	defer store.Close(ctx)

	// Set some values
	store.Set(ctx, "ns1", "key1", config.NewValue("value1"))
	store.Set(ctx, "ns1", "key2", config.NewValue(42))
	store.Set(ctx, "ns2", "key1", config.NewValue(true))

	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}

	if stats.TotalEntries != 3 {
		t.Errorf("TotalEntries = %d, want 3", stats.TotalEntries)
	}
}

func TestStore_GetMany(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	store.Connect(ctx)
	defer store.Close(ctx)

	// Set values
	store.Set(ctx, "ns", "key1", config.NewValue("value1"))
	store.Set(ctx, "ns", "key2", config.NewValue("value2"))

	// GetMany
	results, err := store.GetMany(ctx, "ns", []string{"key1", "key2", "nonexistent"})
	if err != nil {
		t.Fatalf("GetMany failed: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}
}

func TestStore_SetMany(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	store.Connect(ctx)
	defer store.Close(ctx)

	values := map[string]config.Value{
		"key1": config.NewValue("value1"),
		"key2": config.NewValue("value2"),
	}

	if err := store.SetMany(ctx, "ns", values); err != nil {
		t.Fatalf("SetMany failed: %v", err)
	}

	// Verify
	val, _ := store.Get(ctx, "ns", "key1")
	str, _ := val.String()
	if str != "value1" {
		t.Errorf("key1 = %q, want %q", str, "value1")
	}
}

func TestStore_DeleteMany(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	store.Connect(ctx)
	defer store.Close(ctx)

	// Set values
	store.Set(ctx, "ns", "key1", config.NewValue("value1"))
	store.Set(ctx, "ns", "key2", config.NewValue("value2"))

	// DeleteMany
	deleted, err := store.DeleteMany(ctx, "ns", []string{"key1", "key2", "nonexistent"})
	if err != nil {
		t.Fatalf("DeleteMany failed: %v", err)
	}

	if deleted != 2 {
		t.Errorf("Deleted %d, want 2", deleted)
	}

	// Verify deleted
	_, err = store.Get(ctx, "ns", "key1")
	if !config.IsNotFound(err) {
		t.Error("key1 should be deleted")
	}
}

func TestStore_ConcurrentAccess(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	store.Connect(ctx)
	defer store.Close(ctx)

	var wg sync.WaitGroup
	errors := make(chan error, 100)

	// Concurrent writes
	for i := range 50 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := "key" + string(rune('0'+i%10))
			if err := store.Set(ctx, "ns", key, config.NewValue(i)); err != nil {
				errors <- err
			}
		}(i)
	}

	// Concurrent reads
	for i := range 50 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := "key" + string(rune('0'+i%10))
			_, _ = store.Get(ctx, "ns", key) // Ignore not found errors
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("Concurrent operation error: %v", err)
	}
}

func TestStore_ValueMetadata(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	store.Connect(ctx)
	defer store.Close(ctx)

	// Set a value
	store.Set(ctx, "ns", "key", config.NewValue("original"))

	// Get and check initial version
	val1, _ := store.Get(ctx, "ns", "key")
	if val1.Metadata().Version() != 1 {
		t.Errorf("Initial version = %d, want 1", val1.Metadata().Version())
	}

	// Update
	store.Set(ctx, "ns", "key", config.NewValue("updated"))

	// Check incremented version
	val2, _ := store.Get(ctx, "ns", "key")
	if val2.Metadata().Version() != 2 {
		t.Errorf("Updated version = %d, want 2", val2.Metadata().Version())
	}

	// Check timestamps
	if val2.Metadata().UpdatedAt().Before(val2.Metadata().CreatedAt()) {
		t.Error("UpdatedAt should not be before CreatedAt")
	}
}

// Compile-time interface checks
var (
	_ config.Store         = (*Store)(nil)
	_ config.HealthChecker = (*Store)(nil)
	_ config.StatsProvider = (*Store)(nil)
	_ config.BulkStore     = (*Store)(nil)
)
