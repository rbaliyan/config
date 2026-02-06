package memory

import (
	"context"
	"errors"
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
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	// Set a value
	val := config.NewValue("test-value")
	if _, err := store.Set(ctx, "ns", "key", val); err != nil {
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
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	_, err := store.Get(ctx, "ns", "nonexistent")
	if !config.IsNotFound(err) {
		t.Errorf("Expected NotFound error, got: %v", err)
	}
}

func TestStore_Delete(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	// Set a value
	val := config.NewValue("to-delete")
	_, _ = store.Set(ctx, "ns", "key", val)

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
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	err := store.Delete(ctx, "ns", "nonexistent")
	if !config.IsNotFound(err) {
		t.Errorf("Expected NotFound error, got: %v", err)
	}
}

func TestStore_Find(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	// Set multiple values
	_, _ = store.Set(ctx, "ns", "app/db/host", config.NewValue("localhost"))
	_, _ = store.Set(ctx, "ns", "app/db/port", config.NewValue(5432))
	_, _ = store.Set(ctx, "ns", "app/cache/ttl", config.NewValue(300))
	_, _ = store.Set(ctx, "ns", "other/key", config.NewValue("other"))

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
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	// Set 5 values
	for i := range 5 {
		_, _ = store.Set(ctx, "ns", "key"+string(rune('a'+i)), config.NewValue(i))
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

func TestStore_Watch(t *testing.T) {
	store := NewStore()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	// Start watching
	ch, err := store.Watch(ctx, config.WatchFilter{Namespaces: []string{"ns"}})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}

	// Set a value (triggers event)
	go func() {
		time.Sleep(50 * time.Millisecond)
		_, _ = store.Set(ctx, "ns", "watch-key", config.NewValue("watch-value"))
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
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	// Set a value first
	_, _ = store.Set(ctx, "ns", "delete-key", config.NewValue("to-delete"))

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
		_ = store.Delete(ctx, "ns", "delete-key")
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
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	// Set same key in different namespaces
	_, _ = store.Set(ctx, "ns1", "key", config.NewValue("value1"))
	_, _ = store.Set(ctx, "ns2", "key", config.NewValue("value2"))

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
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	if err := store.Health(ctx); err != nil {
		t.Errorf("Health check failed: %v", err)
	}
}

func TestStore_Stats(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	// Set some values
	_, _ = store.Set(ctx, "ns1", "key1", config.NewValue("value1"))
	_, _ = store.Set(ctx, "ns1", "key2", config.NewValue(42))
	_, _ = store.Set(ctx, "ns2", "key1", config.NewValue(true))

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
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	// Set values
	_, _ = store.Set(ctx, "ns", "key1", config.NewValue("value1"))
	_, _ = store.Set(ctx, "ns", "key2", config.NewValue("value2"))

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
	_ = store.Connect(ctx)
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
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	// Set values
	_, _ = store.Set(ctx, "ns", "key1", config.NewValue("value1"))
	_, _ = store.Set(ctx, "ns", "key2", config.NewValue("value2"))

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
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	var wg sync.WaitGroup
	errors := make(chan error, 100)

	// Concurrent writes
	for i := range 50 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := "key" + string(rune('0'+i%10))
			if _, err := store.Set(ctx, "ns", key, config.NewValue(i)); err != nil {
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
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	// Set a value
	_, _ = store.Set(ctx, "ns", "key", config.NewValue("original"))

	// Get and check initial version
	val1, _ := store.Get(ctx, "ns", "key")
	if val1.Metadata().Version() != 1 {
		t.Errorf("Initial version = %d, want 1", val1.Metadata().Version())
	}

	// Update
	_, _ = store.Set(ctx, "ns", "key", config.NewValue("updated"))

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

func TestStore_SetWithIfNotExists(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	// First set should succeed (key doesn't exist)
	val := config.NewValue("first", config.WithValueWriteMode(config.WriteModeCreate))
	if _, err := store.Set(ctx, "ns", "key", val); err != nil {
		t.Fatalf("First Set with IfNotExists failed: %v", err)
	}

	// Second set should fail (key already exists)
	val2 := config.NewValue("second", config.WithValueWriteMode(config.WriteModeCreate))
	_, err := store.Set(ctx, "ns", "key", val2)
	if !config.IsKeyExists(err) {
		t.Errorf("Expected KeyExists error, got: %v", err)
	}

	// Verify original value is unchanged
	got, _ := store.Get(ctx, "ns", "key")
	str, _ := got.String()
	if str != "first" {
		t.Errorf("Value changed to %q, expected %q", str, "first")
	}
}

func TestStore_SetWithIfExists(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	// First set should fail (key doesn't exist)
	val := config.NewValue("first", config.WithValueWriteMode(config.WriteModeUpdate))
	_, err := store.Set(ctx, "ns", "key", val)
	if !config.IsNotFound(err) {
		t.Errorf("Expected NotFound error, got: %v", err)
	}

	// Create the key normally
	_, _ = store.Set(ctx, "ns", "key", config.NewValue("original"))

	// Now update should succeed
	val2 := config.NewValue("updated", config.WithValueWriteMode(config.WriteModeUpdate))
	if _, err := store.Set(ctx, "ns", "key", val2); err != nil {
		t.Fatalf("Set with IfExists failed: %v", err)
	}

	// Verify value was updated
	got, _ := store.Get(ctx, "ns", "key")
	str, _ := got.String()
	if str != "updated" {
		t.Errorf("Value = %q, expected %q", str, "updated")
	}
}

func TestStore_SetUpsertIsDefault(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	// Set without write mode should create
	val := config.NewValue("created")
	if _, err := store.Set(ctx, "ns", "key", val); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Set without write mode should update
	val2 := config.NewValue("updated")
	if _, err := store.Set(ctx, "ns", "key", val2); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Verify updated
	got, _ := store.Get(ctx, "ns", "key")
	str, _ := got.String()
	if str != "updated" {
		t.Errorf("Value = %q, expected %q", str, "updated")
	}
}

// Compile-time interface checks
var (
	_ config.Store         = (*Store)(nil)
	_ config.HealthChecker = (*Store)(nil)
	_ config.StatsProvider = (*Store)(nil)
	_ config.BulkStore     = (*Store)(nil)
)

// --- Additional coverage tests ---

func TestStore_SetManyPartialFailure_InvalidKey(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	values := map[string]config.Value{
		"valid-key": config.NewValue("ok"),
		"":          config.NewValue("invalid empty key"),
		"../bad":    config.NewValue("invalid path traversal"),
	}

	err := store.SetMany(ctx, "ns", values)
	if err == nil {
		t.Fatal("Expected BulkWriteError, got nil")
	}

	var bulkErr *config.BulkWriteError
	if !errors.As(err, &bulkErr) {
		t.Fatalf("Expected BulkWriteError, got %T: %v", err, err)
	}

	// valid-key should succeed
	if len(bulkErr.Succeeded) != 1 {
		t.Errorf("Expected 1 succeeded, got %d", len(bulkErr.Succeeded))
	}

	// Two keys should fail (empty key and path traversal)
	if len(bulkErr.Errors) != 2 {
		t.Errorf("Expected 2 errors, got %d", len(bulkErr.Errors))
	}

	// Verify valid key was actually persisted
	got, getErr := store.Get(ctx, "ns", "valid-key")
	if getErr != nil {
		t.Fatalf("Get valid-key failed: %v", getErr)
	}
	str, _ := got.String()
	if str != "ok" {
		t.Errorf("valid-key = %q, want %q", str, "ok")
	}
}

func TestStore_SetManyClosedStore(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	_ = store.Connect(ctx)
	_ = store.Close(ctx)

	err := store.SetMany(ctx, "ns", map[string]config.Value{
		"key1": config.NewValue("value1"),
	})
	if !errors.Is(err, config.ErrStoreClosed) {
		t.Errorf("Expected ErrStoreClosed, got: %v", err)
	}
}

func TestStore_SetManyInvalidNamespace(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	err := store.SetMany(ctx, "!invalid!", map[string]config.Value{
		"key1": config.NewValue("value1"),
	})
	if !errors.Is(err, config.ErrInvalidNamespace) {
		t.Errorf("Expected ErrInvalidNamespace, got: %v", err)
	}
}

func TestStore_DroppedEvents(t *testing.T) {
	// Create store with very small watch buffer
	store := NewStore(WithWatchBufferSize(1))
	ctx := context.Background()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	// Start watching but never consume from the channel
	ch, err := store.Watch(ctx, config.WatchFilter{})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	_ = ch // intentionally not reading from channel

	// Set many values to overflow the buffer
	for i := range 10 {
		key := "key" + string(rune('a'+i))
		_, _ = store.Set(ctx, "ns", key, config.NewValue(i))
	}

	// Wait for async notifications to complete
	time.Sleep(200 * time.Millisecond)

	dropped := store.DroppedEvents()
	if dropped == 0 {
		t.Error("Expected some dropped events due to full channel buffer, got 0")
	}
}

func TestStore_DroppedEventsCallback(t *testing.T) {
	var mu sync.Mutex
	var droppedKeys []string

	store := NewStore(
		WithWatchBufferSize(1),
		WithOnDropped(func(event config.ChangeEvent) {
			mu.Lock()
			droppedKeys = append(droppedKeys, event.Key)
			mu.Unlock()
		}),
	)
	ctx := context.Background()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	// Start watching but never consume
	ch, err := store.Watch(ctx, config.WatchFilter{})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	_ = ch

	// Overflow the buffer
	for i := range 10 {
		key := "key" + string(rune('a'+i))
		_, _ = store.Set(ctx, "ns", key, config.NewValue(i))
	}

	// Wait for async notifications
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	numDropped := len(droppedKeys)
	mu.Unlock()

	if numDropped == 0 {
		t.Error("Expected onDropped callback to be invoked at least once")
	}
}

func TestStore_FindWithEmptyPrefix(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	_, _ = store.Set(ctx, "ns", "alpha", config.NewValue("a"))
	_, _ = store.Set(ctx, "ns", "beta", config.NewValue("b"))
	_, _ = store.Set(ctx, "ns", "gamma", config.NewValue("c"))

	// Empty prefix should match all keys in the namespace
	page, err := store.Find(ctx, "ns", config.NewFilter().WithPrefix("").Build())
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}

	if len(page.Results()) != 3 {
		t.Errorf("Expected 3 results with empty prefix, got %d", len(page.Results()))
	}
}

func TestStore_FindWithKeysFilterNonExistent(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	_, _ = store.Set(ctx, "ns", "exists", config.NewValue("yes"))

	// Request both existing and non-existing keys
	page, err := store.Find(ctx, "ns", config.NewFilter().WithKeys("exists", "nope", "also-nope").Build())
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}

	results := page.Results()
	if len(results) != 1 {
		t.Errorf("Expected 1 result (only 'exists'), got %d", len(results))
	}
	if _, ok := results["exists"]; !ok {
		t.Error("Expected 'exists' key in results")
	}

	// Keys mode should not set a cursor
	if page.NextCursor() != "" {
		t.Errorf("Expected empty cursor for keys mode, got %q", page.NextCursor())
	}
}

func TestStore_FindCursorPagination(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	// Insert 5 values (they get IDs 1-5 in order)
	for i := range 5 {
		key := "key" + string(rune('a'+i))
		_, _ = store.Set(ctx, "ns", key, config.NewValue(i))
	}

	// Get first page of 2
	page1, err := store.Find(ctx, "ns", config.NewFilter().WithLimit(2).Build())
	if err != nil {
		t.Fatalf("Find page 1 failed: %v", err)
	}
	if len(page1.Results()) != 2 {
		t.Fatalf("Expected 2 results on page 1, got %d", len(page1.Results()))
	}
	cursor1 := page1.NextCursor()
	if cursor1 == "" {
		t.Fatal("Expected non-empty cursor after page 1")
	}

	// Get second page using cursor
	page2, err := store.Find(ctx, "ns", config.NewFilter().WithLimit(2).WithCursor(cursor1).Build())
	if err != nil {
		t.Fatalf("Find page 2 failed: %v", err)
	}
	if len(page2.Results()) != 2 {
		t.Fatalf("Expected 2 results on page 2, got %d", len(page2.Results()))
	}
	cursor2 := page2.NextCursor()

	// Get third page (should have 1 remaining)
	page3, err := store.Find(ctx, "ns", config.NewFilter().WithLimit(2).WithCursor(cursor2).Build())
	if err != nil {
		t.Fatalf("Find page 3 failed: %v", err)
	}
	if len(page3.Results()) != 1 {
		t.Errorf("Expected 1 result on page 3, got %d", len(page3.Results()))
	}

	// Verify no overlapping keys between pages
	allKeys := make(map[string]bool)
	for k := range page1.Results() {
		allKeys[k] = true
	}
	for k := range page2.Results() {
		if allKeys[k] {
			t.Errorf("Key %q appears in both page 1 and page 2", k)
		}
		allKeys[k] = true
	}
	for k := range page3.Results() {
		if allKeys[k] {
			t.Errorf("Key %q appears in an earlier page and page 3", k)
		}
		allKeys[k] = true
	}
	if len(allKeys) != 5 {
		t.Errorf("Expected 5 total unique keys, got %d", len(allKeys))
	}
}

func TestStore_FindCursorWithNoMoreResults(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	_, _ = store.Set(ctx, "ns", "only-key", config.NewValue("val"))

	// Use a very high cursor that is beyond any entry
	page, err := store.Find(ctx, "ns", config.NewFilter().WithCursor("9999").Build())
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}

	if len(page.Results()) != 0 {
		t.Errorf("Expected 0 results with high cursor, got %d", len(page.Results()))
	}
}

func TestStore_DeleteManyMixedExistingNonExisting(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	// Create some entries
	_, _ = store.Set(ctx, "ns", "a", config.NewValue("val-a"))
	_, _ = store.Set(ctx, "ns", "b", config.NewValue("val-b"))
	_, _ = store.Set(ctx, "ns", "c", config.NewValue("val-c"))

	// Delete mix of existing and non-existing keys
	deleted, err := store.DeleteMany(ctx, "ns", []string{"a", "nonexistent1", "c", "nonexistent2"})
	if err != nil {
		t.Fatalf("DeleteMany failed: %v", err)
	}

	// Only 2 should have been deleted (a and c)
	if deleted != 2 {
		t.Errorf("Expected 2 deleted, got %d", deleted)
	}

	// Verify a and c are gone
	_, err = store.Get(ctx, "ns", "a")
	if !config.IsNotFound(err) {
		t.Error("Expected 'a' to be deleted")
	}
	_, err = store.Get(ctx, "ns", "c")
	if !config.IsNotFound(err) {
		t.Error("Expected 'c' to be deleted")
	}

	// Verify b still exists
	got, err := store.Get(ctx, "ns", "b")
	if err != nil {
		t.Fatalf("Expected 'b' to still exist, got error: %v", err)
	}
	str, _ := got.String()
	if str != "val-b" {
		t.Errorf("b = %q, want %q", str, "val-b")
	}
}

func TestStore_DeleteManyClosedStore(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	_ = store.Connect(ctx)
	_ = store.Close(ctx)

	_, err := store.DeleteMany(ctx, "ns", []string{"key1"})
	if !errors.Is(err, config.ErrStoreClosed) {
		t.Errorf("Expected ErrStoreClosed, got: %v", err)
	}
}

func TestStore_DeleteManyInvalidNamespace(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	_, err := store.DeleteMany(ctx, "!bad!", []string{"key1"})
	if !errors.Is(err, config.ErrInvalidNamespace) {
		t.Errorf("Expected ErrInvalidNamespace, got: %v", err)
	}
}

func TestStore_WatchMultipleWatchers(t *testing.T) {
	store := NewStore()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	// Create two watchers on the same namespace
	ch1, err := store.Watch(ctx, config.WatchFilter{Namespaces: []string{"ns"}})
	if err != nil {
		t.Fatalf("Watch 1 failed: %v", err)
	}

	ch2, err := store.Watch(ctx, config.WatchFilter{Namespaces: []string{"ns"}})
	if err != nil {
		t.Fatalf("Watch 2 failed: %v", err)
	}

	// Set a value
	go func() {
		time.Sleep(50 * time.Millisecond)
		_, _ = store.Set(ctx, "ns", "shared-key", config.NewValue("shared-val"))
	}()

	// Both watchers should receive the event
	received := make([]bool, 2)
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		select {
		case ev := <-ch1:
			if ev.Key == "shared-key" {
				received[0] = true
			}
		case <-ctx.Done():
		}
	}()

	go func() {
		defer wg.Done()
		select {
		case ev := <-ch2:
			if ev.Key == "shared-key" {
				received[1] = true
			}
		case <-ctx.Done():
		}
	}()

	wg.Wait()

	if !received[0] {
		t.Error("Watcher 1 did not receive event")
	}
	if !received[1] {
		t.Error("Watcher 2 did not receive event")
	}
}

func TestStore_WatchCloseOneDoesNotAffectOther(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	// Create two watchers with independent contexts
	ctx1, cancel1 := context.WithCancel(ctx)
	ctx2, cancel2 := context.WithTimeout(ctx, 3*time.Second)
	defer cancel2()

	ch1, err := store.Watch(ctx1, config.WatchFilter{Namespaces: []string{"ns"}})
	if err != nil {
		t.Fatalf("Watch 1 failed: %v", err)
	}

	ch2, err := store.Watch(ctx2, config.WatchFilter{Namespaces: []string{"ns"}})
	if err != nil {
		t.Fatalf("Watch 2 failed: %v", err)
	}

	// Cancel watcher 1
	cancel1()
	time.Sleep(50 * time.Millisecond) // Let cleanup goroutine run

	// Verify ch1 is closed
	_, ok := <-ch1
	if ok {
		t.Error("Expected ch1 to be closed after context cancellation")
	}

	// Watcher 2 should still work
	go func() {
		time.Sleep(50 * time.Millisecond)
		_, _ = store.Set(ctx2, "ns", "after-cancel", config.NewValue("still-watching"))
	}()

	select {
	case ev := <-ch2:
		if ev.Key != "after-cancel" {
			t.Errorf("Expected key 'after-cancel', got %q", ev.Key)
		}
	case <-ctx2.Done():
		t.Fatal("Timeout: watcher 2 should still receive events after watcher 1 was cancelled")
	}
}

func TestStore_WatchWithPrefixFilter(t *testing.T) {
	store := NewStore()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	// Watch only keys with prefix "app/"
	ch, err := store.Watch(ctx, config.WatchFilter{
		Namespaces: []string{"ns"},
		Prefixes:   []string{"app/"},
	})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}

	// Set two keys: one matching prefix and one not
	go func() {
		time.Sleep(50 * time.Millisecond)
		_, _ = store.Set(ctx, "ns", "other/key", config.NewValue("should-not-see"))
		_, _ = store.Set(ctx, "ns", "app/key", config.NewValue("should-see"))
	}()

	select {
	case ev := <-ch:
		if ev.Key != "app/key" {
			t.Errorf("Expected 'app/key', got %q (prefix filter not working)", ev.Key)
		}
	case <-ctx.Done():
		t.Fatal("Timeout: expected to receive event for 'app/key'")
	}
}

func TestStore_WatchNamespaceFilter(t *testing.T) {
	store := NewStore()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	// Watch only namespace "target"
	ch, err := store.Watch(ctx, config.WatchFilter{
		Namespaces: []string{"target"},
	})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}

	// Set in a different namespace first, then target
	go func() {
		time.Sleep(50 * time.Millisecond)
		_, _ = store.Set(ctx, "other", "key", config.NewValue("wrong-ns"))
		_, _ = store.Set(ctx, "target", "key", config.NewValue("right-ns"))
	}()

	select {
	case ev := <-ch:
		if ev.Namespace != "target" {
			t.Errorf("Expected namespace 'target', got %q", ev.Namespace)
		}
	case <-ctx.Done():
		t.Fatal("Timeout: expected to receive event for 'target' namespace")
	}
}

func TestStore_WatchClosedStore(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	_ = store.Connect(ctx)
	_ = store.Close(ctx)

	_, err := store.Watch(ctx, config.WatchFilter{})
	if !errors.Is(err, config.ErrStoreClosed) {
		t.Errorf("Expected ErrStoreClosed, got: %v", err)
	}
}

func TestStore_OperationsOnClosedStore(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	_ = store.Connect(ctx)
	_ = store.Close(ctx)

	// Get on closed store
	_, err := store.Get(ctx, "ns", "key")
	if !errors.Is(err, config.ErrStoreClosed) {
		t.Errorf("Get: expected ErrStoreClosed, got: %v", err)
	}

	// Set on closed store
	_, err = store.Set(ctx, "ns", "key", config.NewValue("val"))
	if !errors.Is(err, config.ErrStoreClosed) {
		t.Errorf("Set: expected ErrStoreClosed, got: %v", err)
	}

	// Delete on closed store
	err = store.Delete(ctx, "ns", "key")
	if !errors.Is(err, config.ErrStoreClosed) {
		t.Errorf("Delete: expected ErrStoreClosed, got: %v", err)
	}

	// Find on closed store
	_, err = store.Find(ctx, "ns", config.NewFilter().Build())
	if !errors.Is(err, config.ErrStoreClosed) {
		t.Errorf("Find: expected ErrStoreClosed, got: %v", err)
	}

	// Health on closed store
	err = store.Health(ctx)
	if !errors.Is(err, config.ErrStoreClosed) {
		t.Errorf("Health: expected ErrStoreClosed, got: %v", err)
	}

	// Stats on closed store
	_, err = store.Stats(ctx)
	if !errors.Is(err, config.ErrStoreClosed) {
		t.Errorf("Stats: expected ErrStoreClosed, got: %v", err)
	}

	// GetMany on closed store
	_, err = store.GetMany(ctx, "ns", []string{"key"})
	if !errors.Is(err, config.ErrStoreClosed) {
		t.Errorf("GetMany: expected ErrStoreClosed, got: %v", err)
	}
}

func TestStore_DoubleClose(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	_ = store.Connect(ctx)

	if err := store.Close(ctx); err != nil {
		t.Fatalf("First Close failed: %v", err)
	}

	// Second close should be a no-op (not panic)
	if err := store.Close(ctx); err != nil {
		t.Fatalf("Second Close failed: %v", err)
	}
}

func TestStore_InvalidNamespace(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	// Get with invalid namespace
	_, err := store.Get(ctx, "!invalid!", "key")
	if !errors.Is(err, config.ErrInvalidNamespace) {
		t.Errorf("Get: expected ErrInvalidNamespace, got: %v", err)
	}

	// Set with invalid namespace
	_, err = store.Set(ctx, "!invalid!", "key", config.NewValue("val"))
	if !errors.Is(err, config.ErrInvalidNamespace) {
		t.Errorf("Set: expected ErrInvalidNamespace, got: %v", err)
	}

	// Delete with invalid namespace
	err = store.Delete(ctx, "!invalid!", "key")
	if !errors.Is(err, config.ErrInvalidNamespace) {
		t.Errorf("Delete: expected ErrInvalidNamespace, got: %v", err)
	}

	// Find with invalid namespace
	_, err = store.Find(ctx, "!invalid!", config.NewFilter().Build())
	if !errors.Is(err, config.ErrInvalidNamespace) {
		t.Errorf("Find: expected ErrInvalidNamespace, got: %v", err)
	}

	// GetMany with invalid namespace
	_, err = store.GetMany(ctx, "!invalid!", []string{"key"})
	if !errors.Is(err, config.ErrInvalidNamespace) {
		t.Errorf("GetMany: expected ErrInvalidNamespace, got: %v", err)
	}
}

func TestStore_SetInvalidKey(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	// Empty key
	_, err := store.Set(ctx, "ns", "", config.NewValue("val"))
	if !config.IsInvalidKey(err) {
		t.Errorf("Expected invalid key error for empty key, got: %v", err)
	}

	// Path traversal key
	_, err = store.Set(ctx, "ns", "../bad", config.NewValue("val"))
	if !config.IsInvalidKey(err) {
		t.Errorf("Expected invalid key error for path traversal, got: %v", err)
	}
}

func TestStore_SetReturnsValueWithMetadata(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	// Test that Set returns the stored value with metadata
	result, err := store.Set(ctx, "ns", "key", config.NewValue("hello"))
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	if result == nil {
		t.Fatal("Set returned nil value")
	}

	if result.Metadata().Version() != 1 {
		t.Errorf("Expected version 1, got %d", result.Metadata().Version())
	}

	str, _ := result.String()
	if str != "hello" {
		t.Errorf("Returned value = %q, want %q", str, "hello")
	}
}

func TestStore_FindWithKeysEmptySlice(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	_, _ = store.Set(ctx, "ns", "key1", config.NewValue("val1"))

	// Empty keys slice should fall through to prefix/all logic
	page, err := store.Find(ctx, "ns", config.NewFilter().Build())
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}

	if len(page.Results()) != 1 {
		t.Errorf("Expected 1 result, got %d", len(page.Results()))
	}
}

func TestStore_WithCodecOption(t *testing.T) {
	// Verify WithCodec option applies correctly
	store := NewStore(WithCodec(nil)) // nil should be ignored
	if store == nil {
		t.Fatal("NewStore returned nil")
	}

	ctx := context.Background()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	// Basic operations should still work
	_, err := store.Set(ctx, "ns", "key", config.NewValue("val"))
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
}

func TestStore_WithWatchBufferSizeZero(t *testing.T) {
	// WithWatchBufferSize(0) should be ignored (keep default)
	store := NewStore(WithWatchBufferSize(0))
	if store.bufferSize != 100 {
		t.Errorf("Expected default buffer size 100, got %d", store.bufferSize)
	}
}

func TestStore_FindDifferentNamespaces(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	_, _ = store.Set(ctx, "ns1", "key1", config.NewValue("val1"))
	_, _ = store.Set(ctx, "ns2", "key2", config.NewValue("val2"))
	_, _ = store.Set(ctx, "ns1", "key3", config.NewValue("val3"))

	// Find in ns1 should only return ns1 entries
	page, err := store.Find(ctx, "ns1", config.NewFilter().Build())
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}

	results := page.Results()
	if len(results) != 2 {
		t.Errorf("Expected 2 results from ns1, got %d", len(results))
	}
}

func TestStore_WatchAllNamespaces(t *testing.T) {
	store := NewStore()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	// Watch all namespaces (empty filter)
	ch, err := store.Watch(ctx, config.WatchFilter{})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		_, _ = store.Set(ctx, "any-ns", "key", config.NewValue("val"))
	}()

	select {
	case ev := <-ch:
		if ev.Key != "key" {
			t.Errorf("Expected key 'key', got %q", ev.Key)
		}
	case <-ctx.Done():
		t.Fatal("Timeout: expected to receive event with empty filter")
	}
}

func TestStore_SetManyUpdatesExisting(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	// Create initial entries
	_, _ = store.Set(ctx, "ns", "key1", config.NewValue("original"))

	// SetMany should update existing entry
	err := store.SetMany(ctx, "ns", map[string]config.Value{
		"key1": config.NewValue("updated"),
		"key2": config.NewValue("new"),
	})
	if err != nil {
		t.Fatalf("SetMany failed: %v", err)
	}

	// Verify updated entry has version 2
	got, _ := store.Get(ctx, "ns", "key1")
	if got.Metadata().Version() != 2 {
		t.Errorf("key1 version = %d, want 2", got.Metadata().Version())
	}
	str, _ := got.String()
	if str != "updated" {
		t.Errorf("key1 = %q, want %q", str, "updated")
	}

	// Verify new entry has version 1
	got2, _ := store.Get(ctx, "ns", "key2")
	if got2.Metadata().Version() != 1 {
		t.Errorf("key2 version = %d, want 1", got2.Metadata().Version())
	}
}

func TestStore_DeleteManyNotifications(t *testing.T) {
	store := NewStore()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	_, _ = store.Set(ctx, "ns", "del1", config.NewValue("v1"))
	_, _ = store.Set(ctx, "ns", "del2", config.NewValue("v2"))

	// Allow Set notifications to complete
	time.Sleep(50 * time.Millisecond)

	ch, err := store.Watch(ctx, config.WatchFilter{Namespaces: []string{"ns"}})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		_, _ = store.DeleteMany(ctx, "ns", []string{"del1", "del2"})
	}()

	// Should receive at least one delete event
	select {
	case ev := <-ch:
		if ev.Type != config.ChangeTypeDelete {
			t.Errorf("Expected delete event, got %v", ev.Type)
		}
	case <-ctx.Done():
		t.Fatal("Timeout waiting for delete notification")
	}
}

func TestStore_BulkWriteErrorDetails(t *testing.T) {
	store := NewStore()
	ctx := context.Background()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	values := map[string]config.Value{
		"good-key": config.NewValue("ok"),
		"":         config.NewValue("bad"),
	}

	err := store.SetMany(ctx, "ns", values)
	if err == nil {
		t.Fatal("Expected error")
	}

	if !config.IsBulkWritePartial(err) {
		t.Fatalf("Expected BulkWritePartial, got: %v", err)
	}

	var bulkErr *config.BulkWriteError
	if errors.As(err, &bulkErr) {
		failedKeys := bulkErr.FailedKeys()
		if len(failedKeys) != 1 {
			t.Errorf("Expected 1 failed key, got %d", len(failedKeys))
		}

		keyErrors := bulkErr.KeyErrors()
		if _, ok := keyErrors[""]; !ok {
			t.Error("Expected empty string key in error map")
		}

		// Error message should be informative
		errStr := bulkErr.Error()
		if errStr == "" {
			t.Error("Expected non-empty error message")
		}
	}
}
