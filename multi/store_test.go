package multi

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/memory"
)

var errMock = errors.New("mock error")

type failStore struct {
	config.Store
	connectErr error
	closeErr   error
	getErr     error
	setErr     error
	deleteErr  error
	findErr    error
	watchErr   error
	healthErr  error
	statsErr   error
}

func (f *failStore) Connect(ctx context.Context) error { return f.connectErr }
func (f *failStore) Close(ctx context.Context) error   { return f.closeErr }

func (f *failStore) Get(ctx context.Context, namespace, key string) (config.Value, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	return nil, &config.KeyNotFoundError{Key: key, Namespace: namespace}
}

func (f *failStore) Set(ctx context.Context, namespace, key string, value config.Value) (config.Value, error) {
	if f.setErr != nil {
		return nil, f.setErr
	}
	return value, nil
}

func (f *failStore) Delete(ctx context.Context, namespace, key string) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	return &config.KeyNotFoundError{Key: key, Namespace: namespace}
}

func (f *failStore) Find(ctx context.Context, namespace string, filter config.Filter) (config.Page, error) {
	return nil, f.findErr
}

func (f *failStore) Watch(ctx context.Context, filter config.WatchFilter) (<-chan config.ChangeEvent, error) {
	if f.watchErr != nil {
		return nil, f.watchErr
	}
	ch := make(chan config.ChangeEvent, 1)
	return ch, nil
}

func (f *failStore) Health(ctx context.Context) error {
	return f.healthErr
}

func (f *failStore) Stats(ctx context.Context) (*config.StoreStats, error) {
	if f.statsErr != nil {
		return nil, f.statsErr
	}
	return &config.StoreStats{
		TotalEntries:       5,
		EntriesByType:      map[config.Type]int64{},
		EntriesByNamespace: map[string]int64{},
	}, nil
}

type noWatchStore struct {
	*memory.Store
}

func (s *noWatchStore) Watch(ctx context.Context, filter config.WatchFilter) (<-chan config.ChangeEvent, error) {
	return nil, config.ErrWatchNotSupported
}

func TestNewStore(t *testing.T) {
	store1 := memory.NewStore()
	store2 := memory.NewStore()

	ms := NewStore([]config.Store{store1, store2})
	if ms == nil {
		t.Fatal("NewStore returned nil")
	}
	if len(ms.stores) != 2 {
		t.Errorf("Expected 2 stores, got %d", len(ms.stores))
	}
}

func TestMultiStore_Connect(t *testing.T) {
	store1 := memory.NewStore()
	store2 := memory.NewStore()

	ms := NewStore([]config.Store{store1, store2})
	ctx := context.Background()

	if err := ms.Connect(ctx); err != nil {
		t.Errorf("Connect failed: %v", err)
	}
}

func TestMultiStore_Close(t *testing.T) {
	store1 := memory.NewStore()
	store2 := memory.NewStore()

	ms := NewStore([]config.Store{store1, store2})
	ctx := context.Background()

	_ = ms.Connect(ctx)
	if err := ms.Close(ctx); err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestMultiStore_SetAndGet_Fallback(t *testing.T) {
	store1 := memory.NewStore()
	store2 := memory.NewStore()

	ms := NewStore([]config.Store{store1, store2}, WithStrategy(StrategyFallback))
	ctx := context.Background()

	_ = ms.Connect(ctx)
	defer ms.Close(ctx)

	// Create a value
	val := config.NewValue(42)

	// Set should write to both stores
	if _, err := ms.Set(ctx, "ns", "key", val); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Get from multi-store should work
	got, err := ms.Get(ctx, "ns", "key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	intVal, _ := got.Int64()
	if intVal != 42 {
		t.Errorf("Expected 42, got %d", intVal)
	}

	// Both stores should have the value
	got1, _ := store1.Get(ctx, "ns", "key")
	got2, _ := store2.Get(ctx, "ns", "key")

	int1, _ := got1.Int64()
	int2, _ := got2.Int64()

	if int1 != 42 || int2 != 42 {
		t.Error("Expected both stores to have the value")
	}
}

func TestMultiStore_SetAndGet_ReadThrough(t *testing.T) {
	cache := memory.NewStore()
	backend := memory.NewStore()

	ms := NewStore([]config.Store{cache, backend}, WithStrategy(StrategyReadThrough))
	ctx := context.Background()

	_ = ms.Connect(ctx)
	defer ms.Close(ctx)

	// Directly set value in backend only
	val := config.NewValue("backend-value")
	_, _ = backend.Set(ctx, "ns", "key", val)

	// Get should read from backend and populate cache
	got, err := ms.Get(ctx, "ns", "key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	strVal, _ := got.String()
	if strVal != "backend-value" {
		t.Errorf("Expected 'backend-value', got %s", strVal)
	}

	// Cache should now have the value
	cacheVal, err := cache.Get(ctx, "ns", "key")
	if err != nil {
		t.Fatalf("Cache should have value: %v", err)
	}

	cacheStr, _ := cacheVal.String()
	if cacheStr != "backend-value" {
		t.Errorf("Cache should have 'backend-value', got %s", cacheStr)
	}
}

func TestMultiStore_SetAndGet_ReadThrough_WriteAll(t *testing.T) {
	cache := memory.NewStore()
	backend := memory.NewStore()

	ms := NewStore([]config.Store{cache, backend}, WithStrategy(StrategyReadThrough))
	ctx := context.Background()

	_ = ms.Connect(ctx)
	defer ms.Close(ctx)

	// Set via multi-store writes to all stores for consistency
	val := config.NewValue("new-value")
	if _, err := ms.Set(ctx, "ns", "key", val); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Cache should have it
	cacheVal, err := cache.Get(ctx, "ns", "key")
	if err != nil {
		t.Fatalf("Cache should have value: %v", err)
	}
	cacheStr, _ := cacheVal.String()
	if cacheStr != "new-value" {
		t.Error("Cache should have the value")
	}

	// Backend should also have it (all stores are written for consistency)
	backendVal, err := backend.Get(ctx, "ns", "key")
	if err != nil {
		t.Fatalf("Backend should have value: %v", err)
	}
	backendStr, _ := backendVal.String()
	if backendStr != "new-value" {
		t.Error("Backend should have the value")
	}
}

func TestMultiStore_Delete_Fallback(t *testing.T) {
	store1 := memory.NewStore()
	store2 := memory.NewStore()

	ms := NewStore([]config.Store{store1, store2}, WithStrategy(StrategyFallback))
	ctx := context.Background()

	_ = ms.Connect(ctx)
	defer ms.Close(ctx)

	// Set value in both stores
	val := config.NewValue("value")
	_, _ = ms.Set(ctx, "ns", "key", val)

	// Delete should remove from both
	if err := ms.Delete(ctx, "ns", "key"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Both stores should return NotFound
	_, err1 := store1.Get(ctx, "ns", "key")
	_, err2 := store2.Get(ctx, "ns", "key")

	if !config.IsNotFound(err1) || !config.IsNotFound(err2) {
		t.Error("Expected both stores to have deleted the value")
	}
}

func TestMultiStore_Fallback_PrimaryFailure(t *testing.T) {
	store1 := memory.NewStore()
	store2 := memory.NewStore()

	ms := NewStore([]config.Store{store1, store2}, WithStrategy(StrategyFallback))
	ctx := context.Background()

	_ = ms.Connect(ctx)
	defer ms.Close(ctx)

	// Only set value in secondary store
	val := config.NewValue("secondary-value")
	_, _ = store2.Set(ctx, "ns", "key", val)

	// Get should fall back to secondary
	got, err := ms.Get(ctx, "ns", "key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	strVal, _ := got.String()
	if strVal != "secondary-value" {
		t.Errorf("Expected 'secondary-value', got %s", strVal)
	}
}

func TestMultiStore_Find(t *testing.T) {
	store1 := memory.NewStore()
	store2 := memory.NewStore()

	ms := NewStore([]config.Store{store1, store2})
	ctx := context.Background()

	_ = ms.Connect(ctx)
	defer ms.Close(ctx)

	// Set values in primary store
	val1 := config.NewValue("value1")
	val2 := config.NewValue("value2")
	_, _ = store1.Set(ctx, "ns", "app/key1", val1)
	_, _ = store1.Set(ctx, "ns", "app/key2", val2)

	// Find should search primary only
	page, err := ms.Find(ctx, "ns", config.NewFilter().WithPrefix("app/").Build())
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}

	results := page.Results()
	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}
}

func TestMultiStore_Watch(t *testing.T) {
	store1 := memory.NewStore()
	store2 := memory.NewStore()

	ms := NewStore([]config.Store{store1, store2})
	ctx := context.Background()

	_ = ms.Connect(ctx)
	defer ms.Close(ctx)

	// Watch should work through primary
	ch, err := ms.Watch(ctx, config.WatchFilter{Namespaces: []string{"ns"}})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}

	if ch == nil {
		t.Error("Expected watch channel, got nil")
	}
}

func TestMultiStore_Primary(t *testing.T) {
	store1 := memory.NewStore()
	store2 := memory.NewStore()

	ms := NewStore([]config.Store{store1, store2})

	primary := ms.Primary()
	if primary != store1 {
		t.Error("Expected Primary to return first store")
	}
}

func TestMultiStore_Stores(t *testing.T) {
	store1 := memory.NewStore()
	store2 := memory.NewStore()

	ms := NewStore([]config.Store{store1, store2})

	stores := ms.Stores()
	if len(stores) != 2 {
		t.Errorf("Expected 2 stores, got %d", len(stores))
	}
}

func TestMultiStore_EmptyStores(t *testing.T) {
	ms := NewStore(nil)
	ctx := context.Background()

	// Find should fail gracefully
	_, err := ms.Find(ctx, "ns", config.NewFilter().Build())
	if err != config.ErrStoreNotConnected {
		t.Errorf("Expected ErrStoreNotConnected, got %v", err)
	}

	// Watch should fail gracefully
	_, err = ms.Watch(ctx, config.WatchFilter{})
	if err != config.ErrStoreNotConnected {
		t.Errorf("Expected ErrStoreNotConnected, got %v", err)
	}

	// Primary should return nil
	if ms.Primary() != nil {
		t.Error("Expected Primary to return nil for empty store")
	}
}

func TestMultiStore_WriteThrough(t *testing.T) {
	store1 := memory.NewStore()
	store2 := memory.NewStore()

	ms := NewStore([]config.Store{store1, store2}, WithStrategy(StrategyWriteThrough))
	ctx := context.Background()

	_ = ms.Connect(ctx)
	defer ms.Close(ctx)

	// Set should write to all stores
	val := config.NewValue("sync-value")
	if _, err := ms.Set(ctx, "ns", "key", val); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Both stores should have the value
	got1, _ := store1.Get(ctx, "ns", "key")
	got2, _ := store2.Get(ctx, "ns", "key")

	str1, _ := got1.String()
	str2, _ := got2.String()

	if str1 != "sync-value" || str2 != "sync-value" {
		t.Error("Expected both stores to have the value")
	}
}

func TestMultiStore_Health(t *testing.T) {
	store1 := memory.NewStore()
	store2 := memory.NewStore()

	ms := NewStore([]config.Store{store1, store2})
	ctx := context.Background()

	_ = ms.Connect(ctx)
	defer ms.Close(ctx)

	// Memory stores implement HealthChecker, should be healthy
	if err := ms.Health(ctx); err != nil {
		t.Errorf("Health check failed: %v", err)
	}
}

func TestMultiStore_Health_EmptyStores(t *testing.T) {
	ms := NewStore(nil)
	ctx := context.Background()

	err := ms.Health(ctx)
	if err != config.ErrStoreNotConnected {
		t.Errorf("Expected ErrStoreNotConnected, got %v", err)
	}
}

func TestMultiStore_Stats(t *testing.T) {
	store1 := memory.NewStore()
	store2 := memory.NewStore()

	ms := NewStore([]config.Store{store1, store2})
	ctx := context.Background()

	_ = ms.Connect(ctx)
	defer ms.Close(ctx)

	// Set some values
	_, _ = ms.Set(ctx, "ns1", "key1", config.NewValue("value1"))
	_, _ = ms.Set(ctx, "ns1", "key2", config.NewValue(42))
	_, _ = ms.Set(ctx, "ns2", "key3", config.NewValue(true))

	// Memory stores implement StatsProvider
	stats, err := ms.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}

	if stats == nil {
		t.Fatal("Expected stats, got nil")
	}

	if stats.TotalEntries < 3 {
		t.Errorf("Expected at least 3 entries, got %d", stats.TotalEntries)
	}
}

func TestMultiStore_Stats_EmptyStores(t *testing.T) {
	ms := NewStore(nil)
	ctx := context.Background()

	_, err := ms.Stats(ctx)
	if err != config.ErrStoreNotConnected {
		t.Errorf("Expected ErrStoreNotConnected, got %v", err)
	}
}

func TestMultiStore_Get_AllStoresFail(t *testing.T) {
	store1 := memory.NewStore()
	store2 := memory.NewStore()

	ms := NewStore([]config.Store{store1, store2})
	ctx := context.Background()
	_ = ms.Connect(ctx)
	defer ms.Close(ctx)

	_, err := ms.Get(ctx, "ns", "nokey")
	if !config.IsNotFound(err) {
		t.Errorf("Expected NotFound error, got %v", err)
	}
}

func TestMultiStore_Get_ErrorFromLastStore(t *testing.T) {
	f1 := &failStore{getErr: &config.KeyNotFoundError{Key: "k", Namespace: "ns"}}
	f2 := &failStore{getErr: &config.KeyNotFoundError{Key: "k", Namespace: "ns"}}

	ms := NewStore([]config.Store{f1, f2})
	ctx := context.Background()

	_, err := ms.Get(ctx, "ns", "k")
	if !config.IsNotFound(err) {
		t.Errorf("Expected NotFound from last store, got %v", err)
	}
}

func TestMultiStore_Get_NonRetryableError(t *testing.T) {
	f1 := &failStore{getErr: errMock}
	store2 := memory.NewStore()
	_, _ = store2.Set(context.Background(), "ns", "key", config.NewValue("val"))

	ms := NewStore([]config.Store{f1, store2})
	ctx := context.Background()

	_, err := ms.Get(ctx, "ns", "key")
	if !errors.Is(err, errMock) {
		t.Errorf("Expected non-retryable error to be returned immediately, got %v", err)
	}
}

func TestMultiStore_Get_ClosedStoreRetries(t *testing.T) {
	f1 := &failStore{getErr: config.ErrStoreClosed}
	store2 := memory.NewStore()
	_, _ = store2.Set(context.Background(), "ns", "key", config.NewValue("val"))

	ms := NewStore([]config.Store{f1, store2})
	ctx := context.Background()

	got, err := ms.Get(ctx, "ns", "key")
	if err != nil {
		t.Fatalf("Expected fallback to second store, got error: %v", err)
	}
	s, _ := got.String()
	if s != "val" {
		t.Errorf("Expected 'val', got %s", s)
	}
}

func TestMultiStore_Get_NotConnectedRetries(t *testing.T) {
	f1 := &failStore{getErr: config.ErrStoreNotConnected}
	store2 := memory.NewStore()
	_, _ = store2.Set(context.Background(), "ns", "key", config.NewValue("hello"))

	ms := NewStore([]config.Store{f1, store2})
	ctx := context.Background()

	got, err := ms.Get(ctx, "ns", "key")
	if err != nil {
		t.Fatalf("Expected fallback to second store, got error: %v", err)
	}
	s, _ := got.String()
	if s != "hello" {
		t.Errorf("Expected 'hello', got %s", s)
	}
}

func TestMultiStore_Set_EmptyStores(t *testing.T) {
	ms := NewStore(nil)
	ctx := context.Background()

	_, err := ms.Set(ctx, "ns", "key", config.NewValue("v"))
	if err != config.ErrStoreNotConnected {
		t.Errorf("Expected ErrStoreNotConnected, got %v", err)
	}
}

func TestMultiStore_Set_AllStoresFail(t *testing.T) {
	f1 := &failStore{setErr: errMock}
	f2 := &failStore{setErr: errMock}

	ms := NewStore([]config.Store{f1, f2})
	ctx := context.Background()

	_, err := ms.Set(ctx, "ns", "key", config.NewValue("v"))
	if err == nil {
		t.Fatal("Expected error when all stores fail")
	}
}

func TestMultiStore_Set_PartialFailure(t *testing.T) {
	f1 := &failStore{setErr: errMock}
	store2 := memory.NewStore()

	ms := NewStore([]config.Store{f1, store2})
	ctx := context.Background()
	_ = ms.Connect(ctx)

	result, err := ms.Set(ctx, "ns", "key", config.NewValue("val"))
	if err != nil {
		t.Fatalf("Set should succeed if at least one store succeeds, got %v", err)
	}
	if result == nil {
		t.Fatal("Expected non-nil result")
	}
}

func TestMultiStore_Delete_EmptyStores(t *testing.T) {
	ms := NewStore(nil)
	ctx := context.Background()

	err := ms.Delete(ctx, "ns", "key")
	if err != config.ErrStoreNotConnected {
		t.Errorf("Expected ErrStoreNotConnected, got %v", err)
	}
}

func TestMultiStore_Delete_AllNotFound(t *testing.T) {
	store1 := memory.NewStore()
	store2 := memory.NewStore()

	ms := NewStore([]config.Store{store1, store2})
	ctx := context.Background()
	_ = ms.Connect(ctx)
	defer ms.Close(ctx)

	err := ms.Delete(ctx, "ns", "nokey")
	if !config.IsNotFound(err) {
		t.Errorf("Expected NotFound when key absent from all stores, got %v", err)
	}
}

func TestMultiStore_Delete_PartialPresence(t *testing.T) {
	store1 := memory.NewStore()
	store2 := memory.NewStore()

	ctx := context.Background()
	ms := NewStore([]config.Store{store1, store2})
	_ = ms.Connect(ctx)
	defer ms.Close(ctx)

	_, _ = store1.Set(ctx, "ns", "key", config.NewValue("v"))

	err := ms.Delete(ctx, "ns", "key")
	if err != nil {
		t.Fatalf("Delete should succeed when key exists in at least one store, got %v", err)
	}

	_, err = store1.Get(ctx, "ns", "key")
	if !config.IsNotFound(err) {
		t.Error("Expected key to be deleted from store1")
	}
}

func TestMultiStore_Delete_MixedErrors(t *testing.T) {
	f1 := &failStore{deleteErr: errMock}
	f2 := &failStore{deleteErr: errMock}

	ms := NewStore([]config.Store{f1, f2})
	ctx := context.Background()

	err := ms.Delete(ctx, "ns", "key")
	if err == nil {
		t.Fatal("Expected error when all stores fail with non-NotFound errors")
	}
}

func TestMultiStore_Find_PrimaryOnly(t *testing.T) {
	store1 := memory.NewStore()
	store2 := memory.NewStore()

	ctx := context.Background()
	ms := NewStore([]config.Store{store1, store2})
	_ = ms.Connect(ctx)
	defer ms.Close(ctx)

	_, _ = store1.Set(ctx, "ns", "app/a", config.NewValue("v1"))
	_, _ = store2.Set(ctx, "ns", "app/b", config.NewValue("v2"))
	_, _ = store2.Set(ctx, "ns", "app/c", config.NewValue("v3"))

	page, err := ms.Find(ctx, "ns", config.NewFilter().WithPrefix("app/").Build())
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}

	results := page.Results()
	if len(results) != 1 {
		t.Errorf("Expected 1 result from primary only, got %d", len(results))
	}
	if _, ok := results["app/a"]; !ok {
		t.Error("Expected result from primary store")
	}
}

func TestMultiStore_Watch_FallbackOnUnsupported(t *testing.T) {
	nw := &noWatchStore{Store: memory.NewStore()}
	store2 := memory.NewStore()

	ctx := context.Background()
	ms := NewStore([]config.Store{nw, store2})
	_ = ms.Connect(ctx)
	defer ms.Close(ctx)

	ch, err := ms.Watch(ctx, config.WatchFilter{})
	if err != nil {
		t.Fatalf("Watch should fall back to second store, got %v", err)
	}
	if ch == nil {
		t.Error("Expected non-nil channel from fallback store")
	}
}

func TestMultiStore_Watch_AllUnsupported(t *testing.T) {
	nw1 := &noWatchStore{Store: memory.NewStore()}
	nw2 := &noWatchStore{Store: memory.NewStore()}

	ctx := context.Background()
	ms := NewStore([]config.Store{nw1, nw2})

	_, err := ms.Watch(ctx, config.WatchFilter{})
	if !errors.Is(err, config.ErrWatchNotSupported) {
		t.Errorf("Expected ErrWatchNotSupported, got %v", err)
	}
}

func TestMultiStore_Watch_NonWatchError(t *testing.T) {
	f1 := &failStore{watchErr: errMock}
	store2 := memory.NewStore()

	ms := NewStore([]config.Store{f1, store2})
	ctx := context.Background()

	_, err := ms.Watch(ctx, config.WatchFilter{})
	if !errors.Is(err, errMock) {
		t.Errorf("Expected mock error returned immediately, got %v", err)
	}
}

func TestMultiStore_Close_PropagatesErrors(t *testing.T) {
	f1 := &failStore{closeErr: errors.New("close1")}
	f2 := &failStore{closeErr: errors.New("close2")}

	ms := NewStore([]config.Store{f1, f2})
	ctx := context.Background()

	err := ms.Close(ctx)
	if err == nil {
		t.Fatal("Expected error from Close")
	}
	if !errors.Is(err, f1.closeErr) {
		t.Errorf("Expected close1 in error chain, got %v", err)
	}
	if !errors.Is(err, f2.closeErr) {
		t.Errorf("Expected close2 in error chain, got %v", err)
	}
}

func TestMultiStore_Connect_AllFail(t *testing.T) {
	f1 := &failStore{connectErr: errors.New("conn1")}
	f2 := &failStore{connectErr: errors.New("conn2")}

	ms := NewStore([]config.Store{f1, f2})
	ctx := context.Background()

	err := ms.Connect(ctx)
	if err == nil {
		t.Fatal("Expected error when all stores fail to connect")
	}
}

func TestMultiStore_Connect_PartialFailure(t *testing.T) {
	f1 := &failStore{connectErr: errors.New("conn1")}
	store2 := memory.NewStore()

	ms := NewStore([]config.Store{f1, store2})
	ctx := context.Background()

	err := ms.Connect(ctx)
	if err != nil {
		t.Errorf("Connect should succeed if at least one store connects, got %v", err)
	}
}

func TestMultiStore_Health_AllUnhealthy(t *testing.T) {
	f1 := &failStore{healthErr: errors.New("unhealthy1")}
	f2 := &failStore{healthErr: errors.New("unhealthy2")}

	ms := NewStore([]config.Store{f1, f2})
	ctx := context.Background()

	err := ms.Health(ctx)
	if err == nil {
		t.Fatal("Expected error when all stores are unhealthy")
	}
}

func TestMultiStore_Health_OneHealthy(t *testing.T) {
	f1 := &failStore{healthErr: errors.New("unhealthy")}
	store2 := memory.NewStore()

	ms := NewStore([]config.Store{f1, store2})
	ctx := context.Background()

	err := ms.Health(ctx)
	if err != nil {
		t.Errorf("Health should succeed if at least one store is healthy, got %v", err)
	}
}

func TestMultiStore_Health_NoHealthCheckers(t *testing.T) {
	f1 := &noHealthStore{}
	f2 := &noHealthStore{}

	ms := NewStore([]config.Store{f1, f2})
	ctx := context.Background()

	err := ms.Health(ctx)
	if err != nil {
		t.Errorf("Health should return nil when no stores implement HealthChecker, got %v", err)
	}
}

type noHealthStore struct {
	failStore
}

func (s *noHealthStore) Health(_ context.Context) error {
	return nil
}

func TestMultiStore_Stats_NoStatsProviders(t *testing.T) {
	ms := NewStore([]config.Store{&plainStore{}, &plainStore{}})
	ctx := context.Background()

	stats, err := ms.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats should not error, got %v", err)
	}
	if stats != nil {
		t.Error("Expected nil stats when no stores implement StatsProvider")
	}
}

type plainStore struct{}

func (s *plainStore) Connect(ctx context.Context) error { return nil }
func (s *plainStore) Close(ctx context.Context) error   { return nil }
func (s *plainStore) Get(ctx context.Context, ns, key string) (config.Value, error) {
	return nil, config.ErrNotFound
}
func (s *plainStore) Set(ctx context.Context, ns, key string, v config.Value) (config.Value, error) {
	return v, nil
}
func (s *plainStore) Delete(ctx context.Context, ns, key string) error { return config.ErrNotFound }
func (s *plainStore) Find(ctx context.Context, ns string, f config.Filter) (config.Page, error) {
	return nil, nil
}
func (s *plainStore) Watch(ctx context.Context, f config.WatchFilter) (<-chan config.ChangeEvent, error) {
	return nil, config.ErrWatchNotSupported
}

func TestMultiStore_Stats_Aggregation(t *testing.T) {
	store1 := memory.NewStore()
	store2 := memory.NewStore()

	ctx := context.Background()
	ms := NewStore([]config.Store{store1, store2})
	_ = ms.Connect(ctx)
	defer ms.Close(ctx)

	_, _ = store1.Set(ctx, "ns1", "k1", config.NewValue("v1"))
	_, _ = store1.Set(ctx, "ns1", "k2", config.NewValue("v2"))
	_, _ = store2.Set(ctx, "ns1", "k1", config.NewValue("v1"))
	_, _ = store2.Set(ctx, "ns1", "k2", config.NewValue("v2"))
	_, _ = store2.Set(ctx, "ns2", "k3", config.NewValue(42))

	stats, err := ms.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}
	if stats.TotalEntries != 3 {
		t.Errorf("Expected TotalEntries=3 (max across stores), got %d", stats.TotalEntries)
	}
	if stats.EntriesByNamespace["ns1"] != 2 {
		t.Errorf("Expected ns1 count=2, got %d", stats.EntriesByNamespace["ns1"])
	}
	if stats.EntriesByNamespace["ns2"] != 1 {
		t.Errorf("Expected ns2 count=1, got %d", stats.EntriesByNamespace["ns2"])
	}
}

func TestMultiStore_Stats_StatsErrorSkipped(t *testing.T) {
	f1 := &failStore{statsErr: errMock}
	store2 := memory.NewStore()

	ctx := context.Background()
	ms := NewStore([]config.Store{f1, store2})
	_ = ms.Connect(ctx)

	_, _ = store2.Set(ctx, "ns", "k", config.NewValue("v"))

	stats, err := ms.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats should skip errored store, got %v", err)
	}
	if stats == nil {
		t.Fatal("Expected non-nil stats from second store")
	}
	if stats.TotalEntries != 1 {
		t.Errorf("Expected 1 entry from second store, got %d", stats.TotalEntries)
	}
}

func TestMultiStore_GetMany_Fallback(t *testing.T) {
	store1 := memory.NewStore()
	store2 := memory.NewStore()

	ctx := context.Background()
	ms := NewStore([]config.Store{store1, store2})
	_ = ms.Connect(ctx)
	defer ms.Close(ctx)

	_, _ = store1.Set(ctx, "ns", "k1", config.NewValue("v1"))
	_, _ = store1.Set(ctx, "ns", "k2", config.NewValue("v2"))

	results, err := ms.GetMany(ctx, "ns", []string{"k1", "k2", "k3"})
	if err != nil {
		t.Fatalf("GetMany failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}
	if _, ok := results["k1"]; !ok {
		t.Error("Expected k1 in results")
	}
	if _, ok := results["k2"]; !ok {
		t.Error("Expected k2 in results")
	}
}

func TestMultiStore_GetMany_EmptyStores(t *testing.T) {
	ms := NewStore(nil)
	ctx := context.Background()

	_, err := ms.GetMany(ctx, "ns", []string{"k"})
	if err != config.ErrStoreNotConnected {
		t.Errorf("Expected ErrStoreNotConnected, got %v", err)
	}
}

func TestMultiStore_GetMany_FallbackToSecondStore(t *testing.T) {
	f1 := &failBulkStore{failStore: failStore{getErr: config.ErrStoreClosed}, getManyErr: config.ErrStoreClosed}
	store2 := memory.NewStore()

	ctx := context.Background()
	ms := NewStore([]config.Store{f1, store2})
	_ = ms.Connect(ctx)

	_, _ = store2.Set(ctx, "ns", "k1", config.NewValue("v1"))

	results, err := ms.GetMany(ctx, "ns", []string{"k1"})
	if err != nil {
		t.Fatalf("GetMany should fall back to second store, got %v", err)
	}
	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}
}

func TestMultiStore_GetMany_AllFail(t *testing.T) {
	f1 := &failBulkStore{failStore: failStore{getErr: config.ErrStoreClosed}, getManyErr: config.ErrStoreClosed}
	f2 := &failBulkStore{failStore: failStore{getErr: config.ErrStoreClosed}, getManyErr: config.ErrStoreClosed}

	ms := NewStore([]config.Store{f1, f2})
	ctx := context.Background()

	_, err := ms.GetMany(ctx, "ns", []string{"k1"})
	if err == nil {
		t.Fatal("Expected error when all stores fail")
	}
}

func TestMultiStore_GetMany_ReadThrough(t *testing.T) {
	f1 := &failBulkStore{getManyErr: errMock}
	backend := memory.NewStore()

	ctx := context.Background()
	ms := NewStore([]config.Store{f1, backend}, WithStrategy(StrategyReadThrough))
	_ = ms.Connect(ctx)

	_, _ = backend.Set(ctx, "ns", "k1", config.NewValue("v1"))
	_, _ = backend.Set(ctx, "ns", "k2", config.NewValue("v2"))

	results, err := ms.GetMany(ctx, "ns", []string{"k1", "k2"})
	if err != nil {
		t.Fatalf("GetMany failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}
}

func TestMultiStore_GetMany_NonBulkStoreFallback(t *testing.T) {
	f1 := &failStore{getErr: &config.KeyNotFoundError{Key: "k1", Namespace: "ns"}}
	store2 := memory.NewStore()

	ctx := context.Background()
	ms := NewStore([]config.Store{f1, store2})
	_ = ms.Connect(ctx)

	_, _ = store2.Set(ctx, "ns", "k1", config.NewValue("v1"))

	results, err := ms.GetMany(ctx, "ns", []string{"k1"})
	if err != nil {
		t.Fatalf("GetMany failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("Expected 0 results from failStore (individual gets all fail), got %d", len(results))
	}
}

func TestMultiStore_SetMany_EmptyStores(t *testing.T) {
	ms := NewStore(nil)
	ctx := context.Background()

	err := ms.SetMany(ctx, "ns", map[string]config.Value{"k": config.NewValue("v")})
	if err != config.ErrStoreNotConnected {
		t.Errorf("Expected ErrStoreNotConnected, got %v", err)
	}
}

func TestMultiStore_SetMany_WritesToAll(t *testing.T) {
	store1 := memory.NewStore()
	store2 := memory.NewStore()

	ctx := context.Background()
	ms := NewStore([]config.Store{store1, store2})
	_ = ms.Connect(ctx)
	defer ms.Close(ctx)

	vals := map[string]config.Value{
		"k1": config.NewValue("v1"),
		"k2": config.NewValue("v2"),
	}

	err := ms.SetMany(ctx, "ns", vals)
	if err != nil {
		t.Fatalf("SetMany failed: %v", err)
	}

	for _, s := range []config.Store{store1, store2} {
		v1, err := s.Get(ctx, "ns", "k1")
		if err != nil {
			t.Fatalf("Get k1 from store failed: %v", err)
		}
		str, _ := v1.String()
		if str != "v1" {
			t.Errorf("Expected 'v1', got %s", str)
		}

		v2, err := s.Get(ctx, "ns", "k2")
		if err != nil {
			t.Fatalf("Get k2 from store failed: %v", err)
		}
		str2, _ := v2.String()
		if str2 != "v2" {
			t.Errorf("Expected 'v2', got %s", str2)
		}
	}
}

func TestMultiStore_SetMany_AllFail(t *testing.T) {
	f1 := &failStore{setErr: errMock}
	f2 := &failStore{setErr: errMock}

	ms := NewStore([]config.Store{f1, f2})
	ctx := context.Background()

	err := ms.SetMany(ctx, "ns", map[string]config.Value{"k": config.NewValue("v")})
	if err == nil {
		t.Fatal("Expected error when all stores fail SetMany")
	}
}

func TestMultiStore_SetMany_PartialFailure(t *testing.T) {
	f1 := &failStore{setErr: errMock}
	store2 := memory.NewStore()

	ctx := context.Background()
	ms := NewStore([]config.Store{f1, store2})
	_ = ms.Connect(ctx)

	err := ms.SetMany(ctx, "ns", map[string]config.Value{"k": config.NewValue("v")})
	if err != nil {
		t.Fatalf("SetMany should succeed if at least one store succeeds, got %v", err)
	}

	v, e := store2.Get(ctx, "ns", "k")
	if e != nil {
		t.Fatalf("Expected value in store2: %v", e)
	}
	s, _ := v.String()
	if s != "v" {
		t.Errorf("Expected 'v', got %s", s)
	}
}

func TestMultiStore_DeleteMany_EmptyStores(t *testing.T) {
	ms := NewStore(nil)
	ctx := context.Background()

	_, err := ms.DeleteMany(ctx, "ns", []string{"k"})
	if err != config.ErrStoreNotConnected {
		t.Errorf("Expected ErrStoreNotConnected, got %v", err)
	}
}

func TestMultiStore_DeleteMany_FromAll(t *testing.T) {
	store1 := memory.NewStore()
	store2 := memory.NewStore()

	ctx := context.Background()
	ms := NewStore([]config.Store{store1, store2})
	_ = ms.Connect(ctx)
	defer ms.Close(ctx)

	_, _ = ms.Set(ctx, "ns", "k1", config.NewValue("v1"))
	_, _ = ms.Set(ctx, "ns", "k2", config.NewValue("v2"))

	deleted, err := ms.DeleteMany(ctx, "ns", []string{"k1", "k2"})
	if err != nil {
		t.Fatalf("DeleteMany failed: %v", err)
	}
	if deleted != 2 {
		t.Errorf("Expected 2 deleted, got %d", deleted)
	}

	_, err = store1.Get(ctx, "ns", "k1")
	if !config.IsNotFound(err) {
		t.Error("Expected k1 to be deleted from store1")
	}
	_, err = store2.Get(ctx, "ns", "k2")
	if !config.IsNotFound(err) {
		t.Error("Expected k2 to be deleted from store2")
	}
}

func TestMultiStore_DeleteMany_NonBulkFallback(t *testing.T) {
	store1 := memory.NewStore()
	f2 := &failStore{}

	ctx := context.Background()
	ms := NewStore([]config.Store{store1, f2})
	_ = ms.Connect(ctx)

	_, _ = store1.Set(ctx, "ns", "k1", config.NewValue("v"))

	deleted, err := ms.DeleteMany(ctx, "ns", []string{"k1", "k2"})
	if err != nil {
		t.Fatalf("DeleteMany failed: %v", err)
	}
	if deleted < 1 {
		t.Errorf("Expected at least 1 deleted, got %d", deleted)
	}
}

func TestMultiStore_DeleteMany_BulkAllFail(t *testing.T) {
	f1 := &failBulkStore{failStore: failStore{deleteErr: errMock}, delManyErr: errMock}
	f2 := &failBulkStore{failStore: failStore{deleteErr: errMock}, delManyErr: errMock}

	ms := NewStore([]config.Store{f1, f2})
	ctx := context.Background()

	_, err := ms.DeleteMany(ctx, "ns", []string{"k"})
	if err == nil {
		t.Fatal("Expected error when all stores fail DeleteMany")
	}
}

type failBulkStore struct {
	failStore
	getManyErr error
	setManyErr error
	delManyErr error
}

func (f *failBulkStore) GetMany(ctx context.Context, namespace string, keys []string) (map[string]config.Value, error) {
	return nil, f.getManyErr
}

func (f *failBulkStore) SetMany(ctx context.Context, namespace string, values map[string]config.Value) error {
	return f.setManyErr
}

func (f *failBulkStore) DeleteMany(ctx context.Context, namespace string, keys []string) (int64, error) {
	if f.delManyErr != nil {
		return 0, f.delManyErr
	}
	return 0, nil
}

func TestMultiStore_GetMany_BulkStoreUsed(t *testing.T) {
	store1 := memory.NewStore()
	store2 := memory.NewStore()

	ctx := context.Background()
	ms := NewStore([]config.Store{store1, store2})
	_ = ms.Connect(ctx)
	defer ms.Close(ctx)

	_, _ = store1.Set(ctx, "ns", "k1", config.NewValue("v1"))
	_, _ = store1.Set(ctx, "ns", "k2", config.NewValue("v2"))
	_, _ = store1.Set(ctx, "ns", "k3", config.NewValue("v3"))

	results, err := ms.GetMany(ctx, "ns", []string{"k1", "k2", "k3", "k4"})
	if err != nil {
		t.Fatalf("GetMany failed: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
	}
}

func TestMultiStore_GetMany_NonBulkStoreIndividualGets(t *testing.T) {
	store1 := memory.NewStore()
	f2 := &failStore{}

	ctx := context.Background()
	ms := NewStore([]config.Store{store1, f2})
	_ = ms.Connect(ctx)

	_, _ = store1.Set(ctx, "ns", "k1", config.NewValue("v1"))
	_, _ = store1.Set(ctx, "ns", "k2", config.NewValue("v2"))

	results, err := ms.GetMany(ctx, "ns", []string{"k1", "k2", "k3"})
	if err != nil {
		t.Fatalf("GetMany failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}
}

func TestMultiStore_SetMany_NonBulkStoreFallback(t *testing.T) {
	f1 := &failStore{setErr: nil}
	store2 := memory.NewStore()

	ctx := context.Background()
	ms := NewStore([]config.Store{f1, store2})
	_ = ms.Connect(ctx)

	vals := map[string]config.Value{
		"k1": config.NewValue("v1"),
	}

	err := ms.SetMany(ctx, "ns", vals)
	if err != nil {
		t.Fatalf("SetMany failed: %v", err)
	}

	v, e := store2.Get(ctx, "ns", "k1")
	if e != nil {
		t.Fatalf("Expected store2 to have value: %v", e)
	}
	s, _ := v.String()
	if s != "v1" {
		t.Errorf("Expected 'v1', got %s", s)
	}
}

func TestMultiStore_SetMany_NonBulkStoreError(t *testing.T) {
	f1 := &failStore{setErr: errMock}
	f2 := &failStore{setErr: errMock}

	ms := NewStore([]config.Store{f1, f2})
	ctx := context.Background()

	vals := map[string]config.Value{
		"k1": config.NewValue("v1"),
	}

	err := ms.SetMany(ctx, "ns", vals)
	if err == nil {
		t.Fatal("Expected error when all non-bulk stores fail")
	}
}

func TestMultiStore_ReadThrough_PopulatesMultipleEarlierStores(t *testing.T) {
	cache1 := memory.NewStore()
	cache2 := memory.NewStore()
	backend := memory.NewStore()

	ctx := context.Background()
	ms := NewStore([]config.Store{cache1, cache2, backend}, WithStrategy(StrategyReadThrough))
	_ = ms.Connect(ctx)
	defer ms.Close(ctx)

	_, _ = backend.Set(ctx, "ns", "key", config.NewValue("deep-value"))

	got, err := ms.Get(ctx, "ns", "key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	s, _ := got.String()
	if s != "deep-value" {
		t.Errorf("Expected 'deep-value', got %s", s)
	}

	time.Sleep(10 * time.Millisecond)

	v1, err := cache1.Get(ctx, "ns", "key")
	if err != nil {
		t.Fatalf("cache1 should have value: %v", err)
	}
	s1, _ := v1.String()
	if s1 != "deep-value" {
		t.Errorf("Expected 'deep-value' in cache1, got %s", s1)
	}

	v2, err := cache2.Get(ctx, "ns", "key")
	if err != nil {
		t.Fatalf("cache2 should have value: %v", err)
	}
	s2, _ := v2.String()
	if s2 != "deep-value" {
		t.Errorf("Expected 'deep-value' in cache2, got %s", s2)
	}
}

func TestMultiStore_WithStrategy_Option(t *testing.T) {
	ms := NewStore(nil, WithStrategy(StrategyReadThrough))
	if ms.strategy != StrategyReadThrough {
		t.Errorf("Expected StrategyReadThrough, got %d", ms.strategy)
	}

	ms = NewStore(nil, WithStrategy(StrategyWriteThrough))
	if ms.strategy != StrategyWriteThrough {
		t.Errorf("Expected StrategyWriteThrough, got %d", ms.strategy)
	}

	ms = NewStore(nil)
	if ms.strategy != StrategyFallback {
		t.Errorf("Expected default StrategyFallback, got %d", ms.strategy)
	}
}

func TestMultiStore_Stores_ReturnsCopy(t *testing.T) {
	store1 := memory.NewStore()
	store2 := memory.NewStore()

	ms := NewStore([]config.Store{store1, store2})
	stores := ms.Stores()

	stores[0] = nil

	if ms.stores[0] == nil {
		t.Error("Mutating returned slice should not affect internal stores")
	}
}

func TestMultiStore_GetMany_ReadThrough_PopulatesBulkCache(t *testing.T) {
	f1 := &failBulkStore{getManyErr: errMock}
	backend := memory.NewStore()

	ctx := context.Background()
	ms := NewStore([]config.Store{f1, backend}, WithStrategy(StrategyReadThrough))
	_ = ms.Connect(ctx)

	_, _ = backend.Set(ctx, "ns", "a", config.NewValue("va"))
	_, _ = backend.Set(ctx, "ns", "b", config.NewValue("vb"))

	results, err := ms.GetMany(ctx, "ns", []string{"a", "b"})
	if err != nil {
		t.Fatalf("GetMany failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}
}

func TestMultiStore_GetMany_ReadThrough_NonBulkCachePopulated(t *testing.T) {
	f1 := &failBulkStore{getManyErr: errMock, setManyErr: errMock}
	backend := memory.NewStore()

	ctx := context.Background()
	ms := NewStore([]config.Store{f1, backend}, WithStrategy(StrategyReadThrough))
	_ = ms.Connect(ctx)

	_, _ = backend.Set(ctx, "ns", "k1", config.NewValue("v1"))

	results, err := ms.GetMany(ctx, "ns", []string{"k1"})
	if err != nil {
		t.Fatalf("GetMany failed: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}
}

func TestMultiStore_GetMany_NonBulkStoreReturnsEmpty(t *testing.T) {
	f1 := &failStore{getErr: config.ErrStoreNotConnected}
	backend := memory.NewStore()

	ctx := context.Background()
	ms := NewStore([]config.Store{f1, backend})
	_ = ms.Connect(ctx)

	_, _ = backend.Set(ctx, "ns", "k1", config.NewValue("v1"))

	results, err := ms.GetMany(ctx, "ns", []string{"k1"})
	if err != nil {
		t.Fatalf("GetMany failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("Expected 0 results (non-bulk store returns empty on Get errors), got %d", len(results))
	}
}

func TestMultiStore_SetMany_BulkStoreError(t *testing.T) {
	f1 := &failBulkStore{setManyErr: errMock}
	store2 := memory.NewStore()

	ctx := context.Background()
	ms := NewStore([]config.Store{f1, store2})
	_ = ms.Connect(ctx)

	err := ms.SetMany(ctx, "ns", map[string]config.Value{"k": config.NewValue("v")})
	if err != nil {
		t.Fatalf("SetMany should succeed when at least one store works, got %v", err)
	}
}

func TestMultiStore_DeleteMany_MaxDeleted(t *testing.T) {
	store1 := memory.NewStore()
	store2 := memory.NewStore()

	ctx := context.Background()
	ms := NewStore([]config.Store{store1, store2})
	_ = ms.Connect(ctx)
	defer ms.Close(ctx)

	_, _ = store1.Set(ctx, "ns", "k1", config.NewValue("v"))
	_, _ = store2.Set(ctx, "ns", "k1", config.NewValue("v"))
	_, _ = store2.Set(ctx, "ns", "k2", config.NewValue("v"))

	deleted, err := ms.DeleteMany(ctx, "ns", []string{"k1", "k2"})
	if err != nil {
		t.Fatalf("DeleteMany failed: %v", err)
	}
	if deleted != 2 {
		t.Errorf("Expected max deleted=2, got %d", deleted)
	}
}
