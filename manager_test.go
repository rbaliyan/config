package config_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/internal/testutil"
	"github.com/rbaliyan/config/memory"
)

func TestManagerBasicOperations(t *testing.T) {
	ctx := context.Background()

	// Create manager with memory store
	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	// Connect
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	// Get config for a namespace
	cfg := mgr.Namespace("test")

	// Set a value
	if err := cfg.Set(ctx, "app/timeout", 30); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Get the value
	val, err := cfg.Get(ctx, "app/timeout")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Use Value interface methods
	i, err := val.Int64()
	if err != nil {
		t.Fatalf("Int64 failed: %v", err)
	}
	if i != 30 {
		t.Errorf("Expected 30, got %d", i)
	}
}

func TestNamespaceIsolation(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	// Set same key in different namespaces
	prod := mgr.Namespace("production")
	dev := mgr.Namespace("development")

	if err := prod.Set(ctx, "timeout", 60); err != nil {
		t.Fatalf("Set prod failed: %v", err)
	}
	if err := dev.Set(ctx, "timeout", 10); err != nil {
		t.Fatalf("Set dev failed: %v", err)
	}

	// Verify values are isolated
	prodVal, _ := prod.Get(ctx, "timeout")
	devVal, _ := dev.Get(ctx, "timeout")

	prodInt, _ := prodVal.Int64()
	if prodInt != 60 {
		t.Errorf("Production timeout expected 60, got %d", prodInt)
	}
	devInt, _ := devVal.Int64()
	if devInt != 10 {
		t.Errorf("Development timeout expected 10, got %d", devInt)
	}
}

func TestDelete(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("test")

	// Set a value
	if err := cfg.Set(ctx, "to-delete", "value"); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Delete the value
	if err := cfg.Delete(ctx, "to-delete"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify it's gone
	_, getErr := cfg.Get(ctx, "to-delete")
	if !config.IsNotFound(getErr) {
		t.Errorf("Expected ErrNotFound, got %v", err)
	}
}

// TestManagerHealth verifies the Health() method
func TestManagerHealth(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	// Health should fail before connect
	if err := mgr.Health(ctx); err != config.ErrManagerClosed {
		t.Errorf("Expected ErrManagerClosed before connect, got: %v", err)
	}

	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	// Health should succeed after connect
	if err := mgr.Health(ctx); err != nil {
		t.Errorf("Health should succeed when connected, got: %v", err)
	}

	_ = mgr.Close(ctx)

	// Health should fail after close
	if err := mgr.Health(ctx); err != config.ErrManagerClosed {
		t.Errorf("Expected ErrManagerClosed after close, got: %v", err)
	}
}

// TestManagerRefresh verifies the Refresh() method
func TestManagerRefresh(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("test")

	// Set a value
	_ = cfg.Set(ctx, "app/timeout", 30)

	// Refresh should succeed for existing key
	if err := mgr.Refresh(ctx, "test", "app/timeout"); err != nil {
		t.Errorf("Refresh failed for existing key: %v", err)
	}

	// Refresh should fail for non-existing key
	err = mgr.Refresh(ctx, "test", "nonexistent")
	if !config.IsNotFound(err) {
		t.Errorf("Refresh should return NotFound for nonexistent key, got: %v", err)
	}
}

// TestClosedManagerOperations verifies errors on closed manager
func TestClosedManagerOperations(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	cfg := mgr.Namespace("test")
	_ = cfg.Set(ctx, "key", "value")

	// Close the manager
	_ = mgr.Close(ctx)

	// All operations should fail
	_, err = cfg.Get(ctx, "key")
	if err != config.ErrManagerClosed {
		t.Errorf("Get on closed manager should return ErrManagerClosed, got: %v", err)
	}

	err = cfg.Set(ctx, "key2", "value2")
	if err != config.ErrManagerClosed {
		t.Errorf("Set on closed manager should return ErrManagerClosed, got: %v", err)
	}

	err = cfg.Delete(ctx, "key")
	if err != config.ErrManagerClosed {
		t.Errorf("Delete on closed manager should return ErrManagerClosed, got: %v", err)
	}

	_, err = cfg.Find(ctx, config.NewFilter().Build())
	if err != config.ErrManagerClosed {
		t.Errorf("Find on closed manager should return ErrManagerClosed, got: %v", err)
	}

	err = mgr.Refresh(ctx, "test", "key")
	if err != config.ErrManagerClosed {
		t.Errorf("Refresh on closed manager should return ErrManagerClosed, got: %v", err)
	}
}

// TestManagerConnectWithoutStore verifies connect fails without store
func TestManagerConnectWithoutStore(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New() // No store
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	err = mgr.Connect(ctx)
	if err != config.ErrStoreNotConnected {
		t.Errorf("Connect without store should return ErrStoreNotConnected, got: %v", err)
	}
}

// TestConcurrentAccess tests concurrent read/write operations
func TestConcurrentAccess(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("test")

	// Set initial value
	_ = cfg.Set(ctx, "counter", 0)

	var wg sync.WaitGroup
	numReaders := 10
	numWriters := 5
	iterations := 100

	// Start readers
	for range numReaders {
		wg.Go(func() {
			for range iterations {
				_, _ = cfg.Get(ctx, "counter")
			}
		})
	}

	// Start writers
	var writeCount atomic.Int64
	for range numWriters {
		wg.Go(func() {
			for range iterations {
				count := writeCount.Add(1)
				_ = cfg.Set(ctx, "counter", count)
			}
		})
	}

	wg.Wait()

	// Verify final value exists
	val, err := cfg.Get(ctx, "counter")
	if err != nil {
		t.Fatalf("Final Get failed: %v", err)
	}

	i, _ := val.Int64()
	if i == 0 {
		t.Error("Counter should have been incremented")
	}
}

// TestNamespaceReuse verifies namespace instances are reused
func TestNamespaceReuse(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	// Get same namespace multiple times
	cfg1 := mgr.Namespace("test")
	cfg2 := mgr.Namespace("test")

	// They should be the same instance
	if cfg1 != cfg2 {
		t.Error("Expected same namespace instance to be reused")
	}

	// Different namespaces should be different
	cfg3 := mgr.Namespace("other")
	if cfg1 == cfg3 {
		t.Error("Different namespaces should return different instances")
	}
}

func TestWatchStatus(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	obs, ok := mgr.(config.ManagerObserver)
	if !ok {
		t.Fatal("Manager should implement ManagerObserver")
	}

	ws := obs.WatchStatus()
	if !ws.Connected {
		t.Error("Expected Connected = true")
	}
}

func TestManagerConnectTwice(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	// First connect
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("First Connect failed: %v", err)
	}

	// Second connect should be idempotent (already connected)
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Second Connect failed: %v", err)
	}

	_ = mgr.Close(ctx)

	// Connect after close should fail
	err = mgr.Connect(ctx)
	if !errors.Is(err, config.ErrManagerClosed) {
		t.Errorf("Connect after Close = %v, want ErrManagerClosed", err)
	}
}

// TestProcessWatchEventsSetAndDelete tests that watch events update the cache.
func TestProcessWatchEventsSetAndDelete(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("test")

	// Set and Get a value
	_ = cfg.Set(ctx, "app/name", "myapp")
	val, err := cfg.Get(ctx, "app/name")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	s, _ := val.String()
	if s != "myapp" {
		t.Errorf("Expected 'myapp', got %q", s)
	}

	// Delete it
	if err := cfg.Delete(ctx, "app/name"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Wait for the deletion to be observable (the cache is invalidated via the
	// watch stream, so poll instead of sleeping a fixed interval).
	testutil.WaitUntil(t, 2*time.Second, func() bool {
		_, getErr := cfg.Get(ctx, "app/name")
		return config.IsNotFound(getErr)
	}, "expected NotFound after delete")
}

// TestManagerCloseTwice tests that closing a manager twice is safe.
func TestManagerCloseTwice(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	// First close
	err = mgr.Close(ctx)
	if err != nil {
		t.Fatalf("First Close failed: %v", err)
	}

	// Second close should be safe (no-op)
	err = mgr.Close(ctx)
	if err != nil {
		t.Fatalf("Second Close should not error, got: %v", err)
	}
}

// TestManagerCloseBeforeConnect tests closing a manager that was never connected.
func TestManagerCloseBeforeConnect(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	// Close without connecting should be safe (no-op)
	err = mgr.Close(ctx)
	if err != nil {
		t.Fatalf("Close before connect should not error, got: %v", err)
	}
}

// TestWatchReconnectsAfterFailure tests that watchChanges retries on failure.
func TestWatchReconnectsAfterFailure(t *testing.T) {
	ctx := context.Background()

	underlying := memory.NewStore()
	store := &watchFailStore{Store: underlying}
	store.watchFails.Store(2) // Fail first 2 watch attempts

	mgr, err := config.New(
		config.WithStore(store),
		config.WithWatchInitialBackoff(10*time.Millisecond),
		config.WithWatchMaxBackoff(50*time.Millisecond),
		config.WithWatchBackoffFactor(2.0),
	)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	// After recovery, watch should work - verify via observer
	obs, ok := mgr.(config.ManagerObserver)
	if !ok {
		t.Fatal("Manager should implement ManagerObserver")
	}

	// Wait for the watch to recover (failures reset to 0) instead of sleeping a
	// fixed interval tuned to the backoff schedule.
	testutil.WaitUntil(t, 2*time.Second, func() bool {
		return obs.WatchStatus().ConsecutiveFailures == 0
	}, "watch should recover and reset ConsecutiveFailures to 0")

	_ = mgr.Close(ctx)
}

// TestWatchNotSupported tests that watch exits gracefully when not supported.
func TestWatchNotSupported(t *testing.T) {
	ctx := context.Background()

	store := &watchNotSupportedStore{Store: memory.NewStore()}
	mgr, err := config.New(config.WithStore(store))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	// Give watch goroutine time to exit
	time.Sleep(50 * time.Millisecond)

	// Close should work cleanly
	_ = mgr.Close(ctx)
}

// TestHealthWithHealthChecker tests Health when store implements HealthChecker.
func TestHealthWithHealthChecker(t *testing.T) {
	ctx := context.Background()

	hcStore := &healthCheckStore{
		Store:     memory.NewStore(),
		healthErr: nil,
	}

	mgr, err := config.New(config.WithStore(hcStore))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	// Healthy
	if err := mgr.Health(ctx); err != nil {
		t.Errorf("Health should be nil, got: %v", err)
	}

	// Unhealthy store
	hcStore.healthErr = errors.New("store unhealthy")
	if err := mgr.Health(ctx); err == nil {
		t.Error("Health should return error when store is unhealthy")
	}
}

// TestHealthWithWatchFailures tests Health when watch has consecutive failures.
func TestHealthWithWatchFailures(t *testing.T) {
	ctx := context.Background()

	underlying := memory.NewStore()
	store := &watchFailStore{Store: underlying}
	// Make watch fail many times
	store.watchFails.Store(100)

	mgr, err := config.New(
		config.WithStore(store),
		config.WithWatchInitialBackoff(5*time.Millisecond),
		config.WithWatchMaxBackoff(10*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	// Wait for at least 3 failures to accumulate (Health reports unhealthy)
	// rather than sleeping a fixed interval.
	testutil.WaitUntil(t, 2*time.Second, func() bool {
		return config.IsWatchUnhealthy(mgr.Health(ctx))
	}, "Health should report watch unhealthy after repeated failures")

	err = mgr.Health(ctx)
	if err == nil {
		t.Error("Health should fail with watch failures")
	}
	if !config.IsWatchUnhealthy(err) {
		t.Errorf("Expected watch unhealthy error, got: %v", err)
	}

	// Check WatchStatus shows failures
	obs := mgr.(config.ManagerObserver)
	ws := obs.WatchStatus()
	if ws.ConsecutiveFailures < 3 {
		t.Errorf("ConsecutiveFailures = %d, want >= 3", ws.ConsecutiveFailures)
	}
	if ws.LastError == "" {
		t.Error("LastError should not be empty after failures")
	}
	if ws.LastAttempt.IsZero() {
		t.Error("LastAttempt should not be zero")
	}
}

// TestWatchStatusAfterClose tests WatchStatus after manager is closed.
func TestWatchStatusAfterClose(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	obs := mgr.(config.ManagerObserver)

	_ = mgr.Close(ctx)

	ws := obs.WatchStatus()
	if ws.Connected {
		t.Error("Expected Connected = false after close")
	}
}

// TestIsWatchUnhealthyFalse tests IsWatchUnhealthy with non-matching error.
func TestIsWatchUnhealthyFalse(t *testing.T) {
	if config.IsWatchUnhealthy(errors.New("other")) {
		t.Error("IsWatchUnhealthy should return false for random error")
	}
}

// TestConcurrentNamespaceCreation tests concurrent namespace creation.
func TestConcurrentNamespaceCreation(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	var wg sync.WaitGroup
	results := make([]config.Config, 20)

	for i := range 20 {
		wg.Go(func() {
			results[i] = mgr.Namespace("concurrent")
		})
	}
	wg.Wait()

	// All should return the same instance
	for i := range 20 {
		if results[i] != results[0] {
			t.Error("All concurrent Namespace calls should return the same instance")
			break
		}
	}
}

// TestRefreshNotFoundClearsCache tests that refreshing a deleted key clears cache.
func TestRefreshNotFoundClearsCache(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("test")

	// Set and get (populates cache)
	_ = cfg.Set(ctx, "temp-key", "temp-val")
	_, _ = cfg.Get(ctx, "temp-key")

	// Delete directly from store, then refresh
	_ = cfg.Delete(ctx, "temp-key")

	// Refresh should return NotFound (key is deleted)
	err = mgr.Refresh(ctx, "test", "temp-key")
	if !config.IsNotFound(err) {
		t.Errorf("Refresh after delete should return NotFound, got: %v", err)
	}
}

// TestRefreshBeforeConnect tests that Refresh fails when not connected.
func TestRefreshBeforeConnect(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	err = mgr.Refresh(ctx, "test", "key")
	if err != config.ErrManagerClosed {
		t.Errorf("Refresh before connect = %v, want ErrManagerClosed", err)
	}
}
