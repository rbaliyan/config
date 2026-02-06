package live

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/bind"
	"github.com/rbaliyan/config/memory"
)

func setupTestConfig(t *testing.T) config.Config {
	t.Helper()
	store := memory.NewStore()
	mgr, err := config.New(config.WithStore(store))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := mgr.Connect(context.Background()); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	t.Cleanup(func() {
		_ = mgr.Close(context.Background())
	})
	return mgr.Namespace("test")
}

// refDBConfig is a test struct for Ref tests.
type refDBConfig struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

func TestRef_InitialLoad(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	_ = cfg.Set(ctx, "database/host", "localhost")
	_ = cfg.Set(ctx, "database/port", 5432)

	ref, err := New[refDBConfig](ctx, cfg, "database",
		WithRefPollInterval[refDBConfig](time.Second),
	)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer ref.Close()

	snap := ref.Load()
	if snap == nil {
		t.Fatal("Load returned nil")
	}
	if snap.Host != "localhost" {
		t.Errorf("expected host=localhost, got %s", snap.Host)
	}
	if snap.Port != 5432 {
		t.Errorf("expected port=5432, got %d", snap.Port)
	}

	// Observability
	if ref.LastReload().IsZero() {
		t.Error("expected LastReload to be set")
	}
	if ref.LastError() != nil {
		t.Errorf("expected no error, got %v", ref.LastError())
	}
	if ref.ReloadCount() != 0 {
		t.Errorf("expected reload count=0 (initial load doesn't count), got %d", ref.ReloadCount())
	}
}

func TestRef_InvalidTarget(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	_, err := New[string](ctx, cfg, "database")
	if err == nil {
		t.Fatal("expected error for non-struct type parameter")
	}
}

func TestRef_AutoReload(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	_ = cfg.Set(ctx, "database/host", "localhost")
	_ = cfg.Set(ctx, "database/port", 5432)

	ref, err := New[refDBConfig](ctx, cfg, "database",
		WithRefPollInterval[refDBConfig](50*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer ref.Close()

	// Verify initial
	snap := ref.Load()
	if snap.Host != "localhost" {
		t.Fatalf("initial host=%s, want localhost", snap.Host)
	}

	// Update config
	_ = cfg.Set(ctx, "database/host", "remotehost")
	_ = cfg.Set(ctx, "database/port", 5433)

	// Wait for poll cycle
	time.Sleep(200 * time.Millisecond)

	snap = ref.Load()
	if snap.Host != "remotehost" {
		t.Errorf("expected host=remotehost, got %s", snap.Host)
	}
	if snap.Port != 5433 {
		t.Errorf("expected port=5433, got %d", snap.Port)
	}
	if ref.ReloadCount() < 1 {
		t.Errorf("expected reload count >= 1, got %d", ref.ReloadCount())
	}
}

func TestRef_OnChange(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	_ = cfg.Set(ctx, "database/host", "localhost")
	_ = cfg.Set(ctx, "database/port", 5432)

	var mu sync.Mutex
	var gotOld, gotNew refDBConfig
	var callCount atomic.Int32

	ref, err := New[refDBConfig](ctx, cfg, "database",
		WithRefPollInterval[refDBConfig](50*time.Millisecond),
		OnChange(func(oldVal, newVal refDBConfig) {
			mu.Lock()
			gotOld = oldVal
			gotNew = newVal
			mu.Unlock()
			callCount.Add(1)
		}),
	)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer ref.Close()

	// Update config
	_ = cfg.Set(ctx, "database/host", "newhost")
	_ = cfg.Set(ctx, "database/port", 9999)

	// Wait for change detection
	time.Sleep(200 * time.Millisecond)

	if callCount.Load() < 1 {
		t.Fatal("expected OnChange to be called at least once")
	}

	mu.Lock()
	defer mu.Unlock()
	// The "old" should have been localhost at some point
	// The "new" should have newhost
	if gotNew.Host != "newhost" {
		t.Errorf("expected new host=newhost, got %s", gotNew.Host)
	}
	if gotNew.Port != 9999 {
		t.Errorf("expected new port=9999, got %d", gotNew.Port)
	}
	// Old should still reflect the previous value
	if gotOld.Host == "" {
		t.Error("expected old host to be set")
	}
}

func TestRef_OnError(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	_ = cfg.Set(ctx, "database/host", "localhost")
	_ = cfg.Set(ctx, "database/port", 5432)

	var errorCount atomic.Int32

	ref, err := New[refDBConfig](ctx, cfg, "database",
		WithRefPollInterval[refDBConfig](50*time.Millisecond),
		OnError[refDBConfig](func(err error) {
			errorCount.Add(1)
		}),
	)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer ref.Close()

	// Delete keys to cause error on next reload
	_ = cfg.Delete(ctx, "database/host")
	_ = cfg.Delete(ctx, "database/port")

	// Wait for poll
	time.Sleep(200 * time.Millisecond)

	// Old snapshot should be preserved
	snap := ref.Load()
	if snap == nil {
		t.Fatal("Load returned nil after error — old snapshot should be preserved")
	}
	if snap.Host != "localhost" {
		t.Errorf("expected preserved host=localhost, got %s", snap.Host)
	}

	// LastError should be set
	if ref.LastError() == nil {
		t.Error("expected LastError to be set after reload failure")
	}
}

func TestRef_Close(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	_ = cfg.Set(ctx, "database/host", "localhost")
	_ = cfg.Set(ctx, "database/port", 5432)

	ref, err := New[refDBConfig](ctx, cfg, "database",
		WithRefPollInterval[refDBConfig](50*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	// Close stops polling
	ref.Close()

	countAfter := ref.ReloadCount()
	time.Sleep(200 * time.Millisecond)

	if ref.ReloadCount() != countAfter {
		t.Errorf("expected reload count to stay at %d after Close, got %d",
			countAfter, ref.ReloadCount())
	}

	// Load still works after Close
	snap := ref.Load()
	if snap == nil {
		t.Fatal("Load returned nil after Close")
	}
	if snap.Host != "localhost" {
		t.Errorf("expected host=localhost after Close, got %s", snap.Host)
	}

	// Idempotent Close
	ref.Close()
	ref.Close()
}

func TestRef_ReloadNow(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	_ = cfg.Set(ctx, "database/host", "localhost")
	_ = cfg.Set(ctx, "database/port", 5432)

	ref, err := New[refDBConfig](ctx, cfg, "database",
		WithRefPollInterval[refDBConfig](10*time.Second), // long interval
	)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer ref.Close()

	// Update config
	_ = cfg.Set(ctx, "database/host", "forced-host")

	// Force immediate reload
	if err := ref.ReloadNow(ctx); err != nil {
		t.Fatalf("ReloadNow failed: %v", err)
	}

	snap := ref.Load()
	if snap.Host != "forced-host" {
		t.Errorf("expected host=forced-host, got %s", snap.Host)
	}
	if ref.ReloadCount() != 1 {
		t.Errorf("expected reload count=1, got %d", ref.ReloadCount())
	}
}

func TestRef_ConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	_ = cfg.Set(ctx, "database/host", "localhost")
	_ = cfg.Set(ctx, "database/port", 5432)

	ref, err := New[refDBConfig](ctx, cfg, "database",
		WithRefPollInterval[refDBConfig](20*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer ref.Close()

	// Spawn 50 goroutines reading concurrently while config changes
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				snap := ref.Load()
				if snap == nil {
					t.Error("Load returned nil during concurrent access")
					return
				}
				// Just read fields to exercise the race detector
				_ = snap.Host
				_ = snap.Port
			}
		}()
	}

	// Meanwhile, mutate config
	for i := 0; i < 10; i++ {
		_ = cfg.Set(ctx, "database/port", 6000+i)
		time.Sleep(10 * time.Millisecond)
	}

	wg.Wait()
}

type refNestedConfig struct {
	Database refDBConfig `json:"database"`
	Cache    struct {
		TTL     int  `json:"ttl"`
		Enabled bool `json:"enabled"`
	} `json:"cache"`
}

func TestRef_NestedStruct(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	_ = cfg.Set(ctx, "app/database/host", "dbhost")
	_ = cfg.Set(ctx, "app/database/port", 3306)
	_ = cfg.Set(ctx, "app/cache/ttl", 300)
	_ = cfg.Set(ctx, "app/cache/enabled", true)

	ref, err := New[refNestedConfig](ctx, cfg, "app",
		WithRefPollInterval[refNestedConfig](time.Second),
	)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer ref.Close()

	snap := ref.Load()
	if snap.Database.Host != "dbhost" {
		t.Errorf("expected database.host=dbhost, got %s", snap.Database.Host)
	}
	if snap.Database.Port != 3306 {
		t.Errorf("expected database.port=3306, got %d", snap.Database.Port)
	}
	if snap.Cache.TTL != 300 {
		t.Errorf("expected cache.ttl=300, got %d", snap.Cache.TTL)
	}
	if !snap.Cache.Enabled {
		t.Error("expected cache.enabled=true")
	}
}

type taggedConfig struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

func TestRef_WithBindOptions(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	_ = cfg.Set(ctx, "svc/host", "tagged-host")
	_ = cfg.Set(ctx, "svc/port", 7777)

	ref, err := New[taggedConfig](ctx, cfg, "svc",
		WithRefPollInterval[taggedConfig](time.Second),
		WithBindOptions[taggedConfig](bind.WithFieldTag("yaml")),
	)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer ref.Close()

	snap := ref.Load()
	if snap.Host != "tagged-host" {
		t.Errorf("expected host=tagged-host, got %s", snap.Host)
	}
	if snap.Port != 7777 {
		t.Errorf("expected port=7777, got %d", snap.Port)
	}
}

func TestRef_OnChangeSkipsUnchanged(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	_ = cfg.Set(ctx, "database/host", "localhost")
	_ = cfg.Set(ctx, "database/port", 5432)

	var callCount atomic.Int32

	ref, err := New[refDBConfig](ctx, cfg, "database",
		WithRefPollInterval[refDBConfig](50*time.Millisecond),
		OnChange(func(oldVal, newVal refDBConfig) {
			callCount.Add(1)
		}),
	)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer ref.Close()

	// Wait several poll cycles with NO config changes
	time.Sleep(300 * time.Millisecond)

	// onChange should NOT have been called — config is unchanged
	if callCount.Load() != 0 {
		t.Errorf("expected onChange call count=0 when config unchanged, got %d", callCount.Load())
	}

	// ReloadCount should also be 0 — no actual changes detected
	if ref.ReloadCount() != 0 {
		t.Errorf("expected reload count=0 when config unchanged, got %d", ref.ReloadCount())
	}

	// Now make an actual change
	_ = cfg.Set(ctx, "database/host", "changed-host")
	time.Sleep(200 * time.Millisecond)

	// onChange should have been called exactly once (for the change)
	if callCount.Load() < 1 {
		t.Errorf("expected onChange to be called after config change, got %d calls", callCount.Load())
	}
	if ref.ReloadCount() < 1 {
		t.Errorf("expected reload count >= 1 after config change, got %d", ref.ReloadCount())
	}

	snap := ref.Load()
	if snap.Host != "changed-host" {
		t.Errorf("expected host=changed-host, got %s", snap.Host)
	}
}
