package live

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbaliyan/config"
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
		mgr.Close(context.Background())
	})
	return mgr.Namespace("test")
}

type DatabaseConfig struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

func TestBind(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	// Set up initial config
	cfg.Set(ctx, "database/host", "localhost")
	cfg.Set(ctx, "database/port", 5432)

	var dbConfig DatabaseConfig
	binding, err := Bind(ctx, cfg, "database", &dbConfig,
		WithPollInterval(100*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("Bind failed: %v", err)
	}
	defer binding.Stop()

	// Verify initial load using safe Get() access
	binding.Get(func(target any) {
		cfg := target.(*DatabaseConfig)
		if cfg.Host != "localhost" {
			t.Errorf("expected host=localhost, got %s", cfg.Host)
		}
		if cfg.Port != 5432 {
			t.Errorf("expected port=5432, got %d", cfg.Port)
		}
	})

	// Verify LastReload is set
	if binding.LastReload().IsZero() {
		t.Error("expected LastReload to be set")
	}

	// Verify no error
	if binding.LastError() != nil {
		t.Errorf("expected no error, got %v", binding.LastError())
	}
}

func TestBindInvalidTarget(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	// Test with non-pointer
	var dbConfig DatabaseConfig
	_, err := Bind(ctx, cfg, "database", dbConfig)
	if err != ErrInvalidTarget {
		t.Errorf("expected ErrInvalidTarget, got %v", err)
	}

	// Test with pointer to non-struct
	var str string
	_, err = Bind(ctx, cfg, "database", &str)
	if err != ErrInvalidTarget {
		t.Errorf("expected ErrInvalidTarget, got %v", err)
	}
}

func TestBindAutoReload(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	// Set up initial config
	cfg.Set(ctx, "database/host", "localhost")
	cfg.Set(ctx, "database/port", 5432)

	var reloadCount atomic.Int32
	var dbConfig DatabaseConfig

	binding, err := Bind(ctx, cfg, "database", &dbConfig,
		WithPollInterval(50*time.Millisecond),
		WithOnReload(func() {
			reloadCount.Add(1)
		}),
	)
	if err != nil {
		t.Fatalf("Bind failed: %v", err)
	}
	defer binding.Stop()

	// Initial load counts as one reload
	if reloadCount.Load() != 1 {
		t.Errorf("expected initial reload count=1, got %d", reloadCount.Load())
	}

	// Update config
	cfg.Set(ctx, "database/host", "remotehost")
	cfg.Set(ctx, "database/port", 5433)

	// Wait for auto-reload
	time.Sleep(150 * time.Millisecond)

	// Should have reloaded at least twice (initial + at least one poll)
	if reloadCount.Load() < 2 {
		t.Errorf("expected reload count >= 2, got %d", reloadCount.Load())
	}

	// Verify updated values
	binding.Get(func(target any) {
		cfg := target.(*DatabaseConfig)
		if cfg.Host != "remotehost" {
			t.Errorf("expected host=remotehost, got %s", cfg.Host)
		}
		if cfg.Port != 5433 {
			t.Errorf("expected port=5433, got %d", cfg.Port)
		}
	})
}

func TestBindStop(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	cfg.Set(ctx, "database/host", "localhost")
	cfg.Set(ctx, "database/port", 5432)

	var reloadCount atomic.Int32
	var dbConfig DatabaseConfig

	binding, err := Bind(ctx, cfg, "database", &dbConfig,
		WithPollInterval(50*time.Millisecond),
		WithOnReload(func() {
			reloadCount.Add(1)
		}),
	)
	if err != nil {
		t.Fatalf("Bind failed: %v", err)
	}

	// Stop the binding
	binding.Stop()

	// Get reload count after stop
	countAfterStop := reloadCount.Load()

	// Wait for what would be multiple poll cycles
	time.Sleep(200 * time.Millisecond)

	// Reload count should not have increased
	if reloadCount.Load() != countAfterStop {
		t.Errorf("expected reload count to stay at %d after stop, got %d",
			countAfterStop, reloadCount.Load())
	}

	// Stop should be safe to call multiple times
	binding.Stop()
	binding.Stop()
}

func TestBindReloadNow(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	cfg.Set(ctx, "database/host", "localhost")
	cfg.Set(ctx, "database/port", 5432)

	var dbConfig DatabaseConfig
	binding, err := Bind(ctx, cfg, "database", &dbConfig,
		WithPollInterval(10*time.Second), // Long poll interval
	)
	if err != nil {
		t.Fatalf("Bind failed: %v", err)
	}
	defer binding.Stop()

	// Update config
	cfg.Set(ctx, "database/host", "newhost")

	// Force immediate reload
	if err := binding.ReloadNow(ctx); err != nil {
		t.Fatalf("ReloadNow failed: %v", err)
	}

	// Verify updated value using safe Get() access
	binding.Get(func(target any) {
		cfg := target.(*DatabaseConfig)
		if cfg.Host != "newhost" {
			t.Errorf("expected host=newhost, got %s", cfg.Host)
		}
	})
}

func TestBindOnError(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	cfg.Set(ctx, "database/host", "localhost")
	cfg.Set(ctx, "database/port", 5432)

	var errorCount atomic.Int32
	var dbConfig DatabaseConfig

	binding, err := Bind(ctx, cfg, "database", &dbConfig,
		WithPollInterval(50*time.Millisecond),
		WithOnError(func(err error) {
			errorCount.Add(1)
		}),
	)
	if err != nil {
		t.Fatalf("Bind failed: %v", err)
	}
	defer binding.Stop()

	// Delete the config keys to cause an error on next reload
	cfg.Delete(ctx, "database/host")
	cfg.Delete(ctx, "database/port")

	// Wait for poll to detect error
	time.Sleep(150 * time.Millisecond)

	// Should have encountered at least one error
	if errorCount.Load() < 1 {
		t.Errorf("expected at least one error, got %d", errorCount.Load())
	}

	// LastError should be set
	if binding.LastError() == nil {
		t.Error("expected LastError to be set")
	}
}

func TestBindGet(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	cfg.Set(ctx, "database/host", "localhost")
	cfg.Set(ctx, "database/port", 5432)

	var dbConfig DatabaseConfig
	binding, err := Bind(ctx, cfg, "database", &dbConfig,
		WithPollInterval(100*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("Bind failed: %v", err)
	}
	defer binding.Stop()

	// Use Get for safe access
	var host string
	var port int
	binding.Get(func(target any) {
		cfg := target.(*DatabaseConfig)
		host = cfg.Host
		port = cfg.Port
	})

	if host != "localhost" {
		t.Errorf("expected host=localhost, got %s", host)
	}
	if port != 5432 {
		t.Errorf("expected port=5432, got %d", port)
	}
}

type NestedConfig struct {
	Database DatabaseConfig `json:"database"`
	Cache    CacheConfig    `json:"cache"`
}

type CacheConfig struct {
	TTL     int  `json:"ttl"`
	Enabled bool `json:"enabled"`
}

func TestBindNestedStruct(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	cfg.Set(ctx, "app/database/host", "localhost")
	cfg.Set(ctx, "app/database/port", 5432)
	cfg.Set(ctx, "app/cache/ttl", 300)
	cfg.Set(ctx, "app/cache/enabled", true)

	var appConfig NestedConfig
	binding, err := Bind(ctx, cfg, "app", &appConfig,
		WithPollInterval(100*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("Bind failed: %v", err)
	}
	defer binding.Stop()

	// Verify using safe Get() access
	binding.Get(func(target any) {
		cfg := target.(*NestedConfig)
		if cfg.Database.Host != "localhost" {
			t.Errorf("expected database.host=localhost, got %s", cfg.Database.Host)
		}
		if cfg.Database.Port != 5432 {
			t.Errorf("expected database.port=5432, got %d", cfg.Database.Port)
		}
		if cfg.Cache.TTL != 300 {
			t.Errorf("expected cache.ttl=300, got %d", cfg.Cache.TTL)
		}
		if !cfg.Cache.Enabled {
			t.Error("expected cache.enabled=true")
		}
	})
}
