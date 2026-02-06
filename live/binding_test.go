package live

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/memory"
)

type bindDBConfig struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

func setupBindingConfig(t *testing.T) config.Config {
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

func TestBinding_InitialLoad(t *testing.T) {
	ctx := context.Background()
	cfg := setupBindingConfig(t)

	cfg.Set(ctx, "db/host", "localhost")
	cfg.Set(ctx, "db/port", 5432)

	var target bindDBConfig
	b, err := Bind(ctx, cfg, "db", &target,
		WithPollInterval(time.Second),
	)
	if err != nil {
		t.Fatalf("Bind failed: %v", err)
	}
	defer b.Stop()

	if target.Host != "localhost" {
		t.Errorf("expected host=localhost, got %s", target.Host)
	}
	if target.Port != 5432 {
		t.Errorf("expected port=5432, got %d", target.Port)
	}

	if b.LastReload().IsZero() {
		t.Error("expected LastReload to be set")
	}
	if b.LastError() != nil {
		t.Errorf("expected no error, got %v", b.LastError())
	}
}

func TestBinding_InvalidTarget(t *testing.T) {
	ctx := context.Background()
	cfg := setupBindingConfig(t)

	// Non-pointer
	var target bindDBConfig
	_, err := Bind(ctx, cfg, "db", target)
	if err != ErrInvalidTarget {
		t.Errorf("expected ErrInvalidTarget for non-pointer, got %v", err)
	}

	// Pointer to non-struct
	s := "hello"
	_, err = Bind(ctx, cfg, "db", &s)
	if err != ErrInvalidTarget {
		t.Errorf("expected ErrInvalidTarget for pointer to string, got %v", err)
	}
}

func TestBinding_AutoReload(t *testing.T) {
	ctx := context.Background()
	cfg := setupBindingConfig(t)

	cfg.Set(ctx, "db/host", "localhost")
	cfg.Set(ctx, "db/port", 5432)

	var target bindDBConfig
	b, err := Bind(ctx, cfg, "db", &target,
		WithPollInterval(50*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("Bind failed: %v", err)
	}
	defer b.Stop()

	// Update config
	cfg.Set(ctx, "db/host", "remotehost")
	cfg.Set(ctx, "db/port", 5433)

	// Wait for reload
	time.Sleep(200 * time.Millisecond)

	b.Get(func(t2 any) {
		cfg := t2.(*bindDBConfig)
		if cfg.Host != "remotehost" {
			t.Errorf("expected host=remotehost, got %s", cfg.Host)
		}
		if cfg.Port != 5433 {
			t.Errorf("expected port=5433, got %d", cfg.Port)
		}
	})
}

func TestBinding_OnReload(t *testing.T) {
	ctx := context.Background()
	cfg := setupBindingConfig(t)

	cfg.Set(ctx, "db/host", "localhost")
	cfg.Set(ctx, "db/port", 5432)

	var callCount atomic.Int32
	var target bindDBConfig
	b, err := Bind(ctx, cfg, "db", &target,
		WithPollInterval(50*time.Millisecond),
		WithOnReload(func() {
			callCount.Add(1)
		}),
	)
	if err != nil {
		t.Fatalf("Bind failed: %v", err)
	}
	defer b.Stop()

	// Initial load triggers OnReload
	if callCount.Load() < 1 {
		t.Error("expected OnReload to be called for initial load")
	}

	// Update config
	cfg.Set(ctx, "db/host", "changed")
	time.Sleep(200 * time.Millisecond)

	if callCount.Load() < 2 {
		t.Errorf("expected OnReload to be called at least twice, got %d", callCount.Load())
	}
}

func TestBinding_OnError(t *testing.T) {
	ctx := context.Background()
	cfg := setupBindingConfig(t)

	cfg.Set(ctx, "db/host", "localhost")
	cfg.Set(ctx, "db/port", 5432)

	var errorCount atomic.Int32
	var target bindDBConfig
	b, err := Bind(ctx, cfg, "db", &target,
		WithPollInterval(50*time.Millisecond),
		WithOnError(func(err error) {
			errorCount.Add(1)
		}),
	)
	if err != nil {
		t.Fatalf("Bind failed: %v", err)
	}
	defer b.Stop()

	// Delete keys to cause reload errors
	cfg.Delete(ctx, "db/host")
	cfg.Delete(ctx, "db/port")

	time.Sleep(200 * time.Millisecond)

	// Old values should be preserved (binding holds write lock during reload)
	if target.Host != "localhost" {
		t.Errorf("expected host=localhost preserved, got %s", target.Host)
	}
}

func TestBinding_Stop(t *testing.T) {
	ctx := context.Background()
	cfg := setupBindingConfig(t)

	cfg.Set(ctx, "db/host", "localhost")
	cfg.Set(ctx, "db/port", 5432)

	var target bindDBConfig
	b, err := Bind(ctx, cfg, "db", &target,
		WithPollInterval(50*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("Bind failed: %v", err)
	}

	b.Stop()

	// Idempotent stop
	b.Stop()
	b.Stop()

	// Target should still have the last known values
	if target.Host != "localhost" {
		t.Errorf("expected host=localhost after stop, got %s", target.Host)
	}
}

func TestBinding_ReloadNow(t *testing.T) {
	ctx := context.Background()
	cfg := setupBindingConfig(t)

	cfg.Set(ctx, "db/host", "localhost")
	cfg.Set(ctx, "db/port", 5432)

	var target bindDBConfig
	b, err := Bind(ctx, cfg, "db", &target,
		WithPollInterval(10*time.Second),
	)
	if err != nil {
		t.Fatalf("Bind failed: %v", err)
	}
	defer b.Stop()

	cfg.Set(ctx, "db/host", "forced")

	if err := b.ReloadNow(ctx); err != nil {
		t.Fatalf("ReloadNow failed: %v", err)
	}

	if target.Host != "forced" {
		t.Errorf("expected host=forced, got %s", target.Host)
	}
}

func TestBinding_Get(t *testing.T) {
	ctx := context.Background()
	cfg := setupBindingConfig(t)

	cfg.Set(ctx, "db/host", "localhost")
	cfg.Set(ctx, "db/port", 5432)

	var target bindDBConfig
	b, err := Bind(ctx, cfg, "db", &target,
		WithPollInterval(time.Second),
	)
	if err != nil {
		t.Fatalf("Bind failed: %v", err)
	}
	defer b.Stop()

	b.Get(func(raw any) {
		cfg := raw.(*bindDBConfig)
		if cfg.Host != "localhost" {
			t.Errorf("expected host=localhost, got %s", cfg.Host)
		}
	})
}
