package bind

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/memory"
)

// undecodableValue embeds a real config.Value (so the store can marshal and
// persist it) but fails on Unmarshal, simulating a decode-phase failure — an
// entry whose stored bytes no longer match its codec.
type undecodableValue struct {
	config.Value
}

func (undecodableValue) Unmarshal(context.Context, any) error {
	return errors.New("simulated decode failure")
}

// TestGetStruct_LogsUndecodableEntry verifies that an entry which fails to
// decode is skipped (so the bind still succeeds for the decodable siblings)
// but is logged via the configured WithLogger, rather than dropped silently.
func TestGetStruct_LogsUndecodableEntry(t *testing.T) {
	ctx := context.Background()
	store := memory.NewStore()
	mgr, err := config.New(config.WithStore(store))
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect error: %v", err)
	}
	t.Cleanup(func() { mgr.Close(ctx) })

	cfg := mgr.Namespace("test")

	// A decodable sibling entry.
	if err := cfg.Set(ctx, "app/name", "svc"); err != nil {
		t.Fatalf("Set error: %v", err)
	}
	// An entry that fails to decode at read time, under the same prefix.
	bad := undecodableValue{Value: config.NewValue("unused")}
	if _, err := store.Set(ctx, "test", "app/bad", bad); err != nil {
		t.Fatalf("store.Set error: %v", err)
	}

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	b := New(cfg, WithLogger(logger))
	var target struct {
		Name string `json:"name"`
	}
	if err := b.Bind().GetStruct(ctx, "app", &target); err != nil {
		t.Fatalf("GetStruct should succeed despite an undecodable sibling, got %v", err)
	}

	logged := buf.String()
	if !strings.Contains(logged, "undecodable") {
		t.Errorf("expected a warning about the undecodable entry, got: %q", logged)
	}
	if !strings.Contains(logged, "app/bad") {
		t.Errorf("expected the warning to name the offending key app/bad, got: %q", logged)
	}
	// The decodable sibling must still have bound.
	if target.Name != "svc" {
		t.Errorf("decodable field should still bind; Name = %q, want %q", target.Name, "svc")
	}
}
