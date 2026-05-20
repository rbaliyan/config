package storetest

import (
	"context"
	"errors"
	"testing"

	"github.com/rbaliyan/config"
)

// RunStoreConformanceSuite executes the full [config.Store] contract against
// a backend. Each subtest gets a fresh store from factory so they remain
// independent and parallelisable.
//
// Use this in a backend's test package instead of hand-writing the
// per-backend equivalents:
//
//	func TestMemory_StoreConformance(t *testing.T) {
//	    storetest.RunStoreConformanceSuite(t, func(t *testing.T) config.Store {
//	        s := memory.NewStore()
//	        if err := s.Connect(t.Context()); err != nil { t.Fatalf("connect: %v", err) }
//	        t.Cleanup(func() { _ = s.Close(context.Background()) })
//	        return s
//	    })
//	}
//
// Watch is intentionally excluded from this suite — every backend's
// watch latency and resume semantics differ enough that the timing
// budget belongs in the backend-specific test, not a shared contract.
// Each backend keeps its own watch test alongside its other coverage.
func RunStoreConformanceSuite(t *testing.T, factory Factory) {
	t.Helper()

	t.Run("BasicOperations", func(t *testing.T) {
		t.Parallel()
		store := factory(t)
		ctx := context.Background()

		written, err := store.Set(ctx, "ns", "k", config.NewValue(42, config.WithValueType(config.TypeInt)))
		if err != nil {
			t.Fatalf("Set: %v", err)
		}
		if written.Metadata().Version() != 1 {
			t.Errorf("first Set returned version %d, want 1", written.Metadata().Version())
		}

		got, err := store.Get(ctx, "ns", "k")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if n, _ := got.Int64(); n != 42 {
			t.Errorf("Get returned %d, want 42", n)
		}

		// Update increments version.
		updated, err := store.Set(ctx, "ns", "k", config.NewValue(100, config.WithValueType(config.TypeInt)))
		if err != nil {
			t.Fatalf("update Set: %v", err)
		}
		if updated.Metadata().Version() != 2 {
			t.Errorf("update returned version %d, want 2", updated.Metadata().Version())
		}

		if err := store.Delete(ctx, "ns", "k"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		if _, err := store.Get(ctx, "ns", "k"); !config.IsNotFound(err) {
			t.Errorf("Get after Delete: err = %v, want ErrNotFound", err)
		}
	})

	t.Run("GetMissingReturnsNotFound", func(t *testing.T) {
		t.Parallel()
		store := factory(t)
		_, err := store.Get(context.Background(), "ns", "missing")
		if !errors.Is(err, config.ErrNotFound) {
			t.Fatalf("err = %v, want wraps ErrNotFound", err)
		}
	})

	t.Run("Find_Prefix", func(t *testing.T) {
		t.Parallel()
		store := factory(t)
		ctx := context.Background()

		for _, k := range []string{"app/db/host", "app/db/port", "app/cache/ttl", "other/x"} {
			if _, err := store.Set(ctx, "ns", k, config.NewValue("v")); err != nil {
				t.Fatalf("Set %q: %v", k, err)
			}
		}

		page, err := store.Find(ctx, "ns", config.NewFilter().WithPrefix("app/db").Build())
		if err != nil {
			t.Fatalf("Find: %v", err)
		}
		if got := len(page.Results()); got != 2 {
			t.Fatalf("Find prefix=app/db returned %d, want 2: %v", got, keys(page.Results()))
		}
		if _, ok := page.Results()["other/x"]; ok {
			t.Error("Find prefix=app/db leaked other/x")
		}
	})

	t.Run("Find_Pagination", func(t *testing.T) {
		t.Parallel()
		store := factory(t)
		ctx := context.Background()

		for _, k := range []string{"app/a", "app/b", "app/c"} {
			if _, err := store.Set(ctx, "ns", k, config.NewValue(1)); err != nil {
				t.Fatalf("Set %q: %v", k, err)
			}
		}

		page1, err := store.Find(ctx, "ns", config.NewFilter().WithPrefix("app/").WithLimit(2).Build())
		if err != nil {
			t.Fatalf("Find page 1: %v", err)
		}
		if got := len(page1.Results()); got != 2 {
			t.Fatalf("page 1 returned %d entries, want 2", got)
		}
		if page1.NextCursor() == "" {
			t.Fatal("page 1 NextCursor is empty; expected continuation")
		}

		page2, err := store.Find(ctx, "ns",
			config.NewFilter().WithPrefix("app/").WithLimit(2).WithCursor(page1.NextCursor()).Build())
		if err != nil {
			t.Fatalf("Find page 2: %v", err)
		}
		if got := len(page2.Results()); got != 1 {
			t.Fatalf("page 2 returned %d entries, want 1 (3 total)", got)
		}
	})

	t.Run("Types_RoundTrip", func(t *testing.T) {
		t.Parallel()
		store := factory(t)
		ctx := context.Background()

		cases := []struct {
			key string
			v   any
			typ config.Type
		}{
			{"types/string", "hello", config.TypeString},
			{"types/int", 42, config.TypeInt},
			{"types/float", 3.14, config.TypeFloat},
			{"types/bool", true, config.TypeBool},
		}
		for _, c := range cases {
			val := config.NewValue(c.v, config.WithValueType(c.typ))
			if _, err := store.Set(ctx, "ns", c.key, val); err != nil {
				t.Fatalf("Set %s: %v", c.key, err)
			}
			got, err := store.Get(ctx, "ns", c.key)
			if err != nil {
				t.Fatalf("Get %s: %v", c.key, err)
			}
			if got.Type() != c.typ {
				t.Errorf("%s: Type = %v, want %v", c.key, got.Type(), c.typ)
			}
		}
	})

	t.Run("Health", func(t *testing.T) {
		t.Parallel()
		store := factory(t)
		hc, ok := store.(config.HealthChecker)
		if !ok {
			t.Skip("store does not implement HealthChecker")
		}
		if err := hc.Health(context.Background()); err != nil {
			t.Fatalf("Health on connected store: %v", err)
		}
	})

	t.Run("Stats", func(t *testing.T) {
		t.Parallel()
		store := factory(t)
		ctx := context.Background()
		sp, ok := store.(config.StatsProvider)
		if !ok {
			t.Skip("store does not implement StatsProvider")
		}

		if _, err := store.Set(ctx, "ns1", "a", config.NewValue(1, config.WithValueType(config.TypeInt))); err != nil {
			t.Fatalf("Set: %v", err)
		}
		if _, err := store.Set(ctx, "ns1", "b", config.NewValue("s", config.WithValueType(config.TypeString))); err != nil {
			t.Fatalf("Set: %v", err)
		}
		if _, err := store.Set(ctx, "ns2", "c", config.NewValue(true, config.WithValueType(config.TypeBool))); err != nil {
			t.Fatalf("Set: %v", err)
		}

		stats, err := sp.Stats(ctx)
		if err != nil {
			t.Fatalf("Stats: %v", err)
		}
		if stats.TotalEntries() != 3 {
			t.Errorf("TotalEntries = %d, want 3", stats.TotalEntries())
		}
		if stats.CountForType(config.TypeInt) != 1 {
			t.Errorf("CountForType(Int) = %d, want 1", stats.CountForType(config.TypeInt))
		}
		if stats.CountForNamespace("ns1") != 2 {
			t.Errorf("CountForNamespace(ns1) = %d, want 2", stats.CountForNamespace("ns1"))
		}
	})

	t.Run("SecretValue_TypeAndMaskSurviveRoundTrip", func(t *testing.T) {
		t.Parallel()
		store := factory(t)
		ctx := context.Background()

		val := config.NewSecretValue(config.NewSecret("hunter2"))
		if _, err := store.Set(ctx, "ns", "secret/k", val); err != nil {
			t.Fatalf("Set: %v", err)
		}
		got, err := store.Get(ctx, "ns", "secret/k")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Type() != config.TypeSecret {
			t.Errorf("Type = %v, want TypeSecret", got.Type())
		}
		s, err := got.String()
		if err != nil {
			t.Fatalf("String: %v", err)
		}
		if s != "******" {
			t.Errorf("String() = %q, want \"******\" (plaintext leak)", s)
		}
	})
}

// keys returns the sorted-ish key set for an error message. Used only by
// failure paths so the order isn't important.
func keys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
