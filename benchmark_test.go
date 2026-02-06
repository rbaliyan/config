package config_test

import (
	"context"
	"testing"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/memory"
)

func BenchmarkManagerGet(b *testing.B) {
	ctx := context.Background()
	store := memory.NewStore()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	_, _ = store.Set(ctx, "bench", "key", config.NewValue("hello"))

	mgr, _ := config.New(config.WithStore(store))
	_ = mgr.Connect(ctx)
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("bench")

	b.ResetTimer()
	for b.Loop() {
		_, _ = cfg.Get(ctx, "key")
	}
}

func BenchmarkManagerGetCached(b *testing.B) {
	ctx := context.Background()
	store := memory.NewStore()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	_, _ = store.Set(ctx, "bench", "key", config.NewValue(42))

	mgr, _ := config.New(config.WithStore(store))
	_ = mgr.Connect(ctx)
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("bench")

	// Prime cache
	cfg.Get(ctx, "key")

	b.ResetTimer()
	for b.Loop() {
		_, _ = cfg.Get(ctx, "key")
	}
}

func BenchmarkNewValue(b *testing.B) {
	for b.Loop() {
		config.NewValue(42)
	}
}

func BenchmarkValueInt64(b *testing.B) {
	v := config.NewValue(42)
	b.ResetTimer()
	for b.Loop() {
		_, _ = v.Int64()
	}
}

func BenchmarkValueString(b *testing.B) {
	v := config.NewValue("hello world")
	b.ResetTimer()
	for b.Loop() {
		_, _ = v.String()
	}
}

func BenchmarkValueMarshal(b *testing.B) {
	v := config.NewValue(map[string]any{"host": "localhost", "port": 5432})
	b.ResetTimer()
	for b.Loop() {
		_, _ = v.Marshal()
	}
}

func BenchmarkMarkStale(b *testing.B) {
	v := config.NewValue("hello")
	b.ResetTimer()
	for b.Loop() {
		config.MarkStale(v)
	}
}

func BenchmarkStoreGet(b *testing.B) {
	ctx := context.Background()
	store := memory.NewStore()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	_, _ = store.Set(ctx, "bench", "key", config.NewValue("value"))

	b.ResetTimer()
	for b.Loop() {
		_, _ = store.Get(ctx, "bench", "key")
	}
}

func BenchmarkStoreSet(b *testing.B) {
	ctx := context.Background()
	store := memory.NewStore()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	v := config.NewValue("value")

	b.ResetTimer()
	for b.Loop() {
		_, _ = store.Set(ctx, "bench", "key", v)
	}
}
