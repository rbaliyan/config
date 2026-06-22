package config_test

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/memory"
)

// benchValue is a representative payload reused across encode/decode benches.
func benchMap() map[string]any {
	return map[string]any{"host": "localhost", "port": 5432, "tls": true}
}

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

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _ = cfg.Get(ctx, "key")
	}
}

// BenchmarkCacheGetHit measures the in-memory cache hit path directly.
//
// This is the cache's Get fast path (cache.go: memoryCache.Get), distinct from
// nsConfig.Get which is store-first and only consults the cache as an
// error-fallback. The value is Set once and every loop iteration is a hit.
func BenchmarkCacheGetHit(b *testing.B) {
	ctx := context.Background()
	cache, err := config.NewMemoryCache(0, 0)
	if err != nil {
		b.Fatal(err)
	}
	if err := cache.Set(ctx, "bench", "key", config.NewValue(42)); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _ = cache.Get(ctx, "bench", "key")
	}
}

func BenchmarkNewValue(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		config.NewValue(42)
	}
}

// BenchmarkNewValueByType exercises detectType across the common scalar and
// composite types so a regression in type detection is visible per-type.
func BenchmarkNewValueByType(b *testing.B) {
	cases := []struct {
		name string
		data any
	}{
		{"int", 42},
		{"string", "hello world"},
		{"bool", true},
		{"float", 3.14159},
		{"map", benchMap()},
		{"slice", []any{1, 2, 3, "four"}},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				config.NewValue(tc.data)
			}
		})
	}
}

func BenchmarkValueInt64(b *testing.B) {
	v := config.NewValue(42)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _ = v.Int64()
	}
}

func BenchmarkValueString(b *testing.B) {
	v := config.NewValue("hello world")
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _ = v.String()
	}
}

func BenchmarkValueMarshal(b *testing.B) {
	ctx := context.Background()
	v := config.NewValue(benchMap())
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _ = v.Marshal(ctx)
	}
}

// BenchmarkValueFromBytes measures the decode side: parsing encoded bytes into
// a Value via the registered codec. Mirrors BenchmarkValueMarshal.
func BenchmarkValueFromBytes(b *testing.B) {
	ctx := context.Background()
	data, err := config.NewValue(benchMap()).Marshal(ctx)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _ = config.NewValueFromBytes(ctx, data, "json")
	}
}

// BenchmarkValueUnmarshal measures decoding a Value into a typed target.
func BenchmarkValueUnmarshal(b *testing.B) {
	ctx := context.Background()
	v := config.NewValue(benchMap())
	// Force the encoded form to be materialised so Unmarshal exercises decode.
	if _, err := v.Marshal(ctx); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var out map[string]any
		_ = v.Unmarshal(ctx, &out)
	}
}

func BenchmarkMarkStale(b *testing.B) {
	v := config.NewValue("hello")
	b.ReportAllocs()
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

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _ = store.Get(ctx, "bench", "key")
	}
}

// BenchmarkStoreSet_Update repeatedly Sets the same key, exercising the
// update path (version bump + history append + map overwrite).
//
// History is capped via WithMaxHistory so the per-update cost stays constant:
// the store copies the full history slice on every write, so an uncapped
// single-key loop would be O(n²) and report meaningless figures.
func BenchmarkStoreSet_Update(b *testing.B) {
	ctx := context.Background()
	store := memory.NewStore(memory.WithMaxHistory(8))
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	v := config.NewValue("value")
	// Seed once so the first loop iteration is already an update.
	_, _ = store.Set(ctx, "bench", "key", v)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _ = store.Set(ctx, "bench", "key", v)
	}
}

// BenchmarkStoreSet_Insert varies the key each iteration, exercising the
// insert path (ID allocation + fresh entry). The store grows unbounded, which
// is intentional: it measures the cost of growing the keyspace.
func BenchmarkStoreSet_Insert(b *testing.B) {
	ctx := context.Background()
	store := memory.NewStore()
	_ = store.Connect(ctx)
	defer store.Close(ctx)

	v := config.NewValue("value")

	b.ReportAllocs()
	b.ResetTimer()
	i := 0
	for b.Loop() {
		_, _ = store.Set(ctx, "bench", strconv.Itoa(i), v)
		i++
	}
}

// seededStore returns a connected memory store pre-populated with n entries
// under the "bench" namespace, keyed "key-0".."key-(n-1)".
func seededStore(b *testing.B, n int) *memory.Store {
	b.Helper()
	ctx := context.Background()
	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		b.Fatal(err)
	}
	v := config.NewValue("value")
	for i := 0; i < n; i++ {
		if _, err := store.Set(ctx, "bench", "key-"+strconv.Itoa(i), v); err != nil {
			b.Fatal(err)
		}
	}
	return store
}

// BenchmarkStoreFind measures prefix-scan + sort + pagination cost as the
// dataset grows. Find is O(n) over the namespace, so cost should scale with n.
func BenchmarkStoreFind(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{100, 1000, 10000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			store := seededStore(b, n)
			defer store.Close(ctx)
			filter := config.NewFilter().WithPrefix("key-").WithLimit(50).Build()

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_, _ = store.Find(ctx, "bench", filter)
			}
		})
	}
}

// BenchmarkStoreGetMany measures bulk reads over a seeded store. It reports a
// per-key metric so cost-per-element is comparable across batch sizes.
func BenchmarkStoreGetMany(b *testing.B) {
	ctx := context.Background()
	const seed = 10000
	for _, batch := range []int{10, 100, 1000} {
		b.Run(strconv.Itoa(batch), func(b *testing.B) {
			store := seededStore(b, seed)
			defer store.Close(ctx)
			keys := make([]string, batch)
			for i := 0; i < batch; i++ {
				keys[i] = "key-" + strconv.Itoa(i)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_, _ = store.GetMany(ctx, "bench", keys)
			}
			b.ReportMetric(float64(batch), "keys/op")
		})
	}
}

// BenchmarkStoreSetMany measures bulk writes. A fresh value map is reused; each
// iteration is an update after the first, isolating the per-key write cost.
func BenchmarkStoreSetMany(b *testing.B) {
	ctx := context.Background()
	for _, batch := range []int{10, 100, 1000} {
		b.Run(strconv.Itoa(batch), func(b *testing.B) {
			// Cap history: every iteration updates the same keys, and the
			// store copies the full history slice per write, so an uncapped
			// loop would grow O(n²) and distort the measurement.
			store := memory.NewStore(memory.WithMaxHistory(8))
			if err := store.Connect(ctx); err != nil {
				b.Fatal(err)
			}
			defer store.Close(ctx)
			v := config.NewValue("value")
			values := make(map[string]config.Value, batch)
			for i := 0; i < batch; i++ {
				values["key-"+strconv.Itoa(i)] = v
			}

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_ = store.SetMany(ctx, "bench", values)
			}
			b.ReportMetric(float64(batch), "keys/op")
		})
	}
}

// BenchmarkStoreDeleteMany measures bulk deletes. Each iteration re-seeds the
// batch (timer paused) then deletes it, so only the delete is measured.
func BenchmarkStoreDeleteMany(b *testing.B) {
	ctx := context.Background()
	for _, batch := range []int{10, 100, 1000} {
		b.Run(strconv.Itoa(batch), func(b *testing.B) {
			store := memory.NewStore()
			if err := store.Connect(ctx); err != nil {
				b.Fatal(err)
			}
			defer store.Close(ctx)
			v := config.NewValue("value")
			keys := make([]string, batch)
			values := make(map[string]config.Value, batch)
			for i := 0; i < batch; i++ {
				k := "key-" + strconv.Itoa(i)
				keys[i] = k
				values[k] = v
			}

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				b.StopTimer()
				_ = store.SetMany(ctx, "bench", values)
				b.StartTimer()
				_, _ = store.DeleteMany(ctx, "bench", keys)
			}
			b.ReportMetric(float64(batch), "keys/op")
		})
	}
}

// BenchmarkStoreGetParallel measures read contention on the store's RWMutex.
func BenchmarkStoreGetParallel(b *testing.B) {
	ctx := context.Background()
	store := seededStore(b, 1000)
	defer store.Close(ctx)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = store.Get(ctx, "bench", "key-"+strconv.Itoa(i%1000))
			i++
		}
	})
}

// BenchmarkStoreSetParallel measures write contention on the store's RWMutex.
// Each goroutine writes its own key range to surface lock contention rather
// than logical update conflicts.
func BenchmarkStoreSetParallel(b *testing.B) {
	ctx := context.Background()
	// Cap history: keys are reused across iterations, so bound the per-write
	// history copy to keep the measurement about lock contention, not O(n²)
	// history growth.
	store := memory.NewStore(memory.WithMaxHistory(8))
	_ = store.Connect(ctx)
	defer store.Close(ctx)
	v := config.NewValue("value")

	b.ReportAllocs()
	b.ResetTimer()
	var counter atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		// Distinct prefix per goroutine avoids history growth dominating.
		id := counter.Add(1)
		base := "g" + strconv.FormatInt(id, 10) + "-"
		i := 0
		for pb.Next() {
			_, _ = store.Set(ctx, "bench", base+strconv.Itoa(i%256), v)
			i++
		}
	})
}
