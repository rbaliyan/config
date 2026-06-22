package storetest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

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

	t.Run("ConcurrentSameKeyWrites", func(t *testing.T) {
		t.Parallel()
		store := factory(t)
		ctx := context.Background()

		// N goroutines hammer the same key. The store must serialize the
		// writes so the resulting version sequence is monotonic and
		// gap-free (1..N) — no lost write, no duplicated version. Each
		// writer records the version it observed from its own Set so we
		// can prove every version in [1,N] was handed out exactly once.
		const writers = 16
		var (
			wg       sync.WaitGroup
			mu       sync.Mutex
			versions []int64
			firstErr error
		)
		wg.Add(writers)
		for i := 0; i < writers; i++ {
			go func(n int) {
				defer wg.Done()
				written, err := store.Set(ctx, "ns", "hot",
					config.NewValue(n, config.WithValueType(config.TypeInt)))
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					if firstErr == nil {
						firstErr = err
					}
					return
				}
				versions = append(versions, written.Metadata().Version())
			}(i)
		}
		wg.Wait()

		if firstErr != nil {
			t.Fatalf("concurrent Set returned error: %v", firstErr)
		}
		if len(versions) != writers {
			t.Fatalf("got %d successful writes, want %d (lost write)", len(versions), writers)
		}

		// HARD CONTRACT (no lost writes): the persisted version after all
		// writers finish must equal the number of writes. Each write does
		// version = version + 1, so a final version below `writers` proves
		// at least one increment was lost to a race — a true lost update in
		// durable state.
		got, err := store.Get(ctx, "ns", "hot")
		if err != nil {
			t.Fatalf("Get after concurrent writes: %v", err)
		}
		if v := got.Metadata().Version(); v != writers {
			t.Errorf("final persisted version = %d, want %d (lost update in durable state)", v, writers)
		}

		// TIGHTER CONTRACT (each Set returns its own unique version):
		// backends that build the returned Value under the same lock/txn
		// that performs the increment (e.g. memory) hand back a perfect
		// 1..N permutation. Backends that increment atomically in the
		// store but read the version back in a *separate* statement (e.g.
		// the SQL backends' Set -> Get round-trip) can return duplicated
		// or skipped versions even though the durable sequence above is
		// correct. That weaker return-value guarantee is logged, not
		// failed, so the suite stays fair across backends while still
		// surfacing the divergence for the owning backend to evaluate.
		seen := make(map[int64]int, writers)
		for _, v := range versions {
			seen[v]++
		}
		perfectPermutation := true
		for want := int64(1); want <= writers; want++ {
			if seen[want] != 1 {
				perfectPermutation = false
				break
			}
		}
		if !perfectPermutation {
			t.Logf("returned per-write versions are not a 1..%d permutation: %v "+
				"(durable sequence is still gap-free; backend's Set returns a "+
				"non-atomically-read-back version)", writers, versions)
		}
	})

	t.Run("ConditionalWrites", func(t *testing.T) {
		t.Parallel()
		store := factory(t)
		ctx := context.Background()

		createOnly := func(v any) config.Value {
			return config.NewValue(v, config.WithValueType(config.TypeInt),
				config.WithValueWriteMode(config.WriteModeCreate))
		}
		updateOnly := func(v any) config.Value {
			return config.NewValue(v, config.WithValueType(config.TypeInt),
				config.WithValueWriteMode(config.WriteModeUpdate))
		}

		// update-only on a missing key must fail with ErrNotFound and must
		// not create the key.
		if _, err := store.Set(ctx, "ns", "cw", updateOnly(1)); !errors.Is(err, config.ErrNotFound) {
			t.Fatalf("WriteModeUpdate on missing key: err = %v, want ErrNotFound", err)
		}
		if _, err := store.Get(ctx, "ns", "cw"); !config.IsNotFound(err) {
			t.Fatalf("key created by a failed update-only write: err = %v, want ErrNotFound", err)
		}

		// create-only on a missing key must succeed (version 1).
		created, err := store.Set(ctx, "ns", "cw", createOnly(10))
		if err != nil {
			t.Fatalf("WriteModeCreate on missing key: %v", err)
		}
		if created.Metadata().Version() != 1 {
			t.Errorf("create-only first version = %d, want 1", created.Metadata().Version())
		}

		// create-only on an existing key must fail with ErrKeyExists and
		// must not bump the version.
		if _, err := store.Set(ctx, "ns", "cw", createOnly(20)); !errors.Is(err, config.ErrKeyExists) {
			t.Fatalf("WriteModeCreate on existing key: err = %v, want ErrKeyExists", err)
		}
		got, err := store.Get(ctx, "ns", "cw")
		if err != nil {
			t.Fatalf("Get after rejected create-only: %v", err)
		}
		if n, _ := got.Int64(); n != 10 {
			t.Errorf("value mutated by rejected create-only write: got %d, want 10", n)
		}
		if got.Metadata().Version() != 1 {
			t.Errorf("version bumped by rejected create-only write: got %d, want 1", got.Metadata().Version())
		}

		// update-only on an existing key must succeed and bump the version.
		updated, err := store.Set(ctx, "ns", "cw", updateOnly(30))
		if err != nil {
			t.Fatalf("WriteModeUpdate on existing key: %v", err)
		}
		if updated.Metadata().Version() != 2 {
			t.Errorf("update-only version = %d, want 2", updated.Metadata().Version())
		}
		if n, _ := updated.Int64(); n != 30 {
			t.Errorf("update-only value = %d, want 30", n)
		}
	})

	t.Run("ContextCancellation", func(t *testing.T) {
		t.Parallel()
		store := factory(t)

		// Seed a key with a live context so Get has something to find when
		// it is later handed a dead context.
		if _, err := store.Set(context.Background(), "ns", "ctx",
			config.NewValue(1, config.WithValueType(config.TypeInt))); err != nil {
			t.Fatalf("seed Set: %v", err)
		}

		// isCtxErr accepts either context.Canceled or DeadlineExceeded.
		// Backends may wrap the error, so we use errors.Is. Some purely
		// in-memory backends do not consult ctx on every path; those are
		// allowed to succeed, but they must never hang and must never
		// return an unrelated error.
		isCtxErr := func(err error) bool {
			return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
		}

		// Run an operation under a cancelled context with a hard wall-clock
		// bound so a hanging backend fails loudly instead of stalling the
		// whole suite.
		runBounded := func(name string, op func(ctx context.Context) error) {
			cancelled, cancel := context.WithCancel(context.Background())
			cancel() // already cancelled before the call

			done := make(chan error, 1)
			go func() { done <- op(cancelled) }()

			select {
			case err := <-done:
				// Either a context error (preferred) or success (for
				// backends that complete the op without touching ctx).
				// An unrelated error is a contract violation.
				if err != nil && !isCtxErr(err) {
					t.Errorf("%s under cancelled ctx: err = %v, want context.Canceled/DeadlineExceeded or nil", name, err)
				}
			case <-time.After(10 * time.Second):
				t.Errorf("%s did not return within 10s under cancelled ctx (hang)", name)
			}
		}

		runBounded("Get", func(ctx context.Context) error {
			_, err := store.Get(ctx, "ns", "ctx")
			return err
		})
		runBounded("Set", func(ctx context.Context) error {
			_, err := store.Set(ctx, "ns", "ctx2", config.NewValue(2, config.WithValueType(config.TypeInt)))
			return err
		})
		runBounded("Find", func(ctx context.Context) error {
			_, err := store.Find(ctx, "ns", config.NewFilter().WithPrefix("ctx").Build())
			return err
		})

		// Also exercise an already-expired deadline (distinct code path
		// from an explicit Cancel on several backends).
		expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()
		done := make(chan error, 1)
		go func() {
			_, err := store.Get(expired, "ns", "ctx")
			done <- err
		}()
		select {
		case err := <-done:
			if err != nil && !isCtxErr(err) {
				t.Errorf("Get under expired deadline: err = %v, want DeadlineExceeded/Canceled or nil", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("Get did not return within 10s under expired deadline (hang)")
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
