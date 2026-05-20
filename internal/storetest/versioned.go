package storetest

import (
	"context"
	"errors"
	"testing"

	"github.com/rbaliyan/config"
)

// RunVersionedStoreSuite executes the [config.VersionedStore] conformance
// contract against a backend. Skips per subtest when the backend does not
// implement [config.VersionedStore]; this is opt-in like the rest of
// storetest.
//
// Usage:
//
//	func TestMongoDB_VersionedStoreConformance(t *testing.T) {
//	    storetest.RunVersionedStoreSuite(t, func(t *testing.T) config.Store {
//	        // ... return a connected mongodb.Store with WithVersioning(true)
//	    })
//	}
//
// Memory's [config.VersionedStore] is the spec's reference implementation.
// Backends with backend-specific concerns (snapshot collections, max
// history, orphan cleanup, OnVersionError hooks) keep those tests
// alongside their backend code; this suite locks the universal contract.
func RunVersionedStoreSuite(t *testing.T, factory Factory) {
	t.Helper()

	t.Run("ListAllReturnsDescending", func(t *testing.T) {
		t.Parallel()
		store := factory(t)
		vs := requireVersioned(t, store)
		ctx := context.Background()

		for i := 1; i <= 4; i++ {
			if _, err := store.Set(ctx, "ns", "k", config.NewValue(i)); err != nil {
				t.Fatalf("Set %d: %v", i, err)
			}
		}

		page, err := vs.GetVersions(ctx, "ns", "k", config.NewVersionFilter().Build())
		if err != nil {
			t.Fatalf("GetVersions: %v", err)
		}
		got := page.Versions()
		if len(got) != 4 {
			t.Fatalf("got %d versions, want 4", len(got))
		}
		// Descending: index 0 = newest.
		for i, v := range got {
			want := int64(4 - i)
			if v.Metadata().Version() != want {
				t.Errorf("got[%d].Version = %d, want %d", i, v.Metadata().Version(), want)
			}
		}
	})

	t.Run("SpecificVersionReturnsSingleEntry", func(t *testing.T) {
		t.Parallel()
		store := factory(t)
		vs := requireVersioned(t, store)
		ctx := context.Background()

		for i, val := range []string{"v1", "v2", "v3"} {
			if _, err := store.Set(ctx, "ns", "k", config.NewValue(val)); err != nil {
				t.Fatalf("Set %d: %v", i, err)
			}
		}

		page, err := vs.GetVersions(ctx, "ns", "k", config.NewVersionFilter().WithVersion(2).Build())
		if err != nil {
			t.Fatalf("GetVersions(version=2): %v", err)
		}
		got := page.Versions()
		if len(got) != 1 {
			t.Fatalf("got %d versions, want 1", len(got))
		}
		if got[0].Metadata().Version() != 2 {
			t.Errorf("got version %d, want 2", got[0].Metadata().Version())
		}
		s, _ := got[0].String()
		if s != "v2" {
			t.Errorf("got value %q, want %q", s, "v2")
		}
	})

	t.Run("UnknownKeyReturnsNotFound", func(t *testing.T) {
		t.Parallel()
		store := factory(t)
		vs := requireVersioned(t, store)

		_, err := vs.GetVersions(context.Background(), "ns", "nonexistent",
			config.NewVersionFilter().Build())
		if !errors.Is(err, config.ErrNotFound) {
			t.Fatalf("err = %v, want wraps ErrNotFound", err)
		}
	})

	t.Run("Pagination_DescendingAcrossPages", func(t *testing.T) {
		t.Parallel()
		store := factory(t)
		vs := requireVersioned(t, store)
		ctx := context.Background()

		for i := 1; i <= 5; i++ {
			if _, err := store.Set(ctx, "ns", "k", config.NewValue(i)); err != nil {
				t.Fatalf("Set %d: %v", i, err)
			}
		}

		var versions []int64
		cursor := ""
		for safety := 0; safety < 10; safety++ {
			page, err := vs.GetVersions(ctx, "ns", "k",
				config.NewVersionFilter().WithLimit(2).WithCursor(cursor).Build())
			if err != nil {
				t.Fatalf("GetVersions: %v", err)
			}
			for _, v := range page.Versions() {
				versions = append(versions, v.Metadata().Version())
			}
			if page.NextCursor() == "" {
				break
			}
			cursor = page.NextCursor()
		}

		if len(versions) != 5 {
			t.Fatalf("collected %d versions, want 5: %v", len(versions), versions)
		}
		for i, v := range versions {
			want := int64(5 - i)
			if v != want {
				t.Errorf("versions[%d] = %d, want %d (descending order broken across pages)", i, v, want)
			}
		}
	})

	t.Run("NilFilterEquivalentToDefault", func(t *testing.T) {
		t.Parallel()
		store := factory(t)
		vs := requireVersioned(t, store)
		ctx := context.Background()

		for i := 1; i <= 3; i++ {
			if _, err := store.Set(ctx, "ns", "k", config.NewValue(i)); err != nil {
				t.Fatalf("Set %d: %v", i, err)
			}
		}

		page, err := vs.GetVersions(ctx, "ns", "k", nil)
		if err != nil {
			t.Fatalf("GetVersions(nil filter): %v", err)
		}
		if len(page.Versions()) != 3 {
			t.Fatalf("nil filter returned %d versions, want 3", len(page.Versions()))
		}
	})

	t.Run("InvalidCursorRejected", func(t *testing.T) {
		t.Parallel()
		store := factory(t)
		vs := requireVersioned(t, store)
		ctx := context.Background()

		if _, err := store.Set(ctx, "ns", "k", config.NewValue(1)); err != nil {
			t.Fatalf("seed Set: %v", err)
		}

		// The VersionedStore contract requires garbage cursors to fail
		// rather than silently returning the first page. Whether the
		// error wraps [config.ErrInvalidCursor] is backend-specific —
		// memory currently returns a raw strconv error, while
		// NamespaceLister backends standardize on the envelope. Both
		// behaviors satisfy the "must fail" portion of the contract;
		// the suite locks only the contract, not the implementation.
		if _, err := vs.GetVersions(ctx, "ns", "k",
			config.NewVersionFilter().WithCursor("not-a-valid-cursor").Build()); err == nil {
			t.Fatal("GetVersions with garbage cursor returned nil error; expected failure")
		}
	})
}

// requireVersioned type-asserts the store to VersionedStore and skips
// the subtest when the backend does not opt in.
func requireVersioned(t *testing.T, store config.Store) config.VersionedStore {
	t.Helper()
	vs, ok := store.(config.VersionedStore)
	if !ok {
		t.Skip("store does not implement VersionedStore")
	}
	return vs
}
