package storetest

import (
	"context"
	"testing"

	"github.com/rbaliyan/config"
)

// RunBulkStoreSuite executes the [config.BulkStore] conformance contract
// against a backend. Like [RunStoreConformanceSuite] each subtest gets a
// fresh store from factory.
//
// Usage:
//
//	func TestSQLite_BulkStoreConformance(t *testing.T) {
//	    storetest.RunBulkStoreSuite(t, func(t *testing.T) config.Store {
//	        // ... return a connected sqlite.Store
//	    })
//	}
//
// Skips when the store does not implement [config.BulkStore]; the suite
// is opt-in per backend like the rest of storetest.
func RunBulkStoreSuite(t *testing.T, factory Factory) {
	t.Helper()

	t.Run("SetMany_GetMany_RoundTrip", func(t *testing.T) {
		t.Parallel()
		store := factory(t)
		bs := requireBulk(t, store)
		ctx := context.Background()

		in := map[string]config.Value{
			"a": config.NewValue("va"),
			"b": config.NewValue("vb"),
			"c": config.NewValue("vc"),
		}
		if err := bs.SetMany(ctx, "ns", in); err != nil {
			t.Fatalf("SetMany: %v", err)
		}

		out, err := bs.GetMany(ctx, "ns", []string{"a", "b", "c", "missing"})
		if err != nil {
			t.Fatalf("GetMany: %v", err)
		}
		if len(out) != 3 {
			t.Fatalf("GetMany returned %d entries, want 3 (missing must be absent): %v", len(out), keys(out))
		}
		for k, want := range map[string]string{"a": "va", "b": "vb", "c": "vc"} {
			got, ok := out[k]
			if !ok {
				t.Errorf("GetMany missing key %q", k)
				continue
			}
			s, _ := got.String()
			if s != want {
				t.Errorf("GetMany[%q] = %q, want %q", k, s, want)
			}
		}
	})

	t.Run("SetMany_InvalidKeyDoesNotBlockValidWrites", func(t *testing.T) {
		t.Parallel()
		store := factory(t)
		bs := requireBulk(t, store)
		ctx := context.Background()

		values := map[string]config.Value{
			"":           config.NewValue("invalid"), // empty key fails validation
			"valid/key":  config.NewValue("ok"),
			"valid/key2": config.NewValue("ok2"),
		}
		if err := bs.SetMany(ctx, "ns", values); err == nil {
			t.Fatal("SetMany returned nil error; expected partial-failure error for empty key")
		}

		// Valid keys must still be persisted.
		for _, k := range []string{"valid/key", "valid/key2"} {
			got, err := store.Get(ctx, "ns", k)
			if err != nil {
				t.Errorf("Get %q after partial SetMany: %v", k, err)
				continue
			}
			if s, _ := got.String(); s != map[string]string{"valid/key": "ok", "valid/key2": "ok2"}[k] {
				t.Errorf("Get %q = %q after partial SetMany", k, s)
			}
		}
	})

	t.Run("DeleteMany_CountsActualDeletes", func(t *testing.T) {
		t.Parallel()
		store := factory(t)
		bs := requireBulk(t, store)
		ctx := context.Background()

		if err := bs.SetMany(ctx, "ns", map[string]config.Value{
			"x": config.NewValue(1),
			"y": config.NewValue(2),
		}); err != nil {
			t.Fatalf("SetMany: %v", err)
		}

		n, err := bs.DeleteMany(ctx, "ns", []string{"x", "y", "missing"})
		if err != nil {
			t.Fatalf("DeleteMany: %v", err)
		}
		if n != 2 {
			t.Errorf("DeleteMany returned %d, want 2 (missing must not count)", n)
		}

		for _, k := range []string{"x", "y"} {
			if _, err := store.Get(ctx, "ns", k); !config.IsNotFound(err) {
				t.Errorf("Get %q after DeleteMany: err = %v, want ErrNotFound", k, err)
			}
		}
	})

	t.Run("GetMany_EmptyKeysReturnsEmpty", func(t *testing.T) {
		t.Parallel()
		store := factory(t)
		bs := requireBulk(t, store)

		out, err := bs.GetMany(context.Background(), "ns", nil)
		if err != nil {
			t.Fatalf("GetMany(nil): %v", err)
		}
		if len(out) != 0 {
			t.Errorf("GetMany(nil) returned %d entries, want 0", len(out))
		}
	})
}

// requireBulk type-asserts the store to BulkStore and skips the subtest
// when the backend does not opt in.
func requireBulk(t *testing.T, store config.Store) config.BulkStore {
	t.Helper()
	bs, ok := store.(config.BulkStore)
	if !ok {
		t.Skip("store does not implement BulkStore")
	}
	return bs
}
