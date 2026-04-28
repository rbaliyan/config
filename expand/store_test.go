package expand_test

import (
	"context"
	"testing"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/expand"
	"github.com/rbaliyan/config/memory"
)

// noBulkStore wraps a Store via the plain config.Store interface, hiding any
// BulkStore implementation so fallback paths in expand.Store can be tested.
type noBulkStore struct{ config.Store }

// seedStore connects a memory store and sets a single string key.
func seedStore(t *testing.T, ns, key, value string) *memory.Store {
	t.Helper()
	s := memory.NewStore()
	if err := s.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	v := config.NewValue(value, config.WithValueType(config.TypeString))
	if _, err := s.Set(context.Background(), ns, key, v); err != nil {
		t.Fatalf("Set(%q, %q): %v", ns, key, err)
	}
	return s
}

func TestNewStore_NilInner(t *testing.T) {
	_, err := expand.NewStore(nil, expand.WithDollarExpander(expand.EnvExpander()))
	if err == nil {
		t.Error("expected error for nil inner store")
	}
}

func TestNewStore_NoExpanders(t *testing.T) {
	inner := seedStore(t, "ns", "k", "v")
	_, err := expand.NewStore(inner)
	if err == nil {
		t.Error("expected error when no expanders provided")
	}
}

func TestStore_GetDollar(t *testing.T) {
	t.Setenv("EXPAND_STORE_HOST", "db.example.com")

	inner := seedStore(t, "app", "host", "${EXPAND_STORE_HOST}")
	s, err := expand.NewStore(inner, expand.WithDollarExpander(expand.EnvExpander()))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}

	v, err := s.Get(context.Background(), "app", "host")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got, _ := v.String()
	if got != "db.example.com" {
		t.Errorf("Get: got %q, want db.example.com", got)
	}
}

func TestStore_GetAngle(t *testing.T) {
	t.Setenv("EXPAND_STORE_PORT", "5432")

	inner := seedStore(t, "app", "port", "<EXPAND_STORE_PORT>")
	s, err := expand.NewStore(inner, expand.WithAngleExpander(expand.EnvExpander()))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}

	v, err := s.Get(context.Background(), "app", "port")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got, _ := v.String()
	if got != "5432" {
		t.Errorf("Get: got %q, want 5432", got)
	}
}

func TestStore_GetBoth(t *testing.T) {
	t.Setenv("EXPAND_BOTH_HOST", "pg.internal")

	secrets := map[string]string{"db_pass": "hunter2"}
	secretFn := func(name string) (string, bool) { v, ok := secrets[name]; return v, ok }

	inner := memory.NewStore()
	if err := inner.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = inner.Close(context.Background()) })

	set := func(ns, key, val string) {
		v := config.NewValue(val, config.WithValueType(config.TypeString))
		if _, err := inner.Set(context.Background(), ns, key, v); err != nil {
			t.Fatalf("Set(%q, %q): %v", ns, key, err)
		}
	}
	set("db", "host", "${EXPAND_BOTH_HOST}")
	set("db", "pass", "<db_pass>")

	s, err := expand.NewStore(inner,
		expand.WithDollarExpander(expand.EnvExpander()),
		expand.WithAngleExpander(secretFn),
	)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}

	for _, tc := range []struct{ key, want string }{
		{"host", "pg.internal"},
		{"pass", "hunter2"},
	} {
		v, err := s.Get(context.Background(), "db", tc.key)
		if err != nil {
			t.Fatalf("Get(%q): %v", tc.key, err)
		}
		got, _ := v.String()
		if got != tc.want {
			t.Errorf("key %q: got %q, want %q", tc.key, got, tc.want)
		}
	}
}

func TestStore_GetNotFound(t *testing.T) {
	inner := seedStore(t, "ns", "k", "v")
	s, err := expand.NewStore(inner, expand.WithDollarExpander(expand.EnvExpander()))
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Get(context.Background(), "ns", "missing")
	if !config.IsNotFound(err) {
		t.Errorf("Get missing key: got %v, want ErrNotFound", err)
	}
}

func TestStore_NonStringUnchanged(t *testing.T) {
	inner := memory.NewStore()
	if err := inner.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = inner.Close(context.Background()) })

	v := config.NewValue(42, config.WithValueType(config.TypeInt))
	if _, err := inner.Set(context.Background(), "ns", "count", v); err != nil {
		t.Fatal(err)
	}

	s, err := expand.NewStore(inner, expand.WithDollarExpander(expand.EnvExpander()))
	if err != nil {
		t.Fatal(err)
	}

	got, err := s.Get(context.Background(), "ns", "count")
	if err != nil {
		t.Fatal(err)
	}
	n, err := got.Int64()
	if err != nil {
		t.Fatalf("Int64: %v", err)
	}
	if n != 42 {
		t.Errorf("Int64 = %d, want 42", n)
	}
}

func TestStore_FindExpands(t *testing.T) {
	t.Setenv("EXPAND_FIND_HOST", "find-host")

	inner := memory.NewStore()
	if err := inner.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = inner.Close(context.Background()) })

	set := func(key, val string) {
		v := config.NewValue(val, config.WithValueType(config.TypeString))
		if _, err := inner.Set(context.Background(), "ns", key, v); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	set("host", "${EXPAND_FIND_HOST}")
	set("static", "no-expand")

	s, err := expand.NewStore(inner, expand.WithDollarExpander(expand.EnvExpander()))
	if err != nil {
		t.Fatal(err)
	}

	page, err := s.Find(context.Background(), "ns", config.NewFilter().Build())
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	results := page.Results()
	if v, ok := results["host"]; !ok {
		t.Error("Find: host key missing")
	} else if got, _ := v.String(); got != "find-host" {
		t.Errorf("Find host: got %q, want find-host", got)
	}
	if v, ok := results["static"]; !ok {
		t.Error("Find: static key missing")
	} else if got, _ := v.String(); got != "no-expand" {
		t.Errorf("Find static: got %q, want no-expand", got)
	}
}

func TestStore_MetadataPreserved(t *testing.T) {
	t.Setenv("EXPAND_META_VAR", "expanded")

	inner := seedStore(t, "ns", "k", "${EXPAND_META_VAR}")
	original, _ := inner.Get(context.Background(), "ns", "k")
	origMeta := original.Metadata()

	s, err := expand.NewStore(inner, expand.WithDollarExpander(expand.EnvExpander()))
	if err != nil {
		t.Fatal(err)
	}

	got, err := s.Get(context.Background(), "ns", "k")
	if err != nil {
		t.Fatal(err)
	}
	gotMeta := got.Metadata()
	if gotMeta.Version() != origMeta.Version() {
		t.Errorf("Version: got %d, want %d", gotMeta.Version(), origMeta.Version())
	}
}

func TestStore_UnwrapReturnsInner(t *testing.T) {
	inner := seedStore(t, "ns", "k", "v")
	s, err := expand.NewStore(inner, expand.WithDollarExpander(expand.EnvExpander()))
	if err != nil {
		t.Fatal(err)
	}
	if s.Unwrap() != inner {
		t.Error("Unwrap did not return the inner store")
	}
}

func TestStore_DollarChainedExpanders(t *testing.T) {
	overrides := map[string]string{"CHAIN_KEY": "override"}
	overrideFn := func(name string) (string, bool) { v, ok := overrides[name]; return v, ok }

	t.Setenv("CHAIN_KEY", "from-env")

	inner := seedStore(t, "ns", "k", "${CHAIN_KEY}")
	// override wins over env because it is first in the chain
	s, err := expand.NewStore(inner, expand.WithDollarExpander(overrideFn), expand.WithDollarExpander(expand.EnvExpander()))
	if err != nil {
		t.Fatal(err)
	}

	v, err := s.Get(context.Background(), "ns", "k")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := v.String()
	if got != "override" {
		t.Errorf("chained: got %q, want override", got)
	}
}

func TestStore_GetMany(t *testing.T) {
	t.Setenv("EXPAND_MANY_A", "val-a")
	t.Setenv("EXPAND_MANY_B", "val-b")

	inner := memory.NewStore()
	if err := inner.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = inner.Close(context.Background()) })

	for _, kv := range [][2]string{
		{"a", "${EXPAND_MANY_A}"},
		{"b", "${EXPAND_MANY_B}"},
		{"c", "static"},
	} {
		v := config.NewValue(kv[1], config.WithValueType(config.TypeString))
		if _, err := inner.Set(context.Background(), "ns", kv[0], v); err != nil {
			t.Fatal(err)
		}
	}

	s, err := expand.NewStore(inner, expand.WithDollarExpander(expand.EnvExpander()))
	if err != nil {
		t.Fatal(err)
	}

	results, err := s.GetMany(context.Background(), "ns", []string{"a", "b", "c", "missing"})
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	want := map[string]string{"a": "val-a", "b": "val-b", "c": "static"}
	for k, wv := range want {
		rv, ok := results[k]
		if !ok {
			t.Errorf("GetMany: key %q missing", k)
			continue
		}
		got, _ := rv.String()
		if got != wv {
			t.Errorf("GetMany[%q]: got %q, want %q", k, got, wv)
		}
	}
	if _, ok := results["missing"]; ok {
		t.Error("GetMany: missing key should not appear in results")
	}
}

func TestStore_DeleteMany(t *testing.T) {
	inner := memory.NewStore()
	if err := inner.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = inner.Close(context.Background()) })

	for _, key := range []string{"x", "y"} {
		v := config.NewValue("v", config.WithValueType(config.TypeString))
		if _, err := inner.Set(context.Background(), "ns", key, v); err != nil {
			t.Fatal(err)
		}
	}

	s, err := expand.NewStore(inner, expand.WithDollarExpander(expand.EnvExpander()))
	if err != nil {
		t.Fatal(err)
	}

	n, err := s.DeleteMany(context.Background(), "ns", []string{"x", "y", "gone"})
	if err != nil {
		t.Fatalf("DeleteMany: %v", err)
	}
	if n != 2 {
		t.Errorf("DeleteMany: deleted %d, want 2", n)
	}
}

func TestStore_GetManyFallback(t *testing.T) {
	t.Setenv("EXPAND_FB_A", "a-val")
	t.Setenv("EXPAND_FB_B", "b-val")

	inner := memory.NewStore()
	if err := inner.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = inner.Close(context.Background()) })

	for _, kv := range [][2]string{{"a", "${EXPAND_FB_A}"}, {"b", "${EXPAND_FB_B}"}} {
		v := config.NewValue(kv[1], config.WithValueType(config.TypeString))
		if _, err := inner.Set(context.Background(), "ns", kv[0], v); err != nil {
			t.Fatal(err)
		}
	}

	// Wrap in noBulkStore to force the per-key fallback path.
	s, err := expand.NewStore(noBulkStore{inner}, expand.WithDollarExpander(expand.EnvExpander()))
	if err != nil {
		t.Fatal(err)
	}

	results, err := s.GetMany(context.Background(), "ns", []string{"a", "b", "missing"})
	if err != nil {
		t.Fatalf("GetMany fallback: %v", err)
	}
	for k, want := range map[string]string{"a": "a-val", "b": "b-val"} {
		rv, ok := results[k]
		if !ok {
			t.Errorf("GetMany fallback: key %q missing", k)
			continue
		}
		got, _ := rv.String()
		if got != want {
			t.Errorf("GetMany fallback[%q]: got %q, want %q", k, got, want)
		}
	}
	if _, ok := results["missing"]; ok {
		t.Error("GetMany fallback: missing key should not be in results")
	}
}

func TestStore_DeleteManyFallback(t *testing.T) {
	inner := memory.NewStore()
	if err := inner.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = inner.Close(context.Background()) })

	for _, key := range []string{"x", "y"} {
		v := config.NewValue("v", config.WithValueType(config.TypeString))
		if _, err := inner.Set(context.Background(), "ns", key, v); err != nil {
			t.Fatal(err)
		}
	}

	s, err := expand.NewStore(noBulkStore{inner}, expand.WithDollarExpander(expand.EnvExpander()))
	if err != nil {
		t.Fatal(err)
	}

	n, err := s.DeleteMany(context.Background(), "ns", []string{"x", "y", "gone"})
	if err != nil {
		t.Fatalf("DeleteMany fallback: %v", err)
	}
	if n != 2 {
		t.Errorf("DeleteMany fallback: deleted %d, want 2", n)
	}
}

func TestStore_SetManyWriteModeStripped(t *testing.T) {
	// SetMany must use upsert semantics; create-only hint must be stripped in
	// the fallback path so the write succeeds even if the key already exists.
	inner := memory.NewStore()
	if err := inner.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = inner.Close(context.Background()) })

	// Pre-seed the key so a WriteModeCreate would fail on a direct Set.
	existing := config.NewValue("old", config.WithValueType(config.TypeString))
	if _, err := inner.Set(context.Background(), "ns", "k", existing); err != nil {
		t.Fatal(err)
	}

	s, err := expand.NewStore(noBulkStore{inner}, expand.WithDollarExpander(expand.EnvExpander()))
	if err != nil {
		t.Fatal(err)
	}

	// Pass a value with WriteModeCreate — SetMany must still succeed (upsert).
	newVal := config.NewValue("new",
		config.WithValueType(config.TypeString),
		config.WithValueWriteMode(config.WriteModeCreate),
	)
	if err := s.SetMany(context.Background(), "ns", map[string]config.Value{"k": newVal}); err != nil {
		t.Fatalf("SetMany with WriteModeCreate should succeed (upsert): %v", err)
	}

	got, err := inner.Get(context.Background(), "ns", "k")
	if err != nil {
		t.Fatal(err)
	}
	if sv, _ := got.String(); sv != "new" {
		t.Errorf("value after SetMany: got %q, want new", sv)
	}
}

// staleStore is a minimal config.Store that returns a pre-set stale value from Get.
type staleStore struct {
	config.Store
	ns, key string
	val     config.Value
}

func (s *staleStore) Get(_ context.Context, ns, key string) (config.Value, error) {
	if ns == s.ns && key == s.key {
		return s.val, nil
	}
	return s.Store.Get(context.Background(), ns, key)
}

func TestStore_StalePreserved(t *testing.T) {
	t.Setenv("EXPAND_STALE_VAR", "live-val")

	inner := seedStore(t, "ns", "k", "${EXPAND_STALE_VAR}")

	// Wrap inner so Get("ns","k") returns a stale placeholder value.
	raw := config.NewValue("${EXPAND_STALE_VAR}", config.WithValueType(config.TypeString))
	staleInner := &staleStore{Store: inner, ns: "ns", key: "k", val: config.MarkStale(raw)}

	s, err := expand.NewStore(staleInner, expand.WithDollarExpander(expand.EnvExpander()))
	if err != nil {
		t.Fatal(err)
	}

	got, err := s.Get(context.Background(), "ns", "k")
	if err != nil {
		t.Fatal(err)
	}
	sv, _ := got.String()
	if sv != "live-val" {
		t.Errorf("expanded value: got %q, want live-val", sv)
	}
	if !got.Metadata().IsStale() {
		t.Error("expanded value should preserve IsStale = true")
	}
}

func TestStore_AliasStoreNotSupported(t *testing.T) {
	// A plain store without AliasStore should return errors for alias ops.
	inner := seedStore(t, "ns", "k", "v")
	s, err := expand.NewStore(noBulkStore{inner}, expand.WithDollarExpander(expand.EnvExpander()))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := s.SetAlias(context.Background(), "old", "new"); err == nil {
		t.Error("SetAlias: expected error for non-AliasStore inner")
	}
	if err := s.DeleteAlias(context.Background(), "old"); err == nil {
		t.Error("DeleteAlias: expected error for non-AliasStore inner")
	}
	if _, err := s.GetAlias(context.Background(), "old"); err == nil {
		t.Error("GetAlias: expected error for non-AliasStore inner")
	}
	if _, err := s.ListAliases(context.Background()); err == nil {
		t.Error("ListAliases: expected error for non-AliasStore inner")
	}
}

func TestStore_Health(t *testing.T) {
	inner := seedStore(t, "ns", "k", "v")
	s, err := expand.NewStore(inner, expand.WithDollarExpander(expand.EnvExpander()))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Health(context.Background()); err != nil {
		t.Errorf("Health: %v", err)
	}
}

func TestStore_ConnectClose(t *testing.T) {
	inner := memory.NewStore()
	s, err := expand.NewStore(inner, expand.WithDollarExpander(expand.EnvExpander()))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
