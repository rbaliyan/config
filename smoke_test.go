// Package config smoke tests pin the golden paths across every in-process
// backend so a fast `just smoke` run can gate every PR before slower
// integration suites get a chance to fail.
//
// Smoke tests are hermetic — they spin up only in-process backends
// (memory, sqlite :memory:, file TempDir) and avoid any wall-clock waits
// over a few milliseconds. They are NOT a substitute for the integration
// suite at `just test-integration`; they are the cheap pre-merge net.
package config_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/bind"
	"github.com/rbaliyan/config/codec"
	_ "github.com/rbaliyan/config/codec/toml"
	_ "github.com/rbaliyan/config/codec/yaml"
	"github.com/rbaliyan/config/expand"
	"github.com/rbaliyan/config/file"
	"github.com/rbaliyan/config/live"
	"github.com/rbaliyan/config/memory"
	"github.com/rbaliyan/config/multi"
	"github.com/rbaliyan/config/otel"
	"github.com/rbaliyan/config/replica"
	"github.com/rbaliyan/config/sqlite"
	"github.com/rbaliyan/config/transform"

	_ "modernc.org/sqlite"
)

// smokeCtx returns a bounded context appropriate for smoke tests: long
// enough that a stuck backend fails the test rather than hanging the
// suite, short enough that genuine bugs surface quickly.
func smokeCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// TestSmoke_MemoryRoundTrip pins the simplest possible happy path:
// connect, set, get, delete. If this breaks, nothing else can work.
func TestSmoke_MemoryRoundTrip(t *testing.T) {
	t.Parallel()
	ctx := smokeCtx(t)

	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(ctx) })

	v, err := store.Set(ctx, "smoke", "k", config.NewValue(42))
	if err != nil {
		t.Fatalf("Set: %v", err)
	}
	if got, _ := v.Int64(); got != 42 {
		t.Fatalf("Set returned %d, want 42", got)
	}

	got, err := store.Get(ctx, "smoke", "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if n, _ := got.Int64(); n != 42 {
		t.Fatalf("Get returned %d, want 42", n)
	}

	if err := store.Delete(ctx, "smoke", "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := store.Get(ctx, "smoke", "k"); !config.IsNotFound(err) {
		t.Fatalf("Get after Delete returned %v, want ErrNotFound", err)
	}
}

// TestSmoke_SQLiteRoundTrip is the cheapest non-memory smoke. SQLite via
// `:memory:` keeps the test hermetic while exercising a real SQL store.
func TestSmoke_SQLiteRoundTrip(t *testing.T) {
	t.Parallel()
	ctx := smokeCtx(t)

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })

	store := sqlite.NewStore(db)
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(ctx) })

	if _, err := store.Set(ctx, "smoke", "port", config.NewValue(8080)); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, err := store.Get(ctx, "smoke", "port")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if n, _ := got.Int64(); n != 8080 {
		t.Fatalf("Get returned %d, want 8080", n)
	}
}

// TestSmoke_FileStoreTempDir exercises the file backend via t.TempDir().
// The file store maps top-level JSON keys to namespaces and nested keys to
// entries; smoke just verifies Connect + Get against a pre-written JSON
// file returns the expected shape.
func TestSmoke_FileStoreTempDir(t *testing.T) {
	t.Parallel()
	ctx := smokeCtx(t)

	path := filepath.Join(t.TempDir(), "smoke.json")
	if err := os.WriteFile(path, []byte(`{"smoke": {"port": 9090}}`), 0o600); err != nil {
		t.Fatal(err)
	}

	store := file.NewStore(path)
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(ctx) })

	got, err := store.Get(ctx, "smoke", "port")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if n, _ := got.Int64(); n != 9090 {
		t.Fatalf("Get returned %d, want 9090", n)
	}
}

// TestSmoke_MemoryWatch pins the Watch contract: a Set after the watch
// channel is established produces exactly one ChangeTypeSet event.
func TestSmoke_MemoryWatch(t *testing.T) {
	t.Parallel()
	ctx := smokeCtx(t)

	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(ctx) })

	watchCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	ch, err := store.Watch(watchCtx, config.WatchFilter{Namespaces: []string{"smoke"}})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	if _, err := store.Set(ctx, "smoke", "k", config.NewValue("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	select {
	case ev, ok := <-ch:
		if !ok {
			t.Fatal("watch channel closed unexpectedly")
		}
		if ev.Type != config.ChangeTypeSet {
			t.Fatalf("ChangeType = %v, want ChangeTypeSet", ev.Type)
		}
		if ev.Namespace != "smoke" || ev.Key != "k" {
			t.Fatalf("event = %+v, want namespace=smoke key=k", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no watch event received within 2s")
	}
}

// TestSmoke_NotFoundSentinel pins the documented sentinel-error contract.
// Errors must wrap config.ErrNotFound; config.IsNotFound must report true.
func TestSmoke_NotFoundSentinel(t *testing.T) {
	t.Parallel()
	ctx := smokeCtx(t)

	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(ctx) })

	_, err := store.Get(ctx, "smoke", "missing")
	if !errors.Is(err, config.ErrNotFound) {
		t.Fatalf("errors.Is(err, ErrNotFound) = false, err = %v", err)
	}
	if !config.IsNotFound(err) {
		t.Fatalf("IsNotFound = false, err = %v", err)
	}
}

// TestSmoke_CodecAutoRegistration pins the documented init()-based codec
// registration. Importing codec/yaml and codec/toml should make them
// available via codec.Get; the secret codec is registered by the root
// package.
func TestSmoke_CodecAutoRegistration(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"json", "yaml", "toml", "secret"} {
		if c := codec.Get(name); c == nil {
			t.Errorf("codec.Get(%q) = nil; expected registered codec", name)
		}
	}
}

// TestSmoke_NamespaceLister pins the documented NamespaceLister
// pagination contract on the memory store. Names are returned
// byte-wise sorted and the first page is bounded by limit.
func TestSmoke_NamespaceLister(t *testing.T) {
	t.Parallel()
	ctx := smokeCtx(t)

	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(ctx) })

	for _, ns := range []string{"alpha", "beta", "gamma"} {
		if _, err := store.Set(ctx, ns, "k", config.NewValue(1)); err != nil {
			t.Fatalf("Set %q: %v", ns, err)
		}
	}

	nl, ok := any(store).(config.NamespaceLister)
	if !ok {
		t.Fatal("memory store does not implement NamespaceLister")
	}
	names, cursor, err := nl.ListNamespaces(ctx, "", 10, "")
	if err != nil {
		t.Fatalf("ListNamespaces: %v", err)
	}
	if cursor != "" {
		t.Errorf("nextCursor = %q, want empty (only 3 namespaces, limit 10)", cursor)
	}
	want := []string{"alpha", "beta", "gamma"}
	if len(names) != len(want) {
		t.Fatalf("got %d names, want %d: %v", len(names), len(want), names)
	}
	for i, n := range want {
		if names[i] != n {
			t.Errorf("names[%d] = %q, want %q (byte-wise order broken)", i, names[i], n)
		}
	}
}

// TestSmoke_MultiStoreFallback pins the StrategyFallback contract: a write
// to the multi-store propagates to all backing stores, and a read from a
// fresh secondary returns the value.
func TestSmoke_MultiStoreFallback(t *testing.T) {
	t.Parallel()
	ctx := smokeCtx(t)

	primary := memory.NewStore()
	secondary := memory.NewStore()
	m := multi.NewStore([]config.Store{primary, secondary}, multi.WithStrategy(multi.StrategyFallback))
	if err := m.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = m.Close(ctx) })

	if _, err := m.Set(ctx, "smoke", "k", config.NewValue("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// StrategyFallback writes to all stores — secondary should see it.
	v, err := secondary.Get(ctx, "smoke", "k")
	if err != nil {
		t.Fatalf("secondary.Get: %v", err)
	}
	if got, _ := v.String(); got != "v" {
		t.Fatalf("secondary returned %q, want %q", got, "v")
	}
}

// TestSmoke_LiveRef pins the live.New + Load contract for a typed config
// struct. Catches regressions in the polling-driven binding pipeline.
func TestSmoke_LiveRef(t *testing.T) {
	t.Parallel()
	ctx := smokeCtx(t)

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("config.New: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close(ctx) })

	cfg := mgr.Namespace("app")
	if err := cfg.Set(ctx, "server/port", 8080); err != nil {
		t.Fatalf("Set: %v", err)
	}

	type serverCfg struct {
		Port int `config:"port"`
	}
	ref, err := live.New(ctx, cfg, "server",
		live.WithRefPollInterval[serverCfg](time.Hour), // disable background polling for the smoke
	)
	if err != nil {
		t.Fatalf("live.New: %v", err)
	}
	t.Cleanup(ref.Close)

	snap := ref.Load()
	if snap == nil {
		t.Fatal("ref.Load() = nil")
	}
	if snap.Port != 8080 {
		t.Fatalf("snap.Port = %d, want 8080", snap.Port)
	}
}

// TestSmoke_StoreStatsImmutable pins the v0.8.0 StoreStats interface
// contract: construct via NewStoreStats, observe via interface methods,
// round-trip through MarshalJSON / UnmarshalStoreStats.
func TestSmoke_StoreStatsImmutable(t *testing.T) {
	t.Parallel()

	byType := map[config.Type]int64{config.TypeInt: 2, config.TypeString: 1}
	byNs := map[string]int64{"a": 2, "b": 1}
	s := config.NewStoreStats(3, byType, byNs)

	if s.TotalEntries() != 3 {
		t.Errorf("TotalEntries = %d, want 3", s.TotalEntries())
	}
	if s.CountForType(config.TypeInt) != 2 {
		t.Errorf("CountForType(Int) = %d, want 2", s.CountForType(config.TypeInt))
	}
	if s.CountForNamespace("b") != 1 {
		t.Errorf("CountForNamespace(b) = %d, want 1", s.CountForNamespace("b"))
	}

	data, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := config.UnmarshalStoreStats(data)
	if err != nil {
		t.Fatalf("UnmarshalStoreStats: %v", err)
	}
	if got.TotalEntries() != 3 {
		t.Errorf("round-trip TotalEntries = %d, want 3", got.TotalEntries())
	}
	if got.CountForType(config.TypeInt) != 2 {
		t.Errorf("round-trip CountForType(Int) = %d, want 2", got.CountForType(config.TypeInt))
	}

	// Construction must clone the input maps so caller mutations do not
	// leak into the snapshot.
	byType[config.TypeInt] = 999
	if s.CountForType(config.TypeInt) != 2 {
		t.Error("NewStoreStats did not clone byType — caller mutation leaked into snapshot")
	}
}

// TestSmoke_BindStruct pins bind.New + Bind + GetStruct happy path with a
// struct config tag. Catches regressions in mapstructure integration.
func TestSmoke_BindStruct(t *testing.T) {
	t.Parallel()
	ctx := smokeCtx(t)

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("config.New: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close(ctx) })

	cfg := mgr.Namespace("svc")
	if err := cfg.Set(ctx, "database/port", 5432); err != nil {
		t.Fatalf("Set: %v", err)
	}

	type dbCfg struct {
		Port int `config:"port"`
	}
	var out dbCfg
	b := bind.New(cfg)
	if err := b.Bind().GetStruct(ctx, "database", &out); err != nil {
		t.Fatalf("GetStruct: %v", err)
	}
	if out.Port != 5432 {
		t.Fatalf("out.Port = %d, want 5432", out.Port)
	}
}

// TestSmoke_OTelWrapNoop pins the documented opt-in OTel contract: wrapping
// a store with both flags disabled (the default) yields a working
// pass-through with no provider required.
func TestSmoke_OTelWrapNoop(t *testing.T) {
	t.Parallel()
	ctx := smokeCtx(t)

	wrapped, err := otel.WrapStore(memory.NewStore())
	if err != nil {
		t.Fatalf("WrapStore: %v", err)
	}
	if err := wrapped.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = wrapped.Close(ctx) })

	if _, err := wrapped.Set(ctx, "smoke", "k", config.NewValue(1)); err != nil {
		t.Fatalf("Set: %v", err)
	}
	v, err := wrapped.Get(ctx, "smoke", "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if n, _ := v.Int64(); n != 1 {
		t.Fatalf("Get returned %d, want 1", n)
	}
}

// TestSmoke_BulkStore pins the BulkStore contract (GetMany / SetMany /
// DeleteMany) on the memory backend, which is the reference implementation.
func TestSmoke_BulkStore(t *testing.T) {
	t.Parallel()
	ctx := smokeCtx(t)

	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(ctx) })

	bs, ok := any(store).(config.BulkStore)
	if !ok {
		t.Fatal("memory store does not implement BulkStore")
	}

	values := map[string]config.Value{
		"a": config.NewValue(1),
		"b": config.NewValue(2),
	}
	if err := bs.SetMany(ctx, "smoke", values); err != nil {
		t.Fatalf("SetMany: %v", err)
	}

	got, err := bs.GetMany(ctx, "smoke", []string{"a", "b", "missing"})
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("GetMany returned %d entries, want 2 (missing keys must be absent)", len(got))
	}

	n, err := bs.DeleteMany(ctx, "smoke", []string{"a", "b"})
	if err != nil {
		t.Fatalf("DeleteMany: %v", err)
	}
	if n != 2 {
		t.Fatalf("DeleteMany returned %d, want 2", n)
	}
}

// TestSmoke_ConditionalWrites pins WithIfNotExists and WithIfExists
// contracts at the Config level. Catches regressions in WriteMode plumbing.
func TestSmoke_ConditionalWrites(t *testing.T) {
	t.Parallel()
	ctx := smokeCtx(t)

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("config.New: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close(ctx) })
	cfg := mgr.Namespace("smoke")

	// IfExists on a missing key — must fail with ErrNotFound.
	if err := cfg.Set(ctx, "k", 1, config.WithIfExists()); !config.IsNotFound(err) {
		t.Fatalf("WithIfExists on missing key: err = %v, want ErrNotFound", err)
	}

	// Plain create.
	if err := cfg.Set(ctx, "k", 1); err != nil {
		t.Fatalf("Set initial: %v", err)
	}

	// IfNotExists on existing key — must fail with ErrKeyExists.
	err = cfg.Set(ctx, "k", 2, config.WithIfNotExists())
	if !config.IsKeyExists(err) {
		t.Fatalf("WithIfNotExists on existing key: err = %v, want ErrKeyExists", err)
	}

	// IfExists on existing key — must succeed.
	if err := cfg.Set(ctx, "k", 3, config.WithIfExists()); err != nil {
		t.Fatalf("WithIfExists on existing key: %v", err)
	}
	v, _ := cfg.Get(ctx, "k")
	if n, _ := v.Int64(); n != 3 {
		t.Fatalf("after WithIfExists update, value = %d, want 3", n)
	}
}

// TestSmoke_SecretValue pins the redaction contract: a stored Value of
// TypeSecret always returns the mask from String() and the type survives
// a round-trip through the store. SecretFrom recovery is verified at the
// SQL/document-backend level by the integration suite (the memory store
// wraps stored Values in a metadata envelope that hides the *val type
// SecretFrom would otherwise reach into — that wrapper is intentional,
// so the smoke just pins the externally observable contract).
func TestSmoke_SecretValue(t *testing.T) {
	t.Parallel()
	ctx := smokeCtx(t)

	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(ctx) })

	val := config.NewSecretValue(config.NewSecret("hunter2"))
	if _, err := store.Set(ctx, "smoke", "api_key", val); err != nil {
		t.Fatalf("Set: %v", err)
	}

	got, err := store.Get(ctx, "smoke", "api_key")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Type() != config.TypeSecret {
		t.Fatalf("Type = %v, want TypeSecret", got.Type())
	}
	masked, err := got.String()
	if err != nil {
		t.Fatalf("String: %v", err)
	}
	if masked != "******" {
		t.Fatalf("String() = %q, want \"******\" (plaintext leak)", masked)
	}
}

// smokeXORTransformer is a deterministic, reversible byte transformer used to
// exercise the transform decorator without pulling in a real codec. XOR with a
// fixed key is its own inverse, so Transform then Reverse is the identity.
type smokeXORTransformer struct{ key byte }

func (x smokeXORTransformer) Name() string { return "smoke-xor" }

func (x smokeXORTransformer) Transform(_ context.Context, data []byte) ([]byte, error) {
	out := make([]byte, len(data))
	for i, b := range data {
		out[i] = b ^ x.key
	}
	return out, nil
}

func (x smokeXORTransformer) Reverse(ctx context.Context, data []byte) ([]byte, error) {
	return x.Transform(ctx, data) // XOR is its own inverse.
}

// TestSmoke_TransformRoundTrip pins the transform decorator: a value written
// through the wrapper must come back out byte-identical after the
// Transform/Reverse pair. A broken transformer would corrupt every read.
func TestSmoke_TransformRoundTrip(t *testing.T) {
	t.Parallel()
	ctx := smokeCtx(t)

	s, err := transform.WrapStore(memory.NewStore(), smokeXORTransformer{key: 0x42})
	if err != nil {
		t.Fatalf("WrapStore: %v", err)
	}
	if err := s.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(ctx) })

	if _, err := s.Set(ctx, "smoke", "k", config.NewValue("hello")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, err := s.Get(ctx, "smoke", "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if v, _ := got.String(); v != "hello" {
		t.Fatalf("Get returned %q, want %q (transform round-trip broken)", v, "hello")
	}
}

// TestSmoke_ExpandStore pins query-time placeholder expansion: a value seeded
// with ${VAR} in the backing store must be substituted from the environment on
// read. Regressions here silently ship unresolved placeholders to callers.
func TestSmoke_ExpandStore(t *testing.T) {
	// No t.Parallel(): t.Setenv is incompatible with parallel tests.
	t.Setenv("SMOKE_EXPAND_VAR", "resolved")
	ctx := smokeCtx(t)

	base := memory.NewStore()
	if err := base.Connect(ctx); err != nil {
		t.Fatalf("base.Connect: %v", err)
	}
	t.Cleanup(func() { _ = base.Close(ctx) })

	if _, err := base.Set(ctx, "smoke", "k", config.NewValue("v=${SMOKE_EXPAND_VAR}")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	s, err := expand.NewStore(base, expand.WithDollarExpander(expand.EnvExpander()))
	if err != nil {
		t.Fatalf("expand.NewStore: %v", err)
	}

	got, err := s.Get(ctx, "smoke", "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if v, _ := got.String(); v != "v=resolved" {
		t.Fatalf("Get returned %q, want %q (env expansion broken)", v, "v=resolved")
	}
}

// TestSmoke_ReplicaStore pins the replica primary/secondary contract: a write
// to the primary is readable through the replica facade. Two in-memory stores
// keep it hermetic.
func TestSmoke_ReplicaStore(t *testing.T) {
	t.Parallel()
	ctx := smokeCtx(t)

	primary := memory.NewStore()
	secondary := memory.NewStore()
	s := replica.NewStore(primary, []config.Store{secondary})
	if err := s.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(ctx) })

	if _, err := s.Set(ctx, "smoke", "k", config.NewValue("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, err := s.Get(ctx, "smoke", "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if v, _ := got.String(); v != "v" {
		t.Fatalf("Get returned %q, want %q", v, "v")
	}
}

// TestSmoke_SQLiteFullRoundTrip extends the SQLite smoke past Get: it pins the
// full Set -> Get -> Delete -> Get(ErrNotFound) lifecycle so a regression in
// SQL DELETE or the not-found mapping surfaces in the fast gate.
func TestSmoke_SQLiteFullRoundTrip(t *testing.T) {
	t.Parallel()
	ctx := smokeCtx(t)

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })

	store := sqlite.NewStore(db)
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(ctx) })

	if _, err := store.Set(ctx, "smoke", "k", config.NewValue("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, err := store.Get(ctx, "smoke", "k"); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if err := store.Delete(ctx, "smoke", "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := store.Get(ctx, "smoke", "k"); !errors.Is(err, config.ErrNotFound) {
		t.Fatalf("Get after Delete returned %v, want ErrNotFound", err)
	}
}

// TestSmoke_ManagerCacheHit pins the Manager's resilience cache. The cache is
// store-first: a normal Get reads the backend and refreshes the cache, so a
// genuine cache hit is only recorded when the backend errors and the warmed
// value is served as a fallback. This test warms the cache, fails the backend,
// and asserts the stale value is served and CacheStats().Hits advances. A
// regression that drops the fallback path would surface as a failed Get here.
func TestSmoke_ManagerCacheHit(t *testing.T) {
	t.Parallel()
	ctx := smokeCtx(t)

	store := memory.NewStore()
	mgr, err := config.New(config.WithStore(store))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close(ctx) })

	cfg := mgr.Namespace("smoke")
	if err := cfg.Set(ctx, "k", "v"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, err := cfg.Get(ctx, "k"); err != nil { // warms the resilience cache
		t.Fatalf("Get (warm): %v", err)
	}

	// Fail the backend out from under the manager. The next Get must fall back
	// to the warmed cache rather than propagating the store error.
	if err := store.Close(ctx); err != nil {
		t.Fatalf("store.Close: %v", err)
	}

	got, err := cfg.Get(ctx, "k")
	if err != nil {
		t.Fatalf("Get (fallback): %v (resilience cache not serving stale value)", err)
	}
	if v, _ := got.String(); v != "v" {
		t.Fatalf("fallback Get returned %q, want %q", v, "v")
	}

	obs, ok := mgr.(config.ManagerObserver)
	if !ok {
		t.Fatal("Manager does not implement ManagerObserver")
	}
	if hits := obs.CacheStats().Hits; hits < 1 {
		t.Fatalf("CacheStats().Hits = %d, want >= 1 (cache fallback not recorded)", hits)
	}
}

// TestSmoke_FindPagination pins the Find limit contract: with three keys
// seeded and a limit of two, exactly two results must come back. A broken
// limit would either return everything or nothing.
func TestSmoke_FindPagination(t *testing.T) {
	t.Parallel()
	ctx := smokeCtx(t)

	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(ctx) })

	for _, k := range []string{"a", "b", "c"} {
		if _, err := store.Set(ctx, "smoke", k, config.NewValue(1)); err != nil {
			t.Fatalf("Set %q: %v", k, err)
		}
	}

	page, err := store.Find(ctx, "smoke", config.NewFilter().WithLimit(2).Build())
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if got := len(page.Results()); got != 2 {
		t.Fatalf("Find with limit 2 returned %d results, want 2", got)
	}
}

// TestSmoke_MultiReadThrough pins StrategyReadThrough cache population: a key
// present only in the backend must be copied into the front cache store after
// a Get goes through the multi-store. Catches a read-through that reads but
// never back-fills.
func TestSmoke_MultiReadThrough(t *testing.T) {
	t.Parallel()
	ctx := smokeCtx(t)

	cache := memory.NewStore()
	backend := memory.NewStore()
	m := multi.NewStore([]config.Store{cache, backend}, multi.WithStrategy(multi.StrategyReadThrough))
	if err := m.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = m.Close(ctx) })

	// Seed only the backend directly so the cache starts empty.
	if _, err := backend.Set(ctx, "smoke", "k", config.NewValue("v")); err != nil {
		t.Fatalf("backend.Set: %v", err)
	}
	if _, err := cache.Get(ctx, "smoke", "k"); !config.IsNotFound(err) {
		t.Fatalf("cache pre-Get returned %v, want ErrNotFound (cache should start empty)", err)
	}

	// Read through the multi-store; this should back-fill the cache.
	got, err := m.Get(ctx, "smoke", "k")
	if err != nil {
		t.Fatalf("multi.Get: %v", err)
	}
	if v, _ := got.String(); v != "v" {
		t.Fatalf("multi.Get returned %q, want %q", v, "v")
	}

	// The cache store must now hold the key.
	cached, err := cache.Get(ctx, "smoke", "k")
	if err != nil {
		t.Fatalf("cache.Get after read-through: %v (cache not populated)", err)
	}
	if v, _ := cached.String(); v != "v" {
		t.Fatalf("cache holds %q, want %q", v, "v")
	}
}

// TestSmoke_SQLiteNamespaceLister pins the SQLite NamespaceLister: two seeded
// namespaces must come back byte-sorted with an empty next cursor. Exercises
// the SQL DISTINCT/ORDER BY path that the memory lister cannot cover.
func TestSmoke_SQLiteNamespaceLister(t *testing.T) {
	t.Parallel()
	ctx := smokeCtx(t)

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })

	store := sqlite.NewStore(db)
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(ctx) })

	for _, ns := range []string{"beta", "alpha"} {
		if _, err := store.Set(ctx, ns, "k", config.NewValue(1)); err != nil {
			t.Fatalf("Set %q: %v", ns, err)
		}
	}

	nl, ok := any(store).(config.NamespaceLister)
	if !ok {
		t.Fatal("sqlite store does not implement NamespaceLister")
	}
	names, cursor, err := nl.ListNamespaces(ctx, "", 10, "")
	if err != nil {
		t.Fatalf("ListNamespaces: %v", err)
	}
	if cursor != "" {
		t.Errorf("nextCursor = %q, want empty (only 2 namespaces, limit 10)", cursor)
	}
	want := []string{"alpha", "beta"}
	if len(names) != len(want) {
		t.Fatalf("got %d names, want %d: %v", len(names), len(want), names)
	}
	for i, n := range want {
		if names[i] != n {
			t.Errorf("names[%d] = %q, want %q (byte-wise sort broken)", i, names[i], n)
		}
	}
}
