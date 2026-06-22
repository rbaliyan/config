package replica

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/memory"
)

func connectedMemory(t *testing.T) *memory.Store {
	t.Helper()
	m := memory.NewStore()
	if err := m.Connect(context.Background()); err != nil {
		t.Fatalf("connect memory: %v", err)
	}
	t.Cleanup(func() { _ = m.Close(context.Background()) })
	return m
}

func TestClassifyError(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"not_found_sentinel", config.ErrNotFound, "not_found"},
		{"not_found_typed", &config.KeyNotFoundError{Namespace: "ns", Key: "k"}, "not_found"},
		{"type_mismatch", config.ErrTypeMismatch, "type_mismatch"},
		{"store_closed", config.ErrStoreClosed, "unavailable"},
		{"store_not_connected", config.ErrStoreNotConnected, "unavailable"},
		{"wrapped_not_found", config.WrapStoreError("get", "x", "k", config.ErrNotFound), "not_found"},
		{"net_error", &net.OpError{Op: "dial", Err: errors.New("refused")}, "internal"},
		{"context_cancelled", context.Canceled, "internal"},
		{"unknown", errors.New("boom"), "internal"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := classifyError(tc.err); got != tc.want {
				t.Errorf("classifyError(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}

func TestIsRetriable(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"not_found", config.ErrNotFound, true},
		{"store_closed", config.ErrStoreClosed, true},
		{"store_not_connected", config.ErrStoreNotConnected, true},
		{"type_mismatch", config.ErrTypeMismatch, false},
		{"unknown", errors.New("boom"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isRetriable(tc.err); got != tc.want {
				t.Errorf("isRetriable(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestAlias_DelegatesToPrimary(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	primary := connectedMemory(t)
	secondary := connectedMemory(t)

	// Seed a target key so SetAlias has something to point at.
	if _, err := primary.Set(ctx, config.DefaultNamespace, "target", config.NewValue("v")); err != nil {
		t.Fatalf("seed target: %v", err)
	}

	s := NewStore(primary, []config.Store{secondary}, WithReplicationMode(ModeSync))

	// SetAlias delegates to the primary.
	if _, err := s.SetAlias(ctx, "myalias", "target"); err != nil {
		t.Fatalf("SetAlias: %v", err)
	}

	// GetAlias reads back from the primary.
	got, err := s.GetAlias(ctx, "myalias")
	if err != nil {
		t.Fatalf("GetAlias: %v", err)
	}
	if str, _ := got.String(); str != "target" {
		t.Errorf("GetAlias target = %q, want target", str)
	}

	// The primary itself must now know the alias.
	if _, err := primary.GetAlias(ctx, "myalias"); err != nil {
		t.Errorf("primary missing alias after delegation: %v", err)
	}

	// ListAliases includes it.
	all, err := s.ListAliases(ctx)
	if err != nil {
		t.Fatalf("ListAliases: %v", err)
	}
	if _, ok := all["myalias"]; !ok {
		t.Errorf("ListAliases missing myalias, got keys: %v", all)
	}

	// In ModeSync, the alias should also be propagated to the secondary.
	if _, err := secondary.GetAlias(ctx, "myalias"); err != nil {
		t.Errorf("secondary missing propagated alias: %v", err)
	}

	// DeleteAlias delegates and removes it.
	if err := s.DeleteAlias(ctx, "myalias"); err != nil {
		t.Fatalf("DeleteAlias: %v", err)
	}
	if _, err := s.GetAlias(ctx, "myalias"); !config.IsNotFound(err) {
		t.Errorf("GetAlias after delete: want NotFound, got %v", err)
	}
}

func TestAlias_NotSupportedByPrimary(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	// plainStore (defined in store_test.go) does not implement AliasStore.
	s := NewStore(&plainStore{}, nil)

	if _, err := s.SetAlias(ctx, "a", "b"); !errors.Is(err, errAliasingNotSupported) {
		t.Errorf("SetAlias: want errAliasingNotSupported, got %v", err)
	}
	if err := s.DeleteAlias(ctx, "a"); !errors.Is(err, errAliasingNotSupported) {
		t.Errorf("DeleteAlias: want errAliasingNotSupported, got %v", err)
	}
	if _, err := s.GetAlias(ctx, "a"); !errors.Is(err, errAliasingNotSupported) {
		t.Errorf("GetAlias: want errAliasingNotSupported, got %v", err)
	}
	if _, err := s.ListAliases(ctx); !errors.Is(err, errAliasingNotSupported) {
		t.Errorf("ListAliases: want errAliasingNotSupported, got %v", err)
	}
}

func TestBulkGet_FallbackOnNonBulkStore(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	// nonBulkStore wraps memory but hides BulkStore, forcing the per-key path.
	prim := &nonBulkStore{mem: connectedMemory(t)}
	if _, err := prim.mem.Set(ctx, "ns", "k1", config.NewValue("v1")); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := prim.mem.Set(ctx, "ns", "k2", config.NewValue("v2")); err != nil {
		t.Fatalf("seed: %v", err)
	}

	got, err := bulkGet(ctx, prim, "ns", []string{"k1", "k2", "missing"})
	if err != nil {
		t.Fatalf("bulkGet: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("bulkGet returned %d, want 2 (missing key omitted)", len(got))
	}
}

func TestBulkGet_FallbackPropagatesNonNotFoundError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	prim := &nonBulkStore{mem: connectedMemory(t), getErr: config.ErrStoreClosed}
	_, err := bulkGet(ctx, prim, "ns", []string{"k1"})
	if !errors.Is(err, config.ErrStoreClosed) {
		t.Errorf("bulkGet error = %v, want ErrStoreClosed", err)
	}
}

func TestCountReplicationError_NoMetricsIsNoop(t *testing.T) {
	t.Parallel()
	// With metrics disabled (default), countReplicationError must be a safe no-op.
	s := NewStore(connectedMemory(t), nil)
	if s.metrics != nil {
		t.Fatal("expected metrics to be nil by default")
	}
	s.countReplicationError(context.Background(), "set") // must not panic
}

func TestCountReplicationError_PropagatesSecondaryFailure(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	primary := connectedMemory(t)
	// failOnSet secondary errors on every Set; ModeSync propagation should
	// route through countReplicationError without failing the caller.
	failing := &failOnWrite{setErr: errors.New("secondary down")}

	s := NewStore(primary, []config.Store{failing}, WithReplicationMode(ModeSync))
	if err := s.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer s.Close(ctx)

	// Caller-visible Set must still succeed even though the secondary fails.
	if _, err := s.Set(ctx, "ns", "k", config.NewValue("v")); err != nil {
		t.Errorf("Set should succeed despite secondary failure, got %v", err)
	}
	// SetMany propagation failure is likewise tolerated.
	if err := s.SetMany(ctx, "ns", map[string]config.Value{"k2": config.NewValue("v2")}); err != nil {
		t.Errorf("SetMany should succeed despite secondary failure, got %v", err)
	}
}

// nonBulkStore exposes only the base config.Store surface (no BulkStore), so
// helpers fall back to per-key operations.
type nonBulkStore struct {
	mem    *memory.Store
	getErr error
}

func (n *nonBulkStore) Connect(ctx context.Context) error { return n.mem.Connect(ctx) }
func (n *nonBulkStore) Close(ctx context.Context) error   { return n.mem.Close(ctx) }
func (n *nonBulkStore) Get(ctx context.Context, ns, key string) (config.Value, error) {
	if n.getErr != nil {
		return nil, n.getErr
	}
	return n.mem.Get(ctx, ns, key)
}
func (n *nonBulkStore) Set(ctx context.Context, ns, key string, v config.Value) (config.Value, error) {
	return n.mem.Set(ctx, ns, key, v)
}
func (n *nonBulkStore) Delete(ctx context.Context, ns, key string) error {
	return n.mem.Delete(ctx, ns, key)
}
func (n *nonBulkStore) Find(ctx context.Context, ns string, f config.Filter) (config.Page, error) {
	return n.mem.Find(ctx, ns, f)
}
func (n *nonBulkStore) Watch(ctx context.Context, f config.WatchFilter) (<-chan config.ChangeEvent, error) {
	return n.mem.Watch(ctx, f)
}

// failOnWrite is a minimal store whose writes always fail; reads report not found.
type failOnWrite struct {
	setErr error
}

func (f *failOnWrite) Connect(context.Context) error { return nil }
func (f *failOnWrite) Close(context.Context) error   { return nil }
func (f *failOnWrite) Get(context.Context, string, string) (config.Value, error) {
	return nil, config.ErrNotFound
}
func (f *failOnWrite) Set(context.Context, string, string, config.Value) (config.Value, error) {
	return nil, f.setErr
}
func (f *failOnWrite) Delete(context.Context, string, string) error { return f.setErr }
func (f *failOnWrite) Find(context.Context, string, config.Filter) (config.Page, error) {
	return config.NewPage(nil, "", 0), nil
}
func (f *failOnWrite) Watch(context.Context, config.WatchFilter) (<-chan config.ChangeEvent, error) {
	return nil, config.ErrWatchNotSupported
}
