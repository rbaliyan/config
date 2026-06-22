package transform

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/memory"
)

var seedTime = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

// versionedStore is a minimal config.Store + VersionedStore double whose
// GetVersions returns transformed bytes, so the wrapper must reverse them.
type versionedStore struct {
	config.Store
	versions []config.Value
	backend  string
}

func (v *versionedStore) BackendName() string { return v.backend }

func (v *versionedStore) GetVersions(_ context.Context, _, _ string, _ config.VersionFilter) (config.VersionPage, error) {
	return config.NewVersionPage(v.versions, "next-cursor", 10), nil
}

// errStore is a config.Store + BulkStore double that fails bulk ops with a
// configurable error to exercise partial-failure propagation.
type errStore struct {
	getManyErr    error
	setManyErr    error
	deleteManyErr error
}

func (e *errStore) Connect(context.Context) error { return nil }
func (e *errStore) Close(context.Context) error   { return nil }
func (e *errStore) Get(context.Context, string, string) (config.Value, error) {
	return nil, config.ErrNotFound
}
func (e *errStore) Set(_ context.Context, _, _ string, v config.Value) (config.Value, error) {
	return v, nil
}
func (e *errStore) Delete(context.Context, string, string) error { return config.ErrNotFound }
func (e *errStore) Find(context.Context, string, config.Filter) (config.Page, error) {
	return config.NewPage(nil, "", 0), nil
}
func (e *errStore) Watch(context.Context, config.WatchFilter) (<-chan config.ChangeEvent, error) {
	return nil, config.ErrWatchNotSupported
}
func (e *errStore) GetMany(context.Context, string, []string) (map[string]config.Value, error) {
	if e.getManyErr != nil {
		return nil, e.getManyErr
	}
	return map[string]config.Value{}, nil
}
func (e *errStore) SetMany(context.Context, string, map[string]config.Value) error {
	return e.setManyErr
}
func (e *errStore) DeleteMany(context.Context, string, []string) (int64, error) {
	if e.deleteManyErr != nil {
		return 0, e.deleteManyErr
	}
	return 0, nil
}

// plainBulkless wraps a memory.Store but hides BulkStore so the wrapper takes
// its per-key fallback paths in GetMany/SetMany/DeleteMany.
type plainBulkless struct {
	get    func(ctx context.Context, ns, key string) (config.Value, error)
	set    func(ctx context.Context, ns, key string, v config.Value) (config.Value, error)
	delete func(ctx context.Context, ns, key string) error
	mem    *memory.Store
}

func newPlainBulkless(t *testing.T) *plainBulkless {
	t.Helper()
	m := memory.NewStore()
	if err := m.Connect(context.Background()); err != nil {
		t.Fatalf("connect mem: %v", err)
	}
	t.Cleanup(func() { _ = m.Close(context.Background()) })
	return &plainBulkless{mem: m}
}

func (p *plainBulkless) Connect(ctx context.Context) error { return p.mem.Connect(ctx) }
func (p *plainBulkless) Close(ctx context.Context) error   { return p.mem.Close(ctx) }
func (p *plainBulkless) Get(ctx context.Context, ns, key string) (config.Value, error) {
	if p.get != nil {
		return p.get(ctx, ns, key)
	}
	return p.mem.Get(ctx, ns, key)
}
func (p *plainBulkless) Set(ctx context.Context, ns, key string, v config.Value) (config.Value, error) {
	if p.set != nil {
		return p.set(ctx, ns, key, v)
	}
	return p.mem.Set(ctx, ns, key, v)
}
func (p *plainBulkless) Delete(ctx context.Context, ns, key string) error {
	if p.delete != nil {
		return p.delete(ctx, ns, key)
	}
	return p.mem.Delete(ctx, ns, key)
}
func (p *plainBulkless) Find(ctx context.Context, ns string, f config.Filter) (config.Page, error) {
	return p.mem.Find(ctx, ns, f)
}
func (p *plainBulkless) Watch(ctx context.Context, f config.WatchFilter) (<-chan config.ChangeEvent, error) {
	return p.mem.Watch(ctx, f)
}

func TestBulkFallback_RoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	inner := newPlainBulkless(t)
	s, err := WrapStore(inner, &xorTransformer{key: 0x42})
	if err != nil {
		t.Fatalf("WrapStore: %v", err)
	}
	bulk := s.(config.BulkStore)

	// SetMany fallback (per-key Set).
	if err := bulk.SetMany(ctx, "ns", map[string]config.Value{
		"k1": config.NewValue("one"),
		"k2": config.NewValue("two"),
	}); err != nil {
		t.Fatalf("SetMany fallback: %v", err)
	}

	// GetMany fallback (per-key Get) reverses correctly.
	got, err := bulk.GetMany(ctx, "ns", []string{"k1", "k2", "missing"})
	if err != nil {
		t.Fatalf("GetMany fallback: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("GetMany fallback returned %d, want 2 (missing key omitted)", len(got))
	}
	if str, _ := got["k1"].String(); str != "one" {
		t.Errorf("k1 = %q, want one", str)
	}

	// DeleteMany fallback counts only existing keys.
	deleted, err := bulk.DeleteMany(ctx, "ns", []string{"k1", "missing"})
	if err != nil {
		t.Fatalf("DeleteMany fallback: %v", err)
	}
	if deleted != 1 {
		t.Errorf("DeleteMany fallback deleted = %d, want 1", deleted)
	}
}

func TestBulkFallback_SetManyError(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("set boom")
	inner := newPlainBulkless(t)
	inner.set = func(context.Context, string, string, config.Value) (config.Value, error) {
		return nil, wantErr
	}
	s, _ := WrapStore(inner, &xorTransformer{key: 0x42})
	err := s.(config.BulkStore).SetMany(context.Background(), "ns",
		map[string]config.Value{"k1": config.NewValue("x")})
	if !errors.Is(err, wantErr) {
		t.Errorf("SetMany fallback error = %v, want %v", err, wantErr)
	}
}

func TestBulkFallback_GetManyNonNotFoundError(t *testing.T) {
	t.Parallel()
	wantErr := config.ErrStoreClosed
	inner := newPlainBulkless(t)
	inner.get = func(context.Context, string, string) (config.Value, error) {
		return nil, wantErr
	}
	s, _ := WrapStore(inner, &xorTransformer{key: 0x42})
	_, err := s.(config.BulkStore).GetMany(context.Background(), "ns", []string{"k1"})
	if !errors.Is(err, wantErr) {
		t.Errorf("GetMany fallback error = %v, want %v", err, wantErr)
	}
}

func TestBulkFallback_DeleteManyNonNotFoundError(t *testing.T) {
	t.Parallel()
	wantErr := config.ErrStoreClosed
	inner := newPlainBulkless(t)
	inner.delete = func(context.Context, string, string) error { return wantErr }
	s, _ := WrapStore(inner, &xorTransformer{key: 0x42})
	_, err := s.(config.BulkStore).DeleteMany(context.Background(), "ns", []string{"k1"})
	if !errors.Is(err, wantErr) {
		t.Errorf("DeleteMany fallback error = %v, want %v", err, wantErr)
	}
}

func TestGetVersions_ReversesEachVersion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tr := &xorTransformer{key: 0x42}

	// Build two transformed (XOR'd) version values so the wrapper has to
	// reverse them back to plaintext. The stored value is a raw pass-through
	// whose Marshal yields the ciphertext (mirroring what a real backend holds
	// after transform-on-Set); reverseValue will XOR it back and JSON-decode.
	mk := func(plain string) config.Value {
		enc, err := tr.Transform(ctx, []byte(`"`+plain+`"`))
		if err != nil {
			t.Fatalf("transform seed: %v", err)
		}
		return config.NewRawValue(enc, "json",
			config.WithValueMetadata(2, seedTime, seedTime))
	}

	inner := &versionedStore{
		Store:    memory.NewStore(),
		versions: []config.Value{mk("v2"), mk("v1")},
	}
	s, err := WrapStore(inner, tr)
	if err != nil {
		t.Fatalf("WrapStore: %v", err)
	}

	page, err := s.(config.VersionedStore).GetVersions(ctx, "ns", "k", config.NewVersionFilter().Build())
	if err != nil {
		t.Fatalf("GetVersions: %v", err)
	}
	got := page.Versions()
	if len(got) != 2 {
		t.Fatalf("got %d versions, want 2", len(got))
	}
	if str, _ := got[0].String(); str != "v2" {
		t.Errorf("version[0] = %q, want v2", str)
	}
	if str, _ := got[1].String(); str != "v1" {
		t.Errorf("version[1] = %q, want v1", str)
	}
	if page.NextCursor() != "next-cursor" {
		t.Errorf("NextCursor = %q, want next-cursor", page.NextCursor())
	}
}

func TestGetVersions_NotSupported(t *testing.T) {
	t.Parallel()
	// errStore does not implement VersionedStore.
	s, err := WrapStore(&errStore{}, &xorTransformer{key: 0x42})
	if err != nil {
		t.Fatalf("WrapStore: %v", err)
	}
	_, err = s.(config.VersionedStore).GetVersions(context.Background(), "ns", "k", config.NewVersionFilter().Build())
	if !errors.Is(err, config.ErrVersioningNotSupported) {
		t.Errorf("want ErrVersioningNotSupported, got %v", err)
	}
}

func TestBackendName_DelegatesAndFallsBack(t *testing.T) {
	t.Parallel()
	// Inner store exposes a backend name → wrapper reports it.
	withName := &versionedStore{Store: memory.NewStore(), backend: "custom-backend"}
	s, err := WrapStore(withName, &xorTransformer{key: 0x42})
	if err != nil {
		t.Fatalf("WrapStore: %v", err)
	}
	if got := s.(*transformStore).BackendName(); got != "custom-backend" {
		t.Errorf("BackendName = %q, want custom-backend", got)
	}

	// errStore has no BackendName method → wrapper returns "".
	s2, err := WrapStore(&errStore{}, &xorTransformer{key: 0x42})
	if err != nil {
		t.Fatalf("WrapStore: %v", err)
	}
	if got := s2.(*transformStore).BackendName(); got != "" {
		t.Errorf("BackendName = %q, want empty", got)
	}
}

func TestSetMany_SurfacesBulkFailure(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("bulk set boom")
	s, err := WrapStore(&errStore{setManyErr: wantErr}, &xorTransformer{key: 0x42})
	if err != nil {
		t.Fatalf("WrapStore: %v", err)
	}
	err = s.(config.BulkStore).SetMany(context.Background(), "ns", map[string]config.Value{
		"k1": config.NewValue("a"),
	})
	if !errors.Is(err, wantErr) {
		t.Errorf("SetMany error = %v, want wrap of %v", err, wantErr)
	}
}

func TestGetMany_SurfacesBulkFailure(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("bulk get boom")
	s, err := WrapStore(&errStore{getManyErr: wantErr}, &xorTransformer{key: 0x42})
	if err != nil {
		t.Fatalf("WrapStore: %v", err)
	}
	_, err = s.(config.BulkStore).GetMany(context.Background(), "ns", []string{"k1"})
	if !errors.Is(err, wantErr) {
		t.Errorf("GetMany error = %v, want %v", err, wantErr)
	}
}

func TestDeleteMany_SurfacesBulkFailure(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("bulk delete boom")
	s, err := WrapStore(&errStore{deleteManyErr: wantErr}, &xorTransformer{key: 0x42})
	if err != nil {
		t.Fatalf("WrapStore: %v", err)
	}
	_, err = s.(config.BulkStore).DeleteMany(context.Background(), "ns", []string{"k1"})
	if !errors.Is(err, wantErr) {
		t.Errorf("DeleteMany error = %v, want %v", err, wantErr)
	}
}

func TestGetMany_RoundTripReverses(t *testing.T) {
	t.Parallel()
	ts, _ := newTestStore(t)
	ctx := context.Background()
	if err := ts.SetMany(ctx, "ns", map[string]config.Value{
		"k1": config.NewValue("alpha"),
		"k2": config.NewValue("beta"),
	}); err != nil {
		t.Fatalf("SetMany: %v", err)
	}
	got, err := ts.GetMany(ctx, "ns", []string{"k1", "k2"})
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	for k, want := range map[string]string{"k1": "alpha", "k2": "beta"} {
		s, _ := got[k].String()
		if s != want {
			t.Errorf("GetMany[%s] = %q, want %q (reverse failed)", k, s, want)
		}
	}
}
