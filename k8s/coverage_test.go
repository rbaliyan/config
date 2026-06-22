package k8s

import (
	"context"
	"errors"
	"testing"

	"github.com/rbaliyan/config"
)

// errInjectClient wraps a fakeClient and forces selected operations to fail,
// so the Store's backend-error branches are covered deterministically (no
// dependence on watch goroutine timing).
type errInjectClient struct {
	*fakeClient
	getErr    error
	upsertErr error
	watchErr  error
}

func (c *errInjectClient) Get(ctx context.Context, kind Kind, ns, name string) (*Resource, error) {
	if c.getErr != nil {
		return nil, c.getErr
	}
	return c.fakeClient.Get(ctx, kind, ns, name)
}

func (c *errInjectClient) Upsert(ctx context.Context, kind Kind, ns string, r *Resource) (*Resource, error) {
	if c.upsertErr != nil {
		return nil, c.upsertErr
	}
	return c.fakeClient.Upsert(ctx, kind, ns, r)
}

func (c *errInjectClient) Watch(ctx context.Context, ns string) (<-chan Event, error) {
	if c.watchErr != nil {
		return nil, c.watchErr
	}
	return c.fakeClient.Watch(ctx, ns)
}

func TestStore_Connect_WatchError(t *testing.T) {
	s := NewStore(&errInjectClient{fakeClient: newFakeClient(), watchErr: errors.New("watch boom")})
	if err := s.Connect(context.Background()); err == nil {
		t.Fatal("expected Connect to fail when the client watch fails")
	}
}

func TestStore_Set_UpsertError(t *testing.T) {
	ctx := context.Background()
	s := NewStore(&errInjectClient{fakeClient: newFakeClient(), upsertErr: errors.New("upsert boom")})
	mustConnect(t, s)
	if _, err := s.Set(ctx, "prod", "k", config.NewValue("v")); err == nil {
		t.Error("expected Set to surface the upsert error")
	}
}

func TestStore_Get_BackendError(t *testing.T) {
	ctx := context.Background()
	s := NewStore(&errInjectClient{fakeClient: newFakeClient(), getErr: errors.New("get boom")})
	mustConnect(t, s)
	if _, err := s.Get(ctx, "prod", "k"); err == nil {
		t.Error("expected Get to surface a non-NotFound backend error")
	}
}

func TestStore_BackendName(t *testing.T) {
	if got := NewStore(newFakeClient()).BackendName(); got != "k8s" {
		t.Errorf("BackendName() = %q, want k8s", got)
	}
}

func TestStore_Health_Unhealthy(t *testing.T) {
	ctx := context.Background()
	fc := newFakeClient()
	fc.healthOK = false
	s := NewStore(fc)
	mustConnect(t, s)
	if err := s.Health(ctx); err == nil {
		t.Error("expected Health to report the unhealthy backend")
	}
}

// TestStore_Set_UpdateAndMerge exercises the update path: multiple keys live in
// one ConfigMap, and updating one key must preserve the others (mergeResource).
func TestStore_Set_UpdateAndMerge(t *testing.T) {
	ctx := context.Background()
	fc := newFakeClient()
	s := NewStore(fc)
	mustConnect(t, s)

	if _, err := s.Set(ctx, "prod", "db/host", config.NewValue("h1")); err != nil {
		t.Fatalf("Set host: %v", err)
	}
	if _, err := s.Set(ctx, "prod", "db/port", config.NewValue(5432)); err != nil {
		t.Fatalf("Set port: %v", err)
	}
	// Update the first key; the second must survive the merge.
	if _, err := s.Set(ctx, "prod", "db/host", config.NewValue("h2")); err != nil {
		t.Fatalf("Set host update: %v", err)
	}

	got, err := s.Get(ctx, "prod", "db/host")
	if err != nil {
		t.Fatalf("Get host: %v", err)
	}
	if hs, _ := got.String(); hs != "h2" {
		t.Errorf("host = %q, want h2", hs)
	}
	if _, err := s.Get(ctx, "prod", "db/port"); err != nil {
		t.Errorf("port lost after updating sibling key: %v", err)
	}
}

// TestStore_Secret_RoundTrip covers the Secret read path (decode from a Secret
// resource through makeValueFromBytes).
func TestStore_Secret_RoundTrip(t *testing.T) {
	ctx := context.Background()
	fc := newFakeClient()
	s := NewStore(fc)
	mustConnect(t, s)

	if _, err := s.Set(ctx, "prod", "secret/token", config.NewValue("s3cr3t")); err != nil {
		t.Fatalf("Set secret: %v", err)
	}
	got, err := s.Get(ctx, "prod", "secret/token")
	if err != nil {
		t.Fatalf("Get secret: %v", err)
	}
	if sv, _ := got.String(); sv != "s3cr3t" {
		t.Errorf("secret value = %q, want s3cr3t", sv)
	}
}
