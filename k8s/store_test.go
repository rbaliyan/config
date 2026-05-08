package k8s

import (
	"context"
	"errors"
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/rbaliyan/config"
	_ "github.com/rbaliyan/config/codec/json"
)

// fakeClient is an in-memory Client used by tests. It mimics enough of
// kubernetes' ConfigMap/Secret semantics to exercise Store end-to-end.
type fakeClient struct {
	mu       sync.Mutex
	cms      map[string]map[string]*Resource // namespace -> name -> resource
	secrets  map[string]map[string]*Resource
	rv       int
	events   chan Event
	healthOK bool
}

func newFakeClient() *fakeClient {
	return &fakeClient{
		cms:      map[string]map[string]*Resource{},
		secrets:  map[string]map[string]*Resource{},
		events:   make(chan Event, 32),
		healthOK: true,
	}
}

func (f *fakeClient) bucket(kind Kind) map[string]map[string]*Resource {
	if kind == KindSecret {
		return f.secrets
	}
	return f.cms
}

func (f *fakeClient) Get(_ context.Context, kind Kind, namespace, name string) (*Resource, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ns := f.bucket(kind)[namespace]
	if ns == nil {
		return nil, ErrNotFound
	}
	r := ns[name]
	if r == nil {
		return nil, ErrNotFound
	}
	return cloneResource(r), nil
}

func (f *fakeClient) Upsert(_ context.Context, kind Kind, namespace string, r *Resource) (*Resource, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	bucket := f.bucket(kind)
	if bucket[namespace] == nil {
		bucket[namespace] = map[string]*Resource{}
	}
	old := bucket[namespace][r.Name]
	f.rv++
	stored := cloneResource(r)
	stored.ResourceVersion = itoa(f.rv)
	bucket[namespace][r.Name] = stored

	ev := Event{Kind: kind, Namespace: namespace, New: cloneResource(stored)}
	if old == nil {
		ev.Type = EventAdd
	} else {
		ev.Type = EventUpdate
		ev.Old = cloneResource(old)
	}
	select {
	case f.events <- ev:
	default:
	}
	return cloneResource(stored), nil
}

func (f *fakeClient) Watch(ctx context.Context, _ string) (<-chan Event, error) {
	out := make(chan Event, 32)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-f.events:
				if !ok {
					return
				}
				select {
				case out <- ev:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}

func (f *fakeClient) Health(_ context.Context) error {
	if !f.healthOK {
		return errors.New("unhealthy")
	}
	return nil
}

func cloneResource(r *Resource) *Resource {
	if r == nil {
		return nil
	}
	out := &Resource{
		Name:            r.Name,
		ResourceVersion: r.ResourceVersion,
	}
	if r.Annotations != nil {
		out.Annotations = make(map[string]string, len(r.Annotations))
		maps.Copy(out.Annotations, r.Annotations)
	}
	if r.Data != nil {
		out.Data = make(map[string][]byte, len(r.Data))
		for k, v := range r.Data {
			b := make([]byte, len(v))
			copy(b, v)
			out.Data[k] = b
		}
	}
	return out
}

func itoa(n int) string {
	const digits = "0123456789"
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = digits[n%10]
		n /= 10
	}
	return string(buf[i:])
}

func mustConnect(t *testing.T, s *Store) {
	t.Helper()
	if err := s.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
}

func TestStore_SetGetDelete_ConfigMap(t *testing.T) {
	ctx := context.Background()
	fc := newFakeClient()
	s := NewStore(fc)
	mustConnect(t, s)

	val := config.NewValue(map[string]any{"host": "localhost", "port": 5432})
	stored, err := s.Set(ctx, "prod", "db/conn", val)
	if err != nil {
		t.Fatalf("Set: %v", err)
	}
	if stored.Metadata().Version() == 0 {
		t.Errorf("expected non-zero version, got 0")
	}

	got, err := s.Get(ctx, "prod", "db/conn")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	var out map[string]any
	if err := got.Unmarshal(ctx, &out); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if out["host"] != "localhost" {
		t.Errorf("expected localhost, got %v", out["host"])
	}

	if err := s.Delete(ctx, "prod", "db/conn"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.Get(ctx, "prod", "db/conn"); !config.IsNotFound(err) {
		t.Errorf("expected not found, got %v", err)
	}
}

func TestStore_SecretRouting(t *testing.T) {
	ctx := context.Background()
	fc := newFakeClient()
	s := NewStore(fc)
	mustConnect(t, s)

	val := config.NewValue("super-secret")
	if _, err := s.Set(ctx, "prod", "secret/api-key", val); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, ok := fc.secrets["prod"]["config-secrets-prod"]; !ok {
		t.Errorf("expected secret resource to exist; secrets=%+v", fc.secrets)
	}
	if _, ok := fc.cms["prod"]["config-prod"]; ok {
		t.Errorf("did not expect configmap for secret-prefixed key")
	}
}

func TestStore_GetMissing(t *testing.T) {
	ctx := context.Background()
	fc := newFakeClient()
	s := NewStore(fc)
	mustConnect(t, s)

	if _, err := s.Get(ctx, "prod", "missing/key"); !config.IsNotFound(err) {
		t.Errorf("expected not-found, got %v", err)
	}
}

func TestStore_Find_PrefixAndCursor(t *testing.T) {
	ctx := context.Background()
	fc := newFakeClient()
	s := NewStore(fc)
	mustConnect(t, s)

	for _, k := range []string{"app/a", "app/b", "app/c", "other/x"} {
		if _, err := s.Set(ctx, "prod", k, config.NewValue(k)); err != nil {
			t.Fatalf("Set %s: %v", k, err)
		}
	}

	page, err := s.Find(ctx, "prod", config.NewFilter().WithPrefix("app/").WithLimit(2).Build())
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if len(page.Results()) != 2 {
		t.Errorf("expected 2 results, got %d", len(page.Results()))
	}
	if page.NextCursor() != "app/b" {
		t.Errorf("expected cursor app/b, got %q", page.NextCursor())
	}

	next, err := s.Find(ctx, "prod", config.NewFilter().WithPrefix("app/").WithLimit(2).WithCursor(page.NextCursor()).Build())
	if err != nil {
		t.Fatalf("Find page2: %v", err)
	}
	if _, ok := next.Results()["app/c"]; !ok {
		t.Errorf("expected app/c in second page; got %+v", next.Results())
	}
}

func TestStore_Watch_DeliversChanges(t *testing.T) {
	ctx := t.Context()

	fc := newFakeClient()
	s := NewStore(fc)
	mustConnect(t, s)

	ch, err := s.Watch(ctx, config.WatchFilter{Namespaces: []string{"prod"}})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	if _, err := s.Set(ctx, "prod", "feature/flag", config.NewValue(true)); err != nil {
		t.Fatalf("Set: %v", err)
	}

	select {
	case ev := <-ch:
		if ev.Type != config.ChangeTypeSet || ev.Key != "feature/flag" || ev.Namespace != "prod" {
			t.Errorf("unexpected event: %+v", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for watch event")
	}
}

func TestStore_Health(t *testing.T) {
	fc := newFakeClient()
	s := NewStore(fc)
	mustConnect(t, s)
	if err := s.Health(context.Background()); err != nil {
		t.Errorf("Health: %v", err)
	}
	fc.healthOK = false
	if err := s.Health(context.Background()); err == nil {
		t.Errorf("expected unhealthy error")
	}
}

func TestStore_NotConnected(t *testing.T) {
	s := NewStore(newFakeClient())
	if _, err := s.Get(context.Background(), "prod", "k"); !errors.Is(err, config.ErrStoreNotConnected) {
		t.Errorf("expected ErrStoreNotConnected, got %v", err)
	}
}

func TestStore_AfterClose(t *testing.T) {
	s := NewStore(newFakeClient())
	mustConnect(t, s)
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := s.Get(context.Background(), "prod", "k"); !errors.Is(err, config.ErrStoreClosed) {
		t.Errorf("expected ErrStoreClosed, got %v", err)
	}
}
