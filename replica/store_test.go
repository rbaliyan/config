package replica

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/memory"
)

func TestNewStore(t *testing.T) {
	primary := memory.NewStore()
	secondary := memory.NewStore()

	s := NewStore(primary, []config.Store{secondary})
	if s == nil {
		t.Fatal("NewStore returned nil")
	}
	if s.primary != primary {
		t.Error("Expected primary store to be set")
	}
	if len(s.secondaries) != 1 {
		t.Errorf("Expected 1 secondary, got %d", len(s.secondaries))
	}
	if s.opts.mode != ModeAsync {
		t.Error("Expected default mode ModeAsync")
	}
	if s.opts.readPref != ReadPrimary {
		t.Error("Expected default ReadPrimary")
	}
}

func TestStore_Connect_Close(t *testing.T) {
	primary := memory.NewStore()
	secondary := memory.NewStore()

	s := NewStore(primary, []config.Store{secondary})
	ctx := context.Background()

	if err := s.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	if err := s.Close(ctx); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestStore_Connect_PrimaryFails(t *testing.T) {
	primary := &failStore{connectErr: errors.New("primary down")}
	secondary := memory.NewStore()

	s := NewStore(primary, []config.Store{secondary})
	ctx := context.Background()

	if err := s.Connect(ctx); err == nil {
		t.Fatal("Expected error when primary fails to connect")
	}
}

func TestStore_Connect_AllSecondariesFail(t *testing.T) {
	primary := memory.NewStore()
	f1 := &failStore{connectErr: errors.New("sec1 down")}
	f2 := &failStore{connectErr: errors.New("sec2 down")}

	s := NewStore(primary, []config.Store{f1, f2})
	ctx := context.Background()

	if err := s.Connect(ctx); err == nil {
		t.Fatal("Expected error when all secondaries fail to connect")
	}
}

func TestStore_Connect_OneSecondaryFails(t *testing.T) {
	primary := memory.NewStore()
	f1 := &failStore{connectErr: errors.New("sec1 down")}
	sec2 := memory.NewStore()

	s := NewStore(primary, []config.Store{f1, sec2})
	ctx := context.Background()

	if err := s.Connect(ctx); err != nil {
		t.Errorf("Connect should succeed when at least one secondary connects, got %v", err)
	}
	_ = s.Close(ctx)
}

func TestStore_Set_Get_ReadPrimary(t *testing.T) {
	primary := memory.NewStore()
	secondary := memory.NewStore()

	s := NewStore(primary, []config.Store{secondary}, WithReplicationMode(ModeSync))
	ctx := context.Background()
	_ = s.Connect(ctx)
	defer s.Close(ctx)

	val := config.NewValue(42)
	if _, err := s.Set(ctx, "ns", "key", val); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	got, err := s.Get(ctx, "ns", "key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	i, _ := got.Int64()
	if i != 42 {
		t.Errorf("Expected 42, got %d", i)
	}
}

func TestStore_Set_ModeSync_PropagatestoSecondary(t *testing.T) {
	primary := memory.NewStore()
	secondary := memory.NewStore()

	s := NewStore(primary, []config.Store{secondary}, WithReplicationMode(ModeSync))
	ctx := context.Background()
	_ = s.Connect(ctx)
	defer s.Close(ctx)

	_, err := s.Set(ctx, "ns", "key", config.NewValue("hello"))
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Secondary should have the value synchronously
	got, err := secondary.Get(ctx, "ns", "key")
	if err != nil {
		t.Fatalf("Secondary should have the value: %v", err)
	}
	str, _ := got.String()
	if str != "hello" {
		t.Errorf("Expected 'hello', got %s", str)
	}
}

func TestStore_Delete_ModeSync_PropagatestoSecondary(t *testing.T) {
	primary := memory.NewStore()
	secondary := memory.NewStore()

	s := NewStore(primary, []config.Store{secondary}, WithReplicationMode(ModeSync))
	ctx := context.Background()
	_ = s.Connect(ctx)
	defer s.Close(ctx)

	_, _ = s.Set(ctx, "ns", "key", config.NewValue("value"))

	if err := s.Delete(ctx, "ns", "key"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, err := secondary.Get(ctx, "ns", "key")
	if !config.IsNotFound(err) {
		t.Error("Expected secondary to have deleted the key")
	}
}

func TestStore_Set_ModeAsync_ReplicatesViaWatch(t *testing.T) {
	primary := memory.NewStore()
	secondary := memory.NewStore()

	s := NewStore(primary, []config.Store{secondary}, WithReplicationMode(ModeAsync))
	ctx := context.Background()
	_ = s.Connect(ctx)
	defer s.Close(ctx)

	_, err := s.Set(ctx, "ns", "key", config.NewValue("async-val"))
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Give the background goroutine time to replicate
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		got, err := secondary.Get(ctx, "ns", "key")
		if err == nil {
			str, _ := got.String()
			if str == "async-val" {
				return // success
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("Secondary did not receive async replication within timeout")
}

func TestStore_Delete_ModeAsync_ReplicatesViaWatch(t *testing.T) {
	primary := memory.NewStore()
	secondary := memory.NewStore()

	s := NewStore(primary, []config.Store{secondary}, WithReplicationMode(ModeAsync))
	ctx := context.Background()
	_ = s.Connect(ctx)
	defer s.Close(ctx)

	// Seed both stores
	_, _ = s.Set(ctx, "ns", "key", config.NewValue("v"))

	// Wait for async replication of Set
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, err := secondary.Get(ctx, "ns", "key"); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := s.Delete(ctx, "ns", "key"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	deadline = time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, err := secondary.Get(ctx, "ns", "key"); config.IsNotFound(err) {
			return // success
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("Secondary did not replicate Delete within timeout")
}

func TestStore_Get_ReadSecondary(t *testing.T) {
	primary := memory.NewStore()
	secondary := memory.NewStore()

	s := NewStore(primary, []config.Store{secondary},
		WithReplicationMode(ModeSync),
		WithReadPreference(ReadSecondary),
	)
	ctx := context.Background()
	_ = s.Connect(ctx)
	defer s.Close(ctx)

	// Directly seed secondary (bypass primary)
	_, _ = secondary.Set(ctx, "ns", "key", config.NewValue("sec-val"))

	got, err := s.Get(ctx, "ns", "key")
	if err != nil {
		t.Fatalf("Get with ReadSecondary failed: %v", err)
	}
	str, _ := got.String()
	if str != "sec-val" {
		t.Errorf("Expected 'sec-val', got %s", str)
	}
}

func TestStore_Get_ReadPrimaryPreferred_FallsBack(t *testing.T) {
	primary := &failStore{getErr: config.ErrStoreNotConnected}
	secondary := memory.NewStore()
	ctx := context.Background()
	_, _ = secondary.Set(ctx, "ns", "key", config.NewValue("sec-fallback"))

	s := NewStore(primary, []config.Store{secondary},
		WithReadPreference(ReadPrimaryPreferred),
	)
	// Don't Connect to avoid background goroutine

	got, err := s.Get(ctx, "ns", "key")
	if err != nil {
		t.Fatalf("Get with ReadPrimaryPreferred fallback failed: %v", err)
	}
	str, _ := got.String()
	if str != "sec-fallback" {
		t.Errorf("Expected 'sec-fallback', got %s", str)
	}
}

func TestStore_Get_ReadPrimaryPreferred_UsePrimary(t *testing.T) {
	primary := memory.NewStore()
	secondary := memory.NewStore()
	ctx := context.Background()
	_, _ = primary.Set(ctx, "ns", "key", config.NewValue("pri-val"))
	_, _ = secondary.Set(ctx, "ns", "key", config.NewValue("sec-val"))

	s := NewStore(primary, []config.Store{secondary},
		WithReadPreference(ReadPrimaryPreferred),
	)

	got, err := s.Get(ctx, "ns", "key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	str, _ := got.String()
	if str != "pri-val" {
		t.Errorf("Expected 'pri-val' from primary, got %s", str)
	}
}

func TestStore_Find_ReadPrimary(t *testing.T) {
	primary := memory.NewStore()
	secondary := memory.NewStore()

	ctx := context.Background()
	s := NewStore(primary, []config.Store{secondary}, WithReplicationMode(ModeSync))
	_ = s.Connect(ctx)
	defer s.Close(ctx)

	_, _ = primary.Set(ctx, "ns", "app/k1", config.NewValue("v1"))
	_, _ = secondary.Set(ctx, "ns", "app/k2", config.NewValue("v2"))

	page, err := s.Find(ctx, "ns", config.NewFilter().WithPrefix("app/").Build())
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}
	results := page.Results()
	if len(results) != 1 {
		t.Errorf("Expected 1 result from primary, got %d", len(results))
	}
	if _, ok := results["app/k1"]; !ok {
		t.Error("Expected app/k1 from primary")
	}
}

func TestStore_Find_ReadSecondary(t *testing.T) {
	primary := memory.NewStore()
	secondary := memory.NewStore()

	ctx := context.Background()
	s := NewStore(primary, []config.Store{secondary},
		WithReadPreference(ReadSecondary),
	)
	_ = s.Connect(ctx)
	defer s.Close(ctx)

	_, _ = primary.Set(ctx, "ns", "app/k1", config.NewValue("v1"))
	_, _ = secondary.Set(ctx, "ns", "app/k2", config.NewValue("v2"))

	page, err := s.Find(ctx, "ns", config.NewFilter().WithPrefix("app/").Build())
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}
	results := page.Results()
	if len(results) != 1 {
		t.Errorf("Expected 1 result from secondary, got %d", len(results))
	}
	if _, ok := results["app/k2"]; !ok {
		t.Error("Expected app/k2 from secondary")
	}
}

func TestStore_Watch_DelegatesToPrimary(t *testing.T) {
	primary := memory.NewStore()
	secondary := memory.NewStore()

	ctx := context.Background()
	s := NewStore(primary, []config.Store{secondary})
	_ = s.Connect(ctx)
	defer s.Close(ctx)

	ch, err := s.Watch(ctx, config.WatchFilter{Namespaces: []string{"ns"}})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	if ch == nil {
		t.Error("Expected non-nil watch channel")
	}
}

func TestStore_Health_Delegates(t *testing.T) {
	primary := memory.NewStore()
	secondary := memory.NewStore()

	ctx := context.Background()
	s := NewStore(primary, []config.Store{secondary})
	_ = s.Connect(ctx)
	defer s.Close(ctx)

	if err := s.Health(ctx); err != nil {
		t.Errorf("Health check failed: %v", err)
	}
}

func TestStore_Health_NoHealthChecker(t *testing.T) {
	primary := &plainStore{}
	s := NewStore(primary, nil)

	if err := s.Health(context.Background()); err != nil {
		t.Errorf("Expected nil for non-HealthChecker primary, got %v", err)
	}
}

func TestStore_Stats_DelegatesToPrimary(t *testing.T) {
	primary := memory.NewStore()
	secondary := memory.NewStore()

	ctx := context.Background()
	s := NewStore(primary, []config.Store{secondary})
	_ = s.Connect(ctx)
	defer s.Close(ctx)

	_, _ = s.Set(ctx, "ns", "k1", config.NewValue("v1"))
	time.Sleep(20 * time.Millisecond) // async settle

	stats, err := s.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}
	if stats == nil {
		t.Fatal("Expected non-nil stats")
	}
	if stats.TotalEntries < 1 {
		t.Errorf("Expected at least 1 entry, got %d", stats.TotalEntries)
	}
}

func TestStore_Stats_NoStatsProvider(t *testing.T) {
	primary := &plainStore{}
	s := NewStore(primary, nil)

	stats, err := s.Stats(context.Background())
	if err != nil {
		t.Fatalf("Stats should not error for non-provider, got %v", err)
	}
	if stats != nil {
		t.Error("Expected nil stats for non-provider")
	}
}

func TestStore_InitialSync(t *testing.T) {
	primary := memory.NewStore()
	secondary := memory.NewStore()
	ctx := context.Background()

	// Seed primary before creating the replica store; data must exist before Connect triggers initialSync.
	_ = primary.Connect(ctx)
	_, _ = primary.Set(ctx, "ns1", "k1", config.NewValue("v1"))
	_, _ = primary.Set(ctx, "ns1", "k2", config.NewValue("v2"))
	_, _ = primary.Set(ctx, "ns2", "k3", config.NewValue("v3"))

	s := NewStore(primary, []config.Store{secondary},
		WithInitialSync("ns1", "ns2"),
		WithReplicationMode(ModeSync),
	)
	_ = s.Connect(ctx)
	defer s.Close(ctx)

	// ns1 keys should be in secondary
	for _, key := range []string{"k1", "k2"} {
		got, err := secondary.Get(ctx, "ns1", key)
		if err != nil {
			t.Errorf("Secondary should have ns1/%s after initial sync: %v", key, err)
			continue
		}
		_ = got
	}

	// ns2 key should be in secondary
	got, err := secondary.Get(ctx, "ns2", "k3")
	if err != nil {
		t.Errorf("Secondary should have ns2/k3 after initial sync: %v", err)
	}
	_ = got
}

func TestStore_Primary_Secondaries(t *testing.T) {
	primary := memory.NewStore()
	sec1 := memory.NewStore()
	sec2 := memory.NewStore()

	s := NewStore(primary, []config.Store{sec1, sec2})

	if s.Unwrap() != primary {
		t.Error("Unwrap() should return the primary store")
	}

	secs := s.Secondaries()
	if len(secs) != 2 {
		t.Errorf("Expected 2 secondaries, got %d", len(secs))
	}

	// Mutating the returned slice should not affect internal state
	secs[0] = nil
	if s.secondaries[0] == nil {
		t.Error("Mutating returned Secondaries slice should not affect internal state")
	}
}

func TestStore_GetMany_ReadPrimary(t *testing.T) {
	primary := memory.NewStore()
	secondary := memory.NewStore()

	ctx := context.Background()
	s := NewStore(primary, []config.Store{secondary})
	_ = s.Connect(ctx)
	defer s.Close(ctx)

	_, _ = primary.Set(ctx, "ns", "k1", config.NewValue("v1"))
	_, _ = primary.Set(ctx, "ns", "k2", config.NewValue("v2"))

	results, err := s.GetMany(ctx, "ns", []string{"k1", "k2", "k3"})
	if err != nil {
		t.Fatalf("GetMany failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}
}

func TestStore_GetMany_ReadSecondary(t *testing.T) {
	primary := memory.NewStore()
	secondary := memory.NewStore()

	ctx := context.Background()
	s := NewStore(primary, []config.Store{secondary}, WithReadPreference(ReadSecondary))
	_ = s.Connect(ctx)
	defer s.Close(ctx)

	_, _ = secondary.Set(ctx, "ns", "k1", config.NewValue("from-sec"))

	results, err := s.GetMany(ctx, "ns", []string{"k1"})
	if err != nil {
		t.Fatalf("GetMany failed: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("Expected 1 result from secondary, got %d", len(results))
	}
}

func TestStore_SetMany_ModeSync(t *testing.T) {
	primary := memory.NewStore()
	secondary := memory.NewStore()

	ctx := context.Background()
	s := NewStore(primary, []config.Store{secondary}, WithReplicationMode(ModeSync))
	_ = s.Connect(ctx)
	defer s.Close(ctx)

	vals := map[string]config.Value{
		"k1": config.NewValue("v1"),
		"k2": config.NewValue("v2"),
	}
	if err := s.SetMany(ctx, "ns", vals); err != nil {
		t.Fatalf("SetMany failed: %v", err)
	}

	for _, key := range []string{"k1", "k2"} {
		got, err := secondary.Get(ctx, "ns", key)
		if err != nil {
			t.Errorf("Secondary should have %s: %v", key, err)
			continue
		}
		_ = got
	}
}

func TestStore_DeleteMany_ModeSync(t *testing.T) {
	primary := memory.NewStore()
	secondary := memory.NewStore()

	ctx := context.Background()
	s := NewStore(primary, []config.Store{secondary}, WithReplicationMode(ModeSync))
	_ = s.Connect(ctx)
	defer s.Close(ctx)

	_, _ = s.Set(ctx, "ns", "k1", config.NewValue("v1"))
	_, _ = s.Set(ctx, "ns", "k2", config.NewValue("v2"))

	deleted, err := s.DeleteMany(ctx, "ns", []string{"k1", "k2"})
	if err != nil {
		t.Fatalf("DeleteMany failed: %v", err)
	}
	if deleted != 2 {
		t.Errorf("Expected 2 deleted, got %d", deleted)
	}

	for _, key := range []string{"k1", "k2"} {
		_, err := secondary.Get(ctx, "ns", key)
		if !config.IsNotFound(err) {
			t.Errorf("Expected secondary to have deleted %s", key)
		}
	}
}

func TestStore_GetVersions_NotSupported(t *testing.T) {
	primary := &plainStore{}
	s := NewStore(primary, nil)

	_, err := s.GetVersions(context.Background(), "ns", "key", config.NewVersionFilter().Build())
	if !errors.Is(err, config.ErrVersioningNotSupported) {
		t.Errorf("Expected ErrVersioningNotSupported, got %v", err)
	}
}

func TestStore_SupportsCodec_NoValidator(t *testing.T) {
	primary := &plainStore{}
	s := NewStore(primary, nil)

	if !s.SupportsCodec("json") {
		t.Error("Expected true when primary has no CodecValidator")
	}
}

func TestStore_AsyncReplication_WatchNotSupported(t *testing.T) {
	primary := &noWatchStore{Store: memory.NewStore()}
	secondary := memory.NewStore()

	s := NewStore(primary, []config.Store{secondary}, WithReplicationMode(ModeAsync))
	ctx := context.Background()

	if err := s.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	// Goroutine should exit gracefully when Watch is not supported
	time.Sleep(20 * time.Millisecond)
	if err := s.Close(ctx); err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestStore_Close_StopsReplication(t *testing.T) {
	primary := memory.NewStore()
	secondary := memory.NewStore()

	s := NewStore(primary, []config.Store{secondary}, WithReplicationMode(ModeAsync))
	ctx := context.Background()
	_ = s.Connect(ctx)

	// Close should not hang
	done := make(chan struct{})
	go func() {
		_ = s.Close(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close timed out — replication goroutine may not have stopped")
	}
}

func TestStore_WithWatchBackoff(t *testing.T) {
	primary := memory.NewStore()
	s := NewStore(primary, nil, WithWatchBackoff(1*time.Second))
	if s.opts.watchBackoff != time.Second {
		t.Errorf("Expected 1s backoff, got %v", s.opts.watchBackoff)
	}
}

// --- test helpers ---

type failStore struct {
	connectErr error
	closeErr   error
	getErr     error
}

func (f *failStore) Connect(ctx context.Context) error { return f.connectErr }
func (f *failStore) Close(ctx context.Context) error   { return f.closeErr }

func (f *failStore) Get(ctx context.Context, ns, key string) (config.Value, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	return nil, &config.KeyNotFoundError{Key: key, Namespace: ns}
}

func (f *failStore) Set(ctx context.Context, ns, key string, v config.Value) (config.Value, error) {
	return v, nil
}
func (f *failStore) Delete(ctx context.Context, ns, key string) error { return config.ErrNotFound }
func (f *failStore) Find(ctx context.Context, ns string, fi config.Filter) (config.Page, error) {
	return nil, nil
}
func (f *failStore) Watch(ctx context.Context, fi config.WatchFilter) (<-chan config.ChangeEvent, error) {
	ch := make(chan config.ChangeEvent)
	return ch, nil
}

type noWatchStore struct {
	*memory.Store
}

func (s *noWatchStore) Watch(ctx context.Context, f config.WatchFilter) (<-chan config.ChangeEvent, error) {
	return nil, config.ErrWatchNotSupported
}

type plainStore struct{}

func (s *plainStore) Connect(ctx context.Context) error { return nil }
func (s *plainStore) Close(ctx context.Context) error   { return nil }
func (s *plainStore) Get(ctx context.Context, ns, key string) (config.Value, error) {
	return nil, config.ErrNotFound
}
func (s *plainStore) Set(ctx context.Context, ns, key string, v config.Value) (config.Value, error) {
	return v, nil
}
func (s *plainStore) Delete(ctx context.Context, ns, key string) error { return config.ErrNotFound }
func (s *plainStore) Find(ctx context.Context, ns string, fi config.Filter) (config.Page, error) {
	return config.NewPage(nil, "", 0), nil
}
func (s *plainStore) Watch(ctx context.Context, fi config.WatchFilter) (<-chan config.ChangeEvent, error) {
	return nil, config.ErrWatchNotSupported
}
