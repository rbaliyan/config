package config

import (
	"context"
	"errors"
	"testing"
	"time"
)

// fakeAliasStore is an in-package Store + AliasStore used to drive the
// Manager's alias initialisation paths deterministically.
type fakeAliasStore struct {
	listAliases map[string]Value
	listErr     error

	// setAliasErr is keyed by alias name; a matching entry is returned from
	// SetAlias to exercise the persist-failure and already-exists branches.
	setAliasErr map[string]error

	setCalls []string // aliases passed to SetAlias, in order
}

func (s *fakeAliasStore) Connect(context.Context) error { return nil }
func (s *fakeAliasStore) Close(context.Context) error   { return nil }
func (s *fakeAliasStore) Get(context.Context, string, string) (Value, error) {
	return nil, ErrNotFound
}
func (s *fakeAliasStore) Set(_ context.Context, _, _ string, v Value) (Value, error) { return v, nil }
func (s *fakeAliasStore) Delete(context.Context, string, string) error               { return nil }
func (s *fakeAliasStore) Find(context.Context, string, Filter) (Page, error) {
	return NewPage(nil, "", 0), nil
}
func (s *fakeAliasStore) Watch(context.Context, WatchFilter) (<-chan ChangeEvent, error) {
	return nil, ErrWatchNotSupported
}

func (s *fakeAliasStore) SetAlias(_ context.Context, alias, _ string) (Value, error) {
	s.setCalls = append(s.setCalls, alias)
	if s.setAliasErr != nil {
		if err, ok := s.setAliasErr[alias]; ok {
			return nil, err
		}
	}
	return NewValue("x"), nil
}
func (s *fakeAliasStore) DeleteAlias(context.Context, string) error { return nil }
func (s *fakeAliasStore) GetAlias(context.Context, string) (Value, error) {
	return nil, ErrNotFound
}
func (s *fakeAliasStore) ListAliases(context.Context) (map[string]Value, error) {
	return s.listAliases, s.listErr
}

var (
	_ Store      = (*fakeAliasStore)(nil)
	_ AliasStore = (*fakeAliasStore)(nil)
)

// newTestManager builds a *manager directly (white-box) with a real in-memory
// cache so cache invalidation branches can be observed.
func newTestManager(t *testing.T, store Store) *manager {
	t.Helper()
	m, err := New(WithStore(store))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	mgr, ok := m.(*manager)
	if !ok {
		t.Fatalf("New returned %T, want *manager", m)
	}
	return mgr
}

// TestHandleChangeSetInvalidatesCache covers the ChangeTypeSet branch of
// handleChange: a set event must populate the manager's internal cache.
func TestHandleChangeSetInvalidatesCache(t *testing.T) {
	ctx := context.Background()
	mgr := newTestManager(t, &fakeAliasStore{})
	mgr.status = 1 // mark connected so handleChange does not early-return

	val := NewValue("hello")
	mgr.handleChange(ctx, ChangeEvent{
		Type:      ChangeTypeSet,
		Namespace: "ns",
		Key:       "app/name",
		Value:     val,
		Timestamp: time.Now(),
	})

	got, err := mgr.cache.Get(ctx, "ns", "app/name")
	if err != nil {
		t.Fatalf("expected cached value after set event, got error: %v", err)
	}
	s, _ := got.String()
	if s != "hello" {
		t.Errorf("cached value = %q, want %q", s, "hello")
	}
}

// TestHandleChangeSetNilValueIgnored covers the ChangeTypeSet branch where the
// event carries no value: nothing should be written to the cache.
func TestHandleChangeSetNilValueIgnored(t *testing.T) {
	ctx := context.Background()
	mgr := newTestManager(t, &fakeAliasStore{})
	mgr.status = 1

	mgr.handleChange(ctx, ChangeEvent{
		Type:      ChangeTypeSet,
		Namespace: "ns",
		Key:       "app/name",
		Value:     nil,
		Timestamp: time.Now(),
	})

	if _, err := mgr.cache.Get(ctx, "ns", "app/name"); err == nil {
		t.Error("nil-value set event should not populate cache")
	}
}

// TestHandleChangeDeleteInvalidatesCache covers the ChangeTypeDelete branch:
// a delete event must evict the key from the cache.
func TestHandleChangeDeleteInvalidatesCache(t *testing.T) {
	ctx := context.Background()
	mgr := newTestManager(t, &fakeAliasStore{})
	mgr.status = 1

	// Seed the cache, then deliver a delete event.
	if err := mgr.cache.Set(ctx, "ns", "app/name", NewValue("hello")); err != nil {
		t.Fatalf("seed cache: %v", err)
	}
	mgr.handleChange(ctx, ChangeEvent{
		Type:      ChangeTypeDelete,
		Namespace: "ns",
		Key:       "app/name",
		Timestamp: time.Now(),
	})

	if _, err := mgr.cache.Get(ctx, "ns", "app/name"); err == nil {
		t.Error("delete event should have evicted the key from cache")
	}
}

// TestHandleChangeNotConnectedIsNoop covers the early-return guard in
// handleChange when the manager is not connected.
func TestHandleChangeNotConnectedIsNoop(t *testing.T) {
	ctx := context.Background()
	mgr := newTestManager(t, &fakeAliasStore{})
	// status stays 0 (created, not connected).

	mgr.handleChange(ctx, ChangeEvent{
		Type:      ChangeTypeSet,
		Namespace: "ns",
		Key:       "k",
		Value:     NewValue("v"),
		Timestamp: time.Now(),
	})

	if _, err := mgr.cache.Get(ctx, "ns", "k"); err == nil {
		t.Error("handleChange on a non-connected manager must be a no-op")
	}
}

// TestHandleChangeRoutesAliasEvents covers the alias-routing branch of
// handleChange and the ChangeTypeAliasSet path of handleAliasChange.
func TestHandleChangeRoutesAliasEvents(t *testing.T) {
	ctx := context.Background()
	mgr := newTestManager(t, &fakeAliasStore{})
	mgr.status = 1

	// AliasSet event with a value should install the alias.
	mgr.handleChange(ctx, ChangeEvent{
		Type:      ChangeTypeAliasSet,
		Key:       "db.host",
		Value:     NewValue("database/host"),
		Timestamp: time.Now(),
	})
	if got := mgr.ResolveAlias("db.host"); got != "database/host" {
		t.Errorf("after alias-set event, ResolveAlias = %q, want %q", got, "database/host")
	}

	// AliasDelete event should remove it. Use a strictly later timestamp so the
	// stale-event guard in applyEvent does not drop it.
	mgr.handleChange(ctx, ChangeEvent{
		Type:      ChangeTypeAliasDelete,
		Key:       "db.host",
		Timestamp: time.Now().Add(time.Second),
	})
	if got := mgr.ResolveAlias("db.host"); got != "db.host" {
		t.Errorf("after alias-delete event, ResolveAlias = %q, want unchanged key", got)
	}
}

// TestHandleAliasChangeNilValueIgnored covers the handleAliasChange guard where
// a ChangeTypeAliasSet event arrives with no value: it must be ignored.
func TestHandleAliasChangeNilValueIgnored(t *testing.T) {
	mgr := newTestManager(t, &fakeAliasStore{})
	mgr.status = 1

	mgr.handleAliasChange(ChangeEvent{
		Type:      ChangeTypeAliasSet,
		Key:       "db.host",
		Value:     nil,
		Timestamp: time.Now(),
	})
	if got := mgr.ResolveAlias("db.host"); got != "db.host" {
		t.Errorf("nil-value alias-set event should be ignored, got %q", got)
	}
}

// TestInitAliasesLoadsPersisted covers the happy path of initAliases against an
// AliasStore: persisted aliases are loaded into the resolver.
func TestInitAliasesLoadsPersisted(t *testing.T) {
	ctx := context.Background()
	store := &fakeAliasStore{
		listAliases: map[string]Value{
			"old/key": NewValue("new/key"),
		},
	}
	mgr := newTestManager(t, store)

	mgr.initAliases(ctx)

	if got := mgr.ResolveAlias("old/key"); got != "new/key" {
		t.Errorf("persisted alias not loaded: ResolveAlias = %q, want %q", got, "new/key")
	}
}

// TestInitAliasesListError covers the initAliases branch where ListAliases
// fails: the error is logged and seed aliases are still persisted.
func TestInitAliasesListError(t *testing.T) {
	ctx := context.Background()
	store := &fakeAliasStore{
		listErr: errors.New("boom"),
	}
	mgr := newTestManager(t, store)
	mgr.seedAliases = map[string]string{"a.b": "a/b"}

	mgr.initAliases(ctx)

	// Despite the list failure, the seed alias should have been persisted and
	// installed in the resolver.
	if got := mgr.ResolveAlias("a.b"); got != "a/b" {
		t.Errorf("seed alias not installed after list error: got %q", got)
	}
	if len(store.setCalls) != 1 || store.setCalls[0] != "a.b" {
		t.Errorf("expected SetAlias(\"a.b\") to be called, got %v", store.setCalls)
	}
}

// TestInitAliasesInvalidPersistedValue covers the branch where a persisted
// alias value cannot be coerced to a string and is skipped.
func TestInitAliasesInvalidPersistedValue(t *testing.T) {
	ctx := context.Background()
	store := &fakeAliasStore{
		listAliases: map[string]Value{
			// A nil-raw value: String() returns ErrNotFound, so it is skipped.
			"bad": NewValue(nil),
			"ok":  NewValue("real/target"),
		},
	}
	mgr := newTestManager(t, store)

	mgr.initAliases(ctx)

	if mgr.aliases.has("bad") {
		t.Error("alias with invalid persisted value should be skipped")
	}
	if got := mgr.ResolveAlias("ok"); got != "real/target" {
		t.Errorf("valid persisted alias not loaded: got %q", got)
	}
}

// TestInitAliasesSeedPersistFails covers the branch where persisting a seed
// alias fails with a non-AliasExists error: the alias is not installed.
func TestInitAliasesSeedPersistFails(t *testing.T) {
	ctx := context.Background()
	store := &fakeAliasStore{
		setAliasErr: map[string]error{"x.y": errors.New("store down")},
	}
	mgr := newTestManager(t, store)
	mgr.seedAliases = map[string]string{"x.y": "x/y"}

	mgr.initAliases(ctx)

	if mgr.aliases.has("x.y") {
		t.Error("seed alias should not be installed when persist fails with a non-exists error")
	}
}

// TestInitAliasesSeedAlreadyExists covers the branch where persisting a seed
// alias returns an already-exists error: the failure is tolerated (no install,
// no warning escalation).
func TestInitAliasesSeedAlreadyExists(t *testing.T) {
	ctx := context.Background()
	store := &fakeAliasStore{
		setAliasErr: map[string]error{"dup": &AliasExistsError{Alias: "dup"}},
	}
	mgr := newTestManager(t, store)
	mgr.seedAliases = map[string]string{"dup": "real/dup"}

	mgr.initAliases(ctx)

	if mgr.aliases.has("dup") {
		t.Error("seed alias should not be installed when store reports it already exists")
	}
}

// TestInitAliasesNonAliasStoreSeedsInMemory covers the branch where the store
// does not implement AliasStore: seed aliases are loaded into memory only, and
// an invalid seed is logged and skipped.
func TestInitAliasesNonAliasStoreSeedsInMemory(t *testing.T) {
	ctx := context.Background()
	// plainStore2 does not implement AliasStore.
	mgr := newTestManager(t, &plainStore2{})
	mgr.seedAliases = map[string]string{
		"good.alias": "good/target",
		"bad":        "bad", // self-reference: aliasResolver.set rejects it
	}

	mgr.initAliases(ctx)

	if got := mgr.ResolveAlias("good.alias"); got != "good/target" {
		t.Errorf("in-memory seed not installed: got %q", got)
	}
	if mgr.aliases.has("bad") {
		t.Error("invalid self-referential seed should be skipped")
	}
}

// failingCache returns an error from Set and Delete so the error-logging
// branches of handleChange (guarded by isConnected) are exercised.
type failingCache struct{}

func (failingCache) Get(context.Context, string, string) (Value, error) { return nil, ErrNotFound }
func (failingCache) Set(context.Context, string, string, Value) error {
	return errors.New("cache set failed")
}
func (failingCache) Delete(context.Context, string, string) error {
	return errors.New("cache delete failed")
}
func (failingCache) Stats() CacheStats { return CacheStats{} }

// TestHandleChangeCacheErrorsLogged covers the error-handling branches of
// handleChange for both ChangeTypeSet and ChangeTypeDelete when the cache
// returns an error. The manager must not panic and must remain usable.
func TestHandleChangeCacheErrorsLogged(t *testing.T) {
	ctx := context.Background()
	m, err := New(WithStore(&fakeAliasStore{}), WithCache(failingCache{}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	mgr := m.(*manager)
	mgr.status = 1

	// Set branch: cache.Set errors and is logged (connected).
	mgr.handleChange(ctx, ChangeEvent{
		Type: ChangeTypeSet, Namespace: "ns", Key: "k", Value: NewValue("v"), Timestamp: time.Now(),
	})
	// Delete branch: cache.Delete errors and is logged (connected).
	mgr.handleChange(ctx, ChangeEvent{
		Type: ChangeTypeDelete, Namespace: "ns", Key: "k", Timestamp: time.Now(),
	})
}

// --- value.go codec paths (rawCodec / secretCodec) ---

// TestRawCodecRefusesEncodeDecode covers rawCodec.Encode and rawCodec.Decode,
// which are pass-through-only: both must return an error mentioning the codec
// name, since raw values are opaque bytes managed externally.
func TestRawCodecRefusesEncodeDecode(t *testing.T) {
	ctx := context.Background()
	rc := &rawCodec{name: "encrypted-aes"}

	if rc.Name() != "encrypted-aes" {
		t.Errorf("Name() = %q, want %q", rc.Name(), "encrypted-aes")
	}

	if _, err := rc.Encode(ctx, "anything"); err == nil {
		t.Error("rawCodec.Encode should refuse to encode")
	}
	var out any
	if err := rc.Decode(ctx, []byte("x"), &out); err == nil {
		t.Error("rawCodec.Decode should refuse to decode")
	}
}

// TestSecretCodecEncode covers all branches of secretCodec.Encode: []byte,
// *Secret, and the unsupported-type error path.
func TestSecretCodecEncode(t *testing.T) {
	ctx := context.Background()
	c := secretCodec{}

	if c.Name() != "secret" {
		t.Errorf("Name() = %q, want %q", c.Name(), "secret")
	}

	// []byte pass-through.
	b, err := c.Encode(ctx, []byte("raw-bytes"))
	if err != nil || string(b) != "raw-bytes" {
		t.Errorf("Encode([]byte) = %q, %v; want raw-bytes, nil", b, err)
	}

	// *Secret -> its bytes.
	b, err = c.Encode(ctx, NewSecretBytes([]byte("topsecret")))
	if err != nil || string(b) != "topsecret" {
		t.Errorf("Encode(*Secret) = %q, %v; want topsecret, nil", b, err)
	}

	// Unsupported type.
	if _, err := c.Encode(ctx, 42); err == nil {
		t.Error("Encode(int) should return an error")
	}
}

// TestSecretCodecDecode covers all branches of secretCodec.Decode: *any,
// **Secret, and the unsupported-target error path.
func TestSecretCodecDecode(t *testing.T) {
	ctx := context.Background()
	c := secretCodec{}
	data := []byte("decoded-secret")

	// Decode into *any sets the raw bytes.
	var anyTarget any
	if err := c.Decode(ctx, data, &anyTarget); err != nil {
		t.Fatalf("Decode(*any) error: %v", err)
	}
	if gotBytes, ok := anyTarget.([]byte); !ok || string(gotBytes) != "decoded-secret" {
		t.Errorf("Decode(*any) target = %v, want []byte(decoded-secret)", anyTarget)
	}

	// Decode into **Secret wraps the bytes in a Secret.
	var secretTarget *Secret
	if err := c.Decode(ctx, data, &secretTarget); err != nil {
		t.Fatalf("Decode(**Secret) error: %v", err)
	}
	if secretTarget == nil || string(secretTarget.Bytes()) != "decoded-secret" {
		t.Errorf("Decode(**Secret) = %v, want secret holding decoded-secret", secretTarget)
	}

	// Unsupported target.
	var bad int
	if err := c.Decode(ctx, data, &bad); err == nil {
		t.Error("Decode(*int) should return an error")
	}
}

// plainStore2 is a minimal Store that does NOT implement AliasStore, used to
// exercise the in-memory-only seed branch of initAliases.
var _ Store = (*plainStore2)(nil)

type plainStore2 struct{}

func (p *plainStore2) Connect(context.Context) error                      { return nil }
func (p *plainStore2) Close(context.Context) error                        { return nil }
func (p *plainStore2) Get(context.Context, string, string) (Value, error) { return nil, ErrNotFound }
func (p *plainStore2) Set(_ context.Context, _, _ string, v Value) (Value, error) {
	return v, nil
}
func (p *plainStore2) Delete(context.Context, string, string) error { return nil }
func (p *plainStore2) Find(context.Context, string, Filter) (Page, error) {
	return NewPage(nil, "", 0), nil
}
func (p *plainStore2) Watch(context.Context, WatchFilter) (<-chan ChangeEvent, error) {
	return nil, ErrWatchNotSupported
}
