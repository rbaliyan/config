package consul_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	consulapi "github.com/hashicorp/consul/api"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/consul"
	_ "github.com/rbaliyan/config/codec/json"
)

// fakeConsulKV is an in-memory Consul KV store that serves the Consul KV HTTP API.
// It is used to test the consul.Store without a running Consul agent.
type fakeConsulKV struct {
	mu      sync.RWMutex
	data    map[string][]byte // consul key → raw value bytes
	indexes map[string]uint64 // consul key → ModifyIndex
	globalIndex uint64
	changeCh chan struct{} // closed and recreated on each mutation
	changeMu sync.Mutex
}

func newFakeConsulKV() *fakeConsulKV {
	return &fakeConsulKV{
		data:        make(map[string][]byte),
		indexes:     make(map[string]uint64),
		changeCh:    make(chan struct{}),
		globalIndex: 1, // Consul always starts at index >= 1 (never 0).
	}
}

// notifyChange signals all blocking watchers that data changed.
func (f *fakeConsulKV) notifyChange() {
	f.changeMu.Lock()
	old := f.changeCh
	f.changeCh = make(chan struct{})
	f.changeMu.Unlock()
	close(old)
}

// waitForChange blocks until data changes or the context expires.
func (f *fakeConsulKV) waitForChange(ctx context.Context) {
	f.changeMu.Lock()
	ch := f.changeCh
	f.changeMu.Unlock()
	select {
	case <-ch:
	case <-ctx.Done():
	}
}

// ServeHTTP handles Consul KV HTTP API requests.
func (f *fakeConsulKV) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	if !strings.HasPrefix(path, "/v1/kv/") {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	key := strings.TrimPrefix(path, "/v1/kv/")

	switch r.Method {
	case http.MethodGet:
		f.handleGet(w, r, key)
	case http.MethodPut:
		f.handlePut(w, r, key)
	case http.MethodDelete:
		f.handleDelete(w, r, key)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (f *fakeConsulKV) handleGet(w http.ResponseWriter, r *http.Request, key string) {
	// Use Has() to detect presence of key-only params (no value).
	recurse := r.URL.Query().Has("recurse")
	waitIndexStr := r.URL.Query().Get("index")
	waitStr := r.URL.Query().Get("wait")

	// Blocking query support.
	if waitIndexStr != "" {
		var waitIndex uint64
		fmt.Sscan(waitIndexStr, &waitIndex)
		var waitDur time.Duration
		if waitStr != "" {
			// Consul uses "Xms" format; just parse as ms for simplicity.
			var ms int
			fmt.Sscanf(waitStr, "%dms", &ms)
			waitDur = time.Duration(ms) * time.Millisecond
		}
		if waitDur == 0 {
			waitDur = 5 * time.Millisecond // short timeout for tests
		}

		// Wait until index changes or timeout.
		ctx, cancel := context.WithTimeout(r.Context(), waitDur)
		defer cancel()

		f.mu.RLock()
		currentIndex := f.globalIndex
		f.mu.RUnlock()

		if currentIndex <= waitIndex {
			f.waitForChange(ctx)
		}
	}

	f.mu.RLock()
	defer f.mu.RUnlock()

	w.Header().Set("X-Consul-Index", fmt.Sprintf("%d", f.globalIndex))

	if !recurse {
		// Single key lookup.
		val, ok := f.data[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		pairs := []map[string]any{f.buildPair(key, val)}
		writeJSON(w, pairs)
		return
	}

	// List all keys with prefix.
	var pairs []map[string]any
	for k, v := range f.data {
		if strings.HasPrefix(k, key) {
			pairs = append(pairs, f.buildPair(k, v))
		}
	}
	if len(pairs) == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	writeJSON(w, pairs)
}

func (f *fakeConsulKV) buildPair(key string, val []byte) map[string]any {
	return map[string]any{
		"Key":         key,
		"Value":       base64.StdEncoding.EncodeToString(val),
		"ModifyIndex": f.indexes[key],
		"CreateIndex": f.indexes[key],
		"Flags":       0,
		"LockIndex":   0,
		"Session":     "",
	}
}

func (f *fakeConsulKV) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	casStr := r.URL.Query().Get("cas")
	var body []byte
	if r.Body != nil {
		buf := make([]byte, r.ContentLength)
		if r.ContentLength > 0 {
			r.Body.Read(buf) //nolint:errcheck
			body = buf
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if casStr != "" {
		var cas uint64
		fmt.Sscan(casStr, &cas)
		existing := f.indexes[key]
		if cas == 0 && existing != 0 {
			// CAS create: key already exists.
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, "false")
			return
		}
	}

	f.globalIndex++
	f.data[key] = body
	f.indexes[key] = f.globalIndex
	f.notifyChange()

	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "true")
}

func (f *fakeConsulKV) handleDelete(w http.ResponseWriter, r *http.Request, key string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.data[key]; !ok {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "true")
		return
	}
	delete(f.data, key)
	delete(f.indexes, key)
	f.globalIndex++
	f.notifyChange()

	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "true")
}

func writeJSON(w http.ResponseWriter, v any) {
	data, err := json.Marshal(v)
	if err != nil {
		http.Error(w, "marshal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(data) //nolint:errcheck
}

// newTestStore creates a consul.Store backed by a fake in-memory HTTP server.
func newTestStore(t *testing.T, namespace string, opts ...consul.Option) (*consul.Store, *fakeConsulKV) {
	t.Helper()
	fake := newFakeConsulKV()
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)

	cfg := consulapi.DefaultConfig()
	cfg.Address = strings.TrimPrefix(srv.URL, "http://")
	client, err := consulapi.NewClient(cfg)
	if err != nil {
		t.Fatalf("failed to create consul client: %v", err)
	}

	store, err := consul.New(client, namespace, opts...)
	if err != nil {
		t.Fatalf("consul.New: %v", err)
	}
	ctx := context.Background()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("store.Connect: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(ctx) })
	return store, fake
}

// TestGet_NotFound checks that Get returns ErrNotFound for a missing key.
func TestGet_NotFound(t *testing.T) {
	store, _ := newTestStore(t, "test")
	_, err := store.Get(context.Background(), "test", "missing-key")
	if !config.IsNotFound(err) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

// TestSet_Get_RoundTrip verifies that a value set and then retrieved is equal.
func TestSet_Get_RoundTrip(t *testing.T) {
	store, _ := newTestStore(t, "test")
	ctx := context.Background()

	v := config.NewValue("hello world")
	stored, err := store.Set(ctx, "test", "greeting", v)
	if err != nil {
		t.Fatalf("Set: %v", err)
	}
	if stored == nil {
		t.Fatal("Set returned nil value")
	}
	if stored.Metadata().Version() != 1 {
		t.Errorf("expected version 1, got %d", stored.Metadata().Version())
	}

	got, err := store.Get(ctx, "test", "greeting")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	s, err := got.String()
	if err != nil {
		t.Fatalf("String(): %v", err)
	}
	if s != "hello world" {
		t.Errorf("got %q, want %q", s, "hello world")
	}
}

// TestSet_Update_IncrementsVersion verifies version increments on update.
func TestSet_Update_IncrementsVersion(t *testing.T) {
	store, _ := newTestStore(t, "ns")
	ctx := context.Background()

	if _, err := store.Set(ctx, "ns", "k", config.NewValue("v1")); err != nil {
		t.Fatalf("Set v1: %v", err)
	}
	stored, err := store.Set(ctx, "ns", "k", config.NewValue("v2"))
	if err != nil {
		t.Fatalf("Set v2: %v", err)
	}
	if stored.Metadata().Version() != 2 {
		t.Errorf("expected version 2 after update, got %d", stored.Metadata().Version())
	}
}

// TestSet_WriteModeCreate verifies that create-only fails if key exists.
func TestSet_WriteModeCreate(t *testing.T) {
	store, _ := newTestStore(t, "ns")
	ctx := context.Background()

	v := config.NewValue("initial", config.WithValueWriteMode(config.WriteModeCreate))
	if _, err := store.Set(ctx, "ns", "create-key", v); err != nil {
		t.Fatalf("first create: %v", err)
	}

	v2 := config.NewValue("duplicate", config.WithValueWriteMode(config.WriteModeCreate))
	_, err := store.Set(ctx, "ns", "create-key", v2)
	if !config.IsKeyExists(err) {
		t.Fatalf("expected ErrKeyExists, got %v", err)
	}
}

// TestSet_WriteModeUpdate verifies that update-only fails if key doesn't exist.
func TestSet_WriteModeUpdate(t *testing.T) {
	store, _ := newTestStore(t, "ns")
	ctx := context.Background()

	v := config.NewValue("data", config.WithValueWriteMode(config.WriteModeUpdate))
	_, err := store.Set(ctx, "ns", "no-such-key", v)
	if !config.IsNotFound(err) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

// TestDelete removes a key and checks that subsequent Get returns ErrNotFound.
func TestDelete(t *testing.T) {
	store, _ := newTestStore(t, "ns")
	ctx := context.Background()

	if _, err := store.Set(ctx, "ns", "del-me", config.NewValue(42)); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := store.Delete(ctx, "ns", "del-me"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := store.Get(ctx, "ns", "del-me"); !config.IsNotFound(err) {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
}

// TestDelete_NotFound verifies that deleting a missing key returns ErrNotFound.
func TestDelete_NotFound(t *testing.T) {
	store, _ := newTestStore(t, "ns")
	err := store.Delete(context.Background(), "ns", "ghost")
	if !config.IsNotFound(err) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

// TestFind_Prefix lists keys under a prefix.
func TestFind_Prefix(t *testing.T) {
	store, _ := newTestStore(t, "app")
	ctx := context.Background()

	keys := []string{"db/host", "db/port", "cache/host"}
	for _, k := range keys {
		if _, err := store.Set(ctx, "app", k, config.NewValue(k+"-value")); err != nil {
			t.Fatalf("Set %s: %v", k, err)
		}
	}

	page, err := store.Find(ctx, "app", config.NewFilter().WithPrefix("db/").Build())
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	results := page.Results()
	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d: %v", len(results), results)
	}
	if _, ok := results["db/host"]; !ok {
		t.Error("missing db/host")
	}
	if _, ok := results["db/port"]; !ok {
		t.Error("missing db/port")
	}
}

// TestFind_Keys looks up specific keys.
func TestFind_Keys(t *testing.T) {
	store, _ := newTestStore(t, "ns")
	ctx := context.Background()

	if _, err := store.Set(ctx, "ns", "k1", config.NewValue("v1")); err != nil {
		t.Fatalf("Set k1: %v", err)
	}
	if _, err := store.Set(ctx, "ns", "k2", config.NewValue("v2")); err != nil {
		t.Fatalf("Set k2: %v", err)
	}

	page, err := store.Find(ctx, "ns", config.NewFilter().WithKeys("k1", "k3").Build())
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if len(page.Results()) != 1 {
		t.Errorf("expected 1 result (only k1), got %d", len(page.Results()))
	}
}

// TestGetMany retrieves multiple values.
func TestGetMany(t *testing.T) {
	store, _ := newTestStore(t, "ns")
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("key%d", i)
		if _, err := store.Set(ctx, "ns", k, config.NewValue(i)); err != nil {
			t.Fatalf("Set %s: %v", k, err)
		}
	}

	results, err := store.GetMany(ctx, "ns", []string{"key0", "key2", "key4", "missing"})
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("expected 3 results, got %d", len(results))
	}
	for _, k := range []string{"key0", "key2", "key4"} {
		if _, ok := results[k]; !ok {
			t.Errorf("missing key %s", k)
		}
	}
}

// TestSetMany creates/updates multiple values.
func TestSetMany(t *testing.T) {
	store, _ := newTestStore(t, "ns")
	ctx := context.Background()

	values := map[string]config.Value{
		"a": config.NewValue("aval"),
		"b": config.NewValue("bval"),
		"c": config.NewValue("cval"),
	}
	if err := store.SetMany(ctx, "ns", values); err != nil {
		t.Fatalf("SetMany: %v", err)
	}

	for key, val := range values {
		got, err := store.Get(ctx, "ns", key)
		if err != nil {
			t.Fatalf("Get %s: %v", key, err)
		}
		wantStr, _ := val.String()
		gotStr, _ := got.String()
		if gotStr != wantStr {
			t.Errorf("key %s: got %q, want %q", key, gotStr, wantStr)
		}
	}
}

// TestWatch detects a Set event via Watch.
func TestWatch(t *testing.T) {
	store, _ := newTestStore(t, "ns", consul.WithWatchTimeout(50*time.Millisecond))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch, err := store.Watch(ctx, config.WatchFilter{})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	var received atomic.Int32
	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		for ev := range ch {
			if ev.Key == "watch-key" && ev.Type == config.ChangeTypeSet {
				received.Add(1)
				return
			}
		}
	}()

	// Give the watch goroutine a moment to subscribe.
	time.Sleep(20 * time.Millisecond)

	if _, err := store.Set(ctx, "ns", "watch-key", config.NewValue("watched")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	select {
	case <-doneCh:
	case <-ctx.Done():
		t.Fatal("timed out waiting for watch event")
	}

	if received.Load() == 0 {
		t.Error("expected at least one watch event")
	}
}

// TestWatch_Delete detects a Delete event via Watch.
func TestWatch_Delete(t *testing.T) {
	store, _ := newTestStore(t, "ns", consul.WithWatchTimeout(50*time.Millisecond))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := store.Set(ctx, "ns", "del-watch", config.NewValue("x")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	ch, err := store.Watch(ctx, config.WatchFilter{})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		for ev := range ch {
			if ev.Key == "del-watch" && ev.Type == config.ChangeTypeDelete {
				return
			}
		}
	}()

	time.Sleep(20 * time.Millisecond)
	if err := store.Delete(ctx, "ns", "del-watch"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	select {
	case <-doneCh:
	case <-ctx.Done():
		t.Fatal("timed out waiting for delete watch event")
	}
}

// TestStore_Closed verifies that operations on a closed store return ErrStoreClosed.
func TestStore_Closed(t *testing.T) {
	store, _ := newTestStore(t, "ns")
	ctx := context.Background()

	if err := store.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := store.Get(ctx, "ns", "k"); err != config.ErrStoreClosed {
		t.Errorf("expected ErrStoreClosed, got %v", err)
	}
	if _, err := store.Set(ctx, "ns", "k", config.NewValue(1)); err != config.ErrStoreClosed {
		t.Errorf("expected ErrStoreClosed on Set, got %v", err)
	}
	if err := store.Delete(ctx, "ns", "k"); err != config.ErrStoreClosed {
		t.Errorf("expected ErrStoreClosed on Delete, got %v", err)
	}
	if _, err := store.Find(ctx, "ns", nil); err != config.ErrStoreClosed {
		t.Errorf("expected ErrStoreClosed on Find, got %v", err)
	}
	if _, err := store.Watch(ctx, config.WatchFilter{}); err != config.ErrStoreClosed {
		t.Errorf("expected ErrStoreClosed on Watch, got %v", err)
	}
}

// TestNew_NilClient verifies that New rejects a nil client.
func TestNew_NilClient(t *testing.T) {
	_, err := consul.New(nil, "ns")
	if err == nil {
		t.Fatal("expected error for nil client")
	}
}

// TestNew_InvalidNamespace verifies that New rejects invalid namespace names.
func TestNew_InvalidNamespace(t *testing.T) {
	cfg := consulapi.DefaultConfig()
	client, _ := consulapi.NewClient(cfg)
	_, err := consul.New(client, "invalid namespace!")
	if err == nil {
		t.Fatal("expected error for invalid namespace")
	}
}
