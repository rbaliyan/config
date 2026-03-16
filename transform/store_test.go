package transform

import (
	"context"
	"testing"
	"time"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/memory"
)

// xorTransformer is a deterministic, reversible transformer for testing.
// XOR with a fixed key is its own inverse.
type xorTransformer struct {
	key byte
}

func (x *xorTransformer) Name() string { return "xor" }

func (x *xorTransformer) Transform(data []byte) ([]byte, error) {
	out := make([]byte, len(data))
	for i, b := range data {
		out[i] = b ^ x.key
	}
	return out, nil
}

func (x *xorTransformer) Reverse(data []byte) ([]byte, error) {
	return x.Transform(data) // XOR is its own inverse
}

func newTestStore(t *testing.T) (*transformStore, *memory.Store) {
	t.Helper()
	inner := memory.NewStore()
	s, err := WrapStore(inner, &xorTransformer{key: 0x42})
	if err != nil {
		t.Fatalf("WrapStore: %v", err)
	}
	ts := s.(*transformStore)
	if err := ts.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { ts.Close(context.Background()) })
	return ts, inner
}

func TestWrapStore_NilStore(t *testing.T) {
	_, err := WrapStore(nil, &xorTransformer{key: 0x42})
	if err == nil {
		t.Fatal("expected error for nil store")
	}
}

func TestWrapStore_NilTransformer(t *testing.T) {
	_, err := WrapStore(memory.NewStore(), nil)
	if err == nil {
		t.Fatal("expected error for nil transformer")
	}
}

func TestUnwrap(t *testing.T) {
	inner := memory.NewStore()
	s, err := WrapStore(inner, &xorTransformer{key: 0x42})
	if err != nil {
		t.Fatal(err)
	}
	if s.(*transformStore).Unwrap() != inner {
		t.Fatal("Unwrap did not return inner store")
	}
}

func TestRoundTrip(t *testing.T) {
	ts, _ := newTestStore(t)
	ctx := context.Background()

	original := config.NewValue("hello world")
	result, err := ts.Set(ctx, "", "key1", original)
	if err != nil {
		t.Fatalf("Set: %v", err)
	}

	got, err := ts.Get(ctx, "", "key1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	s, err := got.String()
	if err != nil {
		t.Fatalf("String: %v", err)
	}
	if s != "hello world" {
		t.Fatalf("got %q, want %q", s, "hello world")
	}

	// Result from Set should also be reversed.
	rs, err := result.String()
	if err != nil {
		t.Fatalf("result String: %v", err)
	}
	if rs != "hello world" {
		t.Fatalf("set result got %q, want %q", rs, "hello world")
	}
}

func TestStoredBytesAreTransformed(t *testing.T) {
	ts, inner := newTestStore(t)
	ctx := context.Background()

	original := config.NewValue("hello")
	if _, err := ts.Set(ctx, "", "key1", original); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// Read directly from inner store — bytes should be transformed (not plain).
	raw, err := inner.Get(ctx, "", "key1")
	if err != nil {
		t.Fatalf("inner Get: %v", err)
	}

	data, err := raw.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	// The original JSON encoding of "hello" is `"hello"` (with quotes).
	// After XOR with 0x42, the bytes should differ.
	originalData, _ := original.Marshal()
	if string(data) == string(originalData) {
		t.Fatal("stored bytes should differ from original, but they are identical")
	}
}

func TestMetadataPreservation(t *testing.T) {
	ts, _ := newTestStore(t)
	ctx := context.Background()

	if _, err := ts.Set(ctx, "", "meta-key", config.NewValue(42)); err != nil {
		t.Fatalf("Set: %v", err)
	}

	got, err := ts.Get(ctx, "", "meta-key")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	m := got.Metadata()
	if m.Version() < 1 {
		t.Errorf("version = %d, want >= 1", m.Version())
	}
	if m.CreatedAt().IsZero() {
		t.Error("createdAt is zero")
	}
	if m.UpdatedAt().IsZero() {
		t.Error("updatedAt is zero")
	}

	// Verify type is preserved.
	if got.Type() != config.TypeInt {
		t.Errorf("type = %v, want TypeInt", got.Type())
	}
}

func TestFind(t *testing.T) {
	ts, _ := newTestStore(t)
	ctx := context.Background()

	for _, kv := range []struct{ k, v string }{
		{"a", "alpha"},
		{"b", "bravo"},
		{"c", "charlie"},
	} {
		if _, err := ts.Set(ctx, "", kv.k, config.NewValue(kv.v)); err != nil {
			t.Fatalf("Set %s: %v", kv.k, err)
		}
	}

	page, err := ts.Find(ctx, "", config.NewFilter().WithPrefix("").Build())
	if err != nil {
		t.Fatalf("Find: %v", err)
	}

	results := page.Results()
	if len(results) != 3 {
		t.Fatalf("got %d results, want 3", len(results))
	}

	for _, kv := range []struct{ k, v string }{
		{"a", "alpha"},
		{"b", "bravo"},
		{"c", "charlie"},
	} {
		v, ok := results[kv.k]
		if !ok {
			t.Errorf("missing key %q", kv.k)
			continue
		}
		s, err := v.String()
		if err != nil {
			t.Errorf("String(%s): %v", kv.k, err)
			continue
		}
		if s != kv.v {
			t.Errorf("key %q: got %q, want %q", kv.k, s, kv.v)
		}
	}
}

func TestWatch(t *testing.T) {
	ts, _ := newTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := ts.Watch(ctx, config.WatchFilter{})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	// Set a value — should trigger a watch event.
	if _, err := ts.Set(ctx, "", "watch-key", config.NewValue("watched")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	select {
	case evt := <-ch:
		if evt.Key != "watch-key" {
			t.Errorf("event key = %q, want %q", evt.Key, "watch-key")
		}
		if evt.Value == nil {
			t.Fatal("event value is nil")
		}
		s, err := evt.Value.String()
		if err != nil {
			t.Fatalf("event value String: %v", err)
		}
		if s != "watched" {
			t.Errorf("event value = %q, want %q", s, "watched")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for watch event")
	}

	// Delete should pass through with nil value.
	if err := ts.Delete(ctx, "", "watch-key"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	select {
	case evt := <-ch:
		if evt.Type != config.ChangeTypeDelete {
			t.Errorf("event type = %v, want ChangeTypeDelete", evt.Type)
		}
		if evt.Value != nil {
			t.Error("delete event should have nil value")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for delete event")
	}
}

func TestWatchContextCancellation(t *testing.T) {
	ts, _ := newTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())

	ch, err := ts.Watch(ctx, config.WatchFilter{})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	cancel()

	// Channel should eventually close.
	select {
	case _, ok := <-ch:
		if ok {
			// Got an event, drain and wait for close.
			for range ch {
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for watch channel to close")
	}
}

func TestBulkStore(t *testing.T) {
	ts, _ := newTestStore(t)
	ctx := context.Background()

	// SetMany
	values := map[string]config.Value{
		"k1": config.NewValue("v1"),
		"k2": config.NewValue("v2"),
		"k3": config.NewValue("v3"),
	}
	if err := ts.SetMany(ctx, "", values); err != nil {
		t.Fatalf("SetMany: %v", err)
	}

	// GetMany
	results, err := ts.GetMany(ctx, "", []string{"k1", "k2", "k3"})
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("GetMany returned %d results, want 3", len(results))
	}
	for k, expected := range map[string]string{"k1": "v1", "k2": "v2", "k3": "v3"} {
		v, ok := results[k]
		if !ok {
			t.Errorf("missing key %q", k)
			continue
		}
		s, _ := v.String()
		if s != expected {
			t.Errorf("key %q: got %q, want %q", k, s, expected)
		}
	}

	// DeleteMany
	deleted, err := ts.DeleteMany(ctx, "", []string{"k1", "k2"})
	if err != nil {
		t.Fatalf("DeleteMany: %v", err)
	}
	if deleted != 2 {
		t.Errorf("deleted = %d, want 2", deleted)
	}

	// Verify deletion
	remaining, err := ts.GetMany(ctx, "", []string{"k1", "k2", "k3"})
	if err != nil {
		t.Fatalf("GetMany after delete: %v", err)
	}
	if len(remaining) != 1 {
		t.Errorf("remaining = %d, want 1", len(remaining))
	}
}

func TestOptionalInterfaces(t *testing.T) {
	ts, _ := newTestStore(t)
	ctx := context.Background()

	// Health — memory store implements HealthChecker.
	if err := ts.Health(ctx); err != nil {
		t.Errorf("Health: %v", err)
	}

	// Stats — memory store implements StatsProvider.
	stats, err := ts.Stats(ctx)
	if err != nil {
		t.Errorf("Stats: %v", err)
	}
	if stats == nil {
		t.Error("Stats returned nil")
	}

	// SupportsCodec — memory store doesn't implement CodecValidator, should return true.
	if !ts.SupportsCodec("json") {
		t.Error("SupportsCodec(json) = false, want true")
	}
}

func TestWithWatchBufferSize(t *testing.T) {
	inner := memory.NewStore()
	s, err := WrapStore(inner, &xorTransformer{key: 0x42}, WithWatchBufferSize(10))
	if err != nil {
		t.Fatal(err)
	}
	if s.(*transformStore).opts.watchBufferSize != 10 {
		t.Errorf("watchBufferSize = %d, want 10", s.(*transformStore).opts.watchBufferSize)
	}
}
