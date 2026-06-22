package file

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/internal/testutil"
	"gopkg.in/yaml.v3"
)

// writableStore builds a writable file store rooted at a temp file pre-seeded
// with the given YAML body, connects it, and registers cleanup.
func writableStore(t *testing.T, body string, opts ...StoreOption) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("seed base file: %v", err)
	}
	opts = append([]StoreOption{WithWritable()}, opts...)
	s := NewStore(path, opts...)
	if err := s.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s, path
}

func TestWritable_SetGetDeleteRoundTrip(t *testing.T) {
	ctx := context.Background()
	s, path := writableStore(t, "db:\n  host: localhost\n")

	// Set a new key. Set succeeds and returns a value with metadata.
	set, err := s.Set(ctx, "db", "port", config.NewValue(5432))
	if err != nil {
		t.Fatalf("Set: %v", err)
	}
	if set == nil {
		t.Fatal("Set returned nil value")
	}
	if set.Metadata().Version() < 1 {
		t.Errorf("Set version = %d, want >= 1", set.Metadata().Version())
	}

	// Get round-trips the entry back (key is present and readable).
	if _, err := s.Get(ctx, "db", "port"); err != nil {
		t.Fatalf("Get after Set: %v", err)
	}

	// The sidecar file must exist on disk and contain the written key.
	sidecarPath := path + ".writes.yaml"
	raw, err := os.ReadFile(sidecarPath)
	if err != nil {
		t.Fatalf("read sidecar: %v", err)
	}
	var sc map[string]map[string]any
	if err := yaml.Unmarshal(raw, &sc); err != nil {
		t.Fatalf("unmarshal sidecar: %v", err)
	}
	if _, ok := sc["db"]["port"]; !ok {
		t.Fatalf("sidecar missing db/port, got: %v", sc)
	}

	// Delete removes it; Get then reports not found.
	if err := s.Delete(ctx, "db", "port"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.Get(ctx, "db", "port"); !config.IsNotFound(err) {
		t.Fatalf("Get after Delete: want NotFound, got %v", err)
	}

	// The sidecar tombstone must be persisted (nil value for the key).
	raw, err = os.ReadFile(sidecarPath)
	if err != nil {
		t.Fatalf("read sidecar after delete: %v", err)
	}
	sc = nil
	if err := yaml.Unmarshal(raw, &sc); err != nil {
		t.Fatalf("unmarshal sidecar after delete: %v", err)
	}
	v, present := sc["db"]["port"]
	if !present {
		t.Fatalf("sidecar should retain a tombstone for db/port")
	}
	if v != nil {
		t.Errorf("tombstone value = %v, want nil", v)
	}
}

func TestWritable_SetPersistsAcrossReconnect(t *testing.T) {
	ctx := context.Background()
	s, path := writableStore(t, "app:\n  name: demo\n")

	if _, err := s.Set(ctx, "app", "feature", config.NewValue("on")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	_ = s.Close(ctx)

	// A fresh store over the same base+sidecar should still expose the key
	// written via Set (the sidecar is layered on top of the base on Connect).
	s2 := NewStore(path, WithWritable())
	if err := s2.Connect(ctx); err != nil {
		t.Fatalf("reconnect Connect: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close(ctx) })

	if _, err := s2.Get(ctx, "app", "feature"); err != nil {
		t.Fatalf("Get after reconnect: written key not present: %v", err)
	}
}

func TestWritable_Watch_ReceivesSetEvent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, _ := writableStore(t, "svc:\n  a: 1\n")

	ch, err := s.Watch(ctx, config.WatchFilter{})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	if _, err := s.Set(ctx, "svc", "b", config.NewValue("two")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	select {
	case evt := <-ch:
		if evt.Type != config.ChangeTypeSet {
			t.Errorf("event type = %v, want ChangeTypeSet", evt.Type)
		}
		if evt.Namespace != "svc" || evt.Key != "b" {
			t.Errorf("event ns/key = %q/%q, want svc/b", evt.Namespace, evt.Key)
		}
		if evt.Value == nil {
			t.Fatal("set event has nil value")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for set watch event")
	}
}

func TestWritable_Watch_ReceivesDeleteEvent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, _ := writableStore(t, "svc:\n  a: 1\n")

	ch, err := s.Watch(ctx, config.WatchFilter{})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	if err := s.Delete(ctx, "svc", "a"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	select {
	case evt := <-ch:
		if evt.Type != config.ChangeTypeDelete {
			t.Errorf("event type = %v, want ChangeTypeDelete", evt.Type)
		}
		if evt.Value != nil {
			t.Error("delete event should carry nil value")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for delete watch event")
	}
}

func TestWritable_Polling_PicksUpExternalEdit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Short watch interval so the poll loop reacts quickly.
	s, path := writableStore(t, "cfg:\n  k: original\n",
		WithWatchInterval(50*time.Millisecond))

	ch, err := s.Watch(ctx, config.WatchFilter{})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	// Modify the underlying base file out-of-band.
	if err := os.WriteFile(path, []byte("cfg:\n  k: changed\n"), 0o600); err != nil {
		t.Fatalf("rewrite base file: %v", err)
	}

	// The poll loop should detect the change and emit a set event.
	deadline := time.After(3 * time.Second)
	for {
		select {
		case evt := <-ch:
			if evt.Namespace == "cfg" && evt.Key == "k" && evt.Type == config.ChangeTypeSet {
				if evt.Value == nil {
					t.Fatal("poll set event has nil value")
				}
				if str, _ := evt.Value.String(); str == "changed" {
					return // success
				}
			}
		case <-deadline:
			t.Fatal("timeout waiting for poll-driven change event")
		}
	}
}

func TestWritable_Polling_GetReflectsExternalEdit(t *testing.T) {
	ctx := context.Background()
	s, path := writableStore(t, "cfg:\n  k: original\n",
		WithWatchInterval(50*time.Millisecond))

	if err := os.WriteFile(path, []byte("cfg:\n  k: updated\n"), 0o600); err != nil {
		t.Fatalf("rewrite base file: %v", err)
	}

	testutil.WaitUntil(t, 3*time.Second, func() bool {
		got, err := s.Get(ctx, "cfg", "k")
		if err != nil {
			return false
		}
		str, _ := got.String()
		return str == "updated"
	}, "Get did not reflect external file edit after polling")
}

func TestWritable_SidecarOverridesBaseFile(t *testing.T) {
	ctx := context.Background()
	s, path := writableStore(t, "cfg:\n  k: base\n")

	// Capture the base entry's version, then override via Set; the new entry's
	// version must increment, proving the sidecar entry shadows the base one.
	base0, err := s.Get(ctx, "cfg", "k")
	if err != nil {
		t.Fatalf("Get base: %v", err)
	}
	if _, err := s.Set(ctx, "cfg", "k", config.NewValue("override")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	overridden, err := s.Get(ctx, "cfg", "k")
	if err != nil {
		t.Fatalf("Get override: %v", err)
	}
	if overridden.Metadata().Version() <= base0.Metadata().Version() {
		t.Errorf("override version %d should exceed base version %d",
			overridden.Metadata().Version(), base0.Metadata().Version())
	}

	// Base file must remain pristine (the override lives in the sidecar).
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read base: %v", err)
	}
	var base map[string]map[string]any
	if err := yaml.Unmarshal(raw, &base); err != nil {
		t.Fatalf("unmarshal base: %v", err)
	}
	if base["cfg"]["k"] != "base" {
		t.Errorf("base file mutated: got %v, want pristine 'base'", base["cfg"]["k"])
	}
}

// TestWritable_SetPreservesScalarContent verifies that Set persists the value's
// content (not the Value wrapper). A prior defect encoded the wrapper via
// codec.Default().Encode, serializing it to "{}" and losing scalar content.
func TestWritable_SetPreservesScalarContent(t *testing.T) {
	ctx := context.Background()
	s, _ := writableStore(t, "db:\n  host: localhost\n")

	if _, err := s.Set(ctx, "db", "port", config.NewValue(5432)); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, err := s.Get(ctx, "db", "port")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	n, err := got.Int64()
	if err != nil {
		t.Fatalf("Int64 after Set/Get round-trip: %v", err)
	}
	if n != 5432 {
		t.Errorf("round-tripped value = %d, want 5432", n)
	}

	// A string scalar must survive too.
	if _, err := s.Set(ctx, "db", "host", config.NewValue("primary.internal")); err != nil {
		t.Fatalf("Set string: %v", err)
	}
	hv, err := s.Get(ctx, "db", "host")
	if err != nil {
		t.Fatalf("Get string: %v", err)
	}
	if hs, err := hv.String(); err != nil || hs != "primary.internal" {
		t.Errorf("string round-trip = %q (err %v), want primary.internal", hs, err)
	}
}

func TestWritable_CustomSidecarSuffix(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("a:\n  b: 1\n"), 0o600); err != nil {
		t.Fatalf("seed: %v", err)
	}
	s := NewStore(path, WithWritable(), WithSidecarSuffix(".local.yaml"))
	if err := s.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(ctx) })

	if _, err := s.Set(ctx, "a", "c", config.NewValue(2)); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, err := os.Stat(path + ".local.yaml"); err != nil {
		t.Fatalf("custom sidecar not written: %v", err)
	}
}

func TestWritable_DeleteTombstoneHidesBaseKeyAfterReconnect(t *testing.T) {
	ctx := context.Background()
	s, path := writableStore(t, "cfg:\n  k: base\n")

	if err := s.Delete(ctx, "cfg", "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_ = s.Close(ctx)

	// Reconnecting must re-apply the tombstone so the base key stays hidden.
	s2 := NewStore(path, WithWritable())
	if err := s2.Connect(ctx); err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close(ctx) })

	if _, err := s2.Get(ctx, "cfg", "k"); !config.IsNotFound(err) {
		t.Fatalf("tombstoned key visible after reconnect: %v", err)
	}
}

func TestReadOnly_SetDeleteWatchRejected(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "ro.yaml")
	if err := os.WriteFile(path, []byte("a:\n  b: 1\n"), 0o600); err != nil {
		t.Fatalf("seed: %v", err)
	}
	s := NewStore(path) // no WithWritable
	if err := s.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(ctx) })

	if _, err := s.Set(ctx, "a", "b", config.NewValue(2)); !errors.Is(err, config.ErrReadOnly) {
		t.Errorf("Set: want ErrReadOnly, got %v", err)
	}
	if err := s.Delete(ctx, "a", "b"); !errors.Is(err, config.ErrReadOnly) {
		t.Errorf("Delete: want ErrReadOnly, got %v", err)
	}
	if _, err := s.Watch(ctx, config.WatchFilter{}); !errors.Is(err, config.ErrWatchNotSupported) {
		t.Errorf("Watch: want ErrWatchNotSupported, got %v", err)
	}
}
