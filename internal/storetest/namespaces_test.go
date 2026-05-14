package storetest_test

import (
	"context"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/internal/cursor"
	"github.com/rbaliyan/config/internal/storetest"
)

// TestNamespaceListerSuite_AgainstReferenceMock runs the conformance suite
// end-to-end against a minimal in-package mock. The mock is intentionally
// a test fixture, not a production Store — it exists so the suite logic
// has coverage before any real backend lands an implementation.
func TestNamespaceListerSuite_AgainstReferenceMock(t *testing.T) {
	storetest.RunNamespaceListerSuite(t, func(t *testing.T) config.Store {
		return newMockStore()
	})
}

// mockStore is a small in-memory Store that also implements
// [config.NamespaceLister]. It exists solely to drive the conformance
// suite from this package's tests.
type mockStore struct {
	mu      sync.Mutex
	entries map[string]map[string]struct{} // namespace -> keys
}

func newMockStore() *mockStore {
	return &mockStore{entries: make(map[string]map[string]struct{})}
}

const mockBackend = "storetest-mock"

func (s *mockStore) Connect(ctx context.Context) error { return nil }
func (s *mockStore) Close(ctx context.Context) error   { return nil }

func (s *mockStore) Get(ctx context.Context, namespace, key string) (config.Value, error) {
	return nil, &config.KeyNotFoundError{Key: key, Namespace: namespace}
}

func (s *mockStore) Set(ctx context.Context, namespace, key string, value config.Value) (config.Value, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys, ok := s.entries[namespace]
	if !ok {
		keys = make(map[string]struct{})
		s.entries[namespace] = keys
	}
	keys[key] = struct{}{}
	return value, nil
}

func (s *mockStore) Delete(ctx context.Context, namespace, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys, ok := s.entries[namespace]
	if !ok {
		return &config.KeyNotFoundError{Key: key, Namespace: namespace}
	}
	delete(keys, key)
	if len(keys) == 0 {
		delete(s.entries, namespace)
	}
	return nil
}

func (s *mockStore) Find(ctx context.Context, namespace string, filter config.Filter) (config.Page, error) {
	return config.NewPage(nil, "", 0), nil
}

func (s *mockStore) Watch(ctx context.Context, filter config.WatchFilter) (<-chan config.ChangeEvent, error) {
	return nil, config.ErrWatchNotSupported
}

// ListNamespaces is the actual subject of the conformance suite. The
// implementation is deliberately straightforward: snapshot the namespace
// set, apply prefix, sort byte-wise, and keyset-paginate using a cursor
// whose payload is the last-emitted name.
//
// The mock picks the "default-fallback" branch the [config.NamespaceLister]
// contract allows for `limit <= 0` (silently coercing to 50). The suite's
// LimitZeroIsHandled subtest also accepts the "error" branch, so this
// choice is the mock's local preference, not a contractual requirement.
func (s *mockStore) ListNamespaces(ctx context.Context, prefix string, limit int, c string) ([]string, string, error) {
	if limit <= 0 {
		limit = 50
	}
	var after string
	if c != "" {
		decoded, err := cursor.UnmarshalString(mockBackend, c)
		if err != nil {
			return nil, "", err
		}
		after = decoded
	}

	s.mu.Lock()
	all := make([]string, 0, len(s.entries))
	for ns, keys := range s.entries {
		if len(keys) == 0 {
			continue
		}
		all = append(all, ns)
	}
	s.mu.Unlock()

	sort.Strings(all)

	out := make([]string, 0, limit)
	for _, ns := range all {
		if prefix != "" && !strings.HasPrefix(ns, prefix) {
			continue
		}
		if after != "" && ns <= after {
			continue
		}
		if len(out) == limit {
			next, err := cursor.MarshalString(mockBackend, out[len(out)-1])
			if err != nil {
				return nil, "", err
			}
			return out, next, nil
		}
		out = append(out, ns)
	}
	return out, "", nil
}

// Compile-time interface checks.
var (
	_ config.Store           = (*mockStore)(nil)
	_ config.NamespaceLister = (*mockStore)(nil)
)
