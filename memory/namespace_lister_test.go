package memory_test

import (
	"context"
	"testing"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/internal/storetest"
	"github.com/rbaliyan/config/memory"
)

// TestMemory_NamespaceListerConformance runs the shared [storetest] suite
// against the memory store. Memory is the spec's reference implementation
// for the [config.NamespaceLister] contract — passing this suite is the
// gating signal for adding similar tests to every other backend.
func TestMemory_NamespaceListerConformance(t *testing.T) {
	storetest.RunNamespaceListerSuite(t, func(t *testing.T) config.Store {
		s := memory.NewStore()
		ctx := context.Background()
		if err := s.Connect(ctx); err != nil {
			t.Fatalf("memory.Connect: %v", err)
		}
		t.Cleanup(func() { _ = s.Close(ctx) })
		return s
	})
}
