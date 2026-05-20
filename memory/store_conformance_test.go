package memory_test

import (
	"context"
	"testing"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/internal/storetest"
	"github.com/rbaliyan/config/memory"
)

// memoryFactory builds a fresh, connected, empty memory store for the
// shared storetest suites. Each subtest gets its own store; t.Cleanup
// closes it.
func memoryFactory(t *testing.T) config.Store {
	t.Helper()
	s := memory.NewStore()
	ctx := context.Background()
	if err := s.Connect(ctx); err != nil {
		t.Fatalf("memory.Connect: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(ctx) })
	return s
}

// TestMemory_StoreConformance runs the shared [config.Store] suite.
// Memory is the spec's reference implementation; this is the gating
// signal for shipping the same suite against SQL/document backends.
func TestMemory_StoreConformance(t *testing.T) {
	storetest.RunStoreConformanceSuite(t, memoryFactory)
}

// TestMemory_BulkStoreConformance runs the shared [config.BulkStore] suite.
func TestMemory_BulkStoreConformance(t *testing.T) {
	storetest.RunBulkStoreSuite(t, memoryFactory)
}

// TestMemory_VersionedStoreConformance runs the shared
// [config.VersionedStore] suite. Memory is the reference VersionedStore
// implementation today.
func TestMemory_VersionedStoreConformance(t *testing.T) {
	storetest.RunVersionedStoreSuite(t, memoryFactory)
}
