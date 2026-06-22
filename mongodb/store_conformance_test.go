package mongodb_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/internal/storetest"
	"github.com/rbaliyan/config/mongodb"
)

// mongoConformanceSeq disambiguates the per-factory-call collection names so
// parallel subtests cannot share documents.
var mongoConformanceSeq atomic.Int64

// probeMongoOnce skips the parent test when MongoDB is unreachable. Per-subtest
// factories then assume connectivity and only connect; this avoids paying the
// 3s probe timeout once per subtest when CI runs without a mongo container.
func probeMongoOnce(t *testing.T) {
	t.Helper()
	probeCtx, probeCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer probeCancel()
	probeClient, err := mongo.Connect(options.Client().ApplyURI(getMongoURI()))
	if err != nil {
		t.Skipf("MongoDB not available: %v", err)
	}
	if err := probeClient.Ping(probeCtx, nil); err != nil {
		_ = probeClient.Disconnect(probeCtx)
		t.Skipf("MongoDB not available: %v", err)
	}
	_ = probeClient.Disconnect(probeCtx)
}

// mongoFactory builds a fresh, connected, empty MongoDB store on a
// uniquely-named collection. Assumes [probeMongoOnce] has already
// verified connectivity at the parent test level.
func mongoFactory(t *testing.T) config.Store {
	t.Helper()
	seq := mongoConformanceSeq.Add(1)
	coll := fmt.Sprintf("entries_conf_%d_%d", time.Now().UnixNano(), seq)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	client, err := mongo.Connect(options.Client().ApplyURI(getMongoURI()))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	s := mongodb.NewStore(client,
		mongodb.WithDatabase("config_test"),
		mongodb.WithCollection(coll),
	)
	if err := s.Connect(ctx); err != nil {
		_ = client.Disconnect(ctx)
		t.Fatalf("store connect: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = client.Database("config_test").Collection(coll).Drop(cleanupCtx)
		_ = s.Close(cleanupCtx)
		_ = client.Disconnect(cleanupCtx)
	})
	return s
}

// mongoVersionedFactory mirrors mongoFactory but enables snapshot
// versioning via WithVersioning(true). Assumes [probeMongoOnce] has
// already verified connectivity.
func mongoVersionedFactory(t *testing.T) config.Store {
	t.Helper()
	seq := mongoConformanceSeq.Add(1)
	coll := fmt.Sprintf("entries_conf_ver_%d_%d", time.Now().UnixNano(), seq)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	client, err := mongo.Connect(options.Client().ApplyURI(getMongoURI()))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	s := mongodb.NewStore(client,
		mongodb.WithDatabase("config_test"),
		mongodb.WithCollection(coll),
		mongodb.WithVersioning(true),
	)
	if err := s.Connect(ctx); err != nil {
		_ = client.Disconnect(ctx)
		t.Fatalf("store connect: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = client.Database("config_test").Collection(coll).Drop(cleanupCtx)
		_ = client.Database("config_test").Collection(coll + "_versions").Drop(cleanupCtx)
		_ = s.Close(cleanupCtx)
		_ = client.Disconnect(cleanupCtx)
	})
	return s
}

// TestMongoDB_StoreConformance runs the shared [config.Store] suite.
func TestMongoDB_StoreConformance(t *testing.T) {
	probeMongoOnce(t)
	storetest.RunStoreConformanceSuite(t, mongoFactory)
}

// TestMongoDB_BulkStoreConformance runs the shared [config.BulkStore]
// suite.
func TestMongoDB_BulkStoreConformance(t *testing.T) {
	probeMongoOnce(t)
	storetest.RunBulkStoreSuite(t, mongoFactory)
}

// TestMongoDB_VersionedStoreConformance runs the shared
// [config.VersionedStore] suite against a versioning-enabled MongoDB
// store. The backend-specific tests (snapshot collection layout,
// MaxHistory, CleanupOrphans, OnVersionError hook) live in
// version_store_test.go.
func TestMongoDB_VersionedStoreConformance(t *testing.T) {
	probeMongoOnce(t)
	storetest.RunVersionedStoreSuite(t, mongoVersionedFactory)
}

// TestMongoDB_WatchOrdering runs the shared watch-ordering contract over
// change streams: N sequential Sets must arrive in order on the watch
// channel. Docker-gated via probeMongoOnce.
func TestMongoDB_WatchOrdering(t *testing.T) {
	probeMongoOnce(t)
	storetest.RunWatchOrderingContract(t, mongoFactory)
}
