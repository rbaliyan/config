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

// nsListerSeq disambiguates per-factory-call collection names so parallel
// subtests of the conformance suite cannot accidentally share documents.
var nsListerSeq atomic.Int64

// TestMongoDB_NamespaceListerConformance runs the shared [storetest] suite
// against the MongoDB store. Each subtest gets its own collection so the
// scenarios stay independent under t.Parallel().
func TestMongoDB_NamespaceListerConformance(t *testing.T) {
	// Probe once so we report "MongoDB not available" cleanly instead of
	// failing every subtest with the same connection error.
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

	storetest.RunNamespaceListerSuite(t, func(t *testing.T) config.Store {
		seq := nsListerSeq.Add(1)
		coll := fmt.Sprintf("entries_nstest_%d_%d", time.Now().UnixNano(), seq)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
	})
}
