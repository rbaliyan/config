package storetest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/rbaliyan/config"
)

// RunWatchOrderingContract asserts that a backend's [config.Store.Watch]
// delivers events for sequential writes to a single key in the same order
// the writes were applied — not merely that the first event arrives.
//
// This is deliberately NOT part of [RunStoreConformanceSuite]: every
// backend's watch latency and resume semantics differ enough that the
// timing budget belongs in a separately-wired contract. Backends opt in
// from their own test package:
//
//	func TestMemory_WatchOrdering(t *testing.T) {
//	    storetest.RunWatchOrderingContract(t, memoryFactory)
//	}
//
// The contract is intentionally tolerant of two backend-specific realities:
//
//   - readiness: the watch channel may not be live the instant Watch
//     returns (postgres LISTEN, mongo change streams). The contract polls
//     for the first event before asserting order, rather than sleeping a
//     fixed amount.
//   - coalescing: a backend MAY collapse consecutive writes to the same
//     key into fewer events (last-writer-wins fan-out). The contract only
//     asserts that the events it DOES receive are a strictly increasing
//     subsequence of the write order — never out-of-order, never a
//     resurrected stale value after a newer one.
func RunWatchOrderingContract(t *testing.T, factory Factory) {
	t.Helper()

	store := factory(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ch, err := store.Watch(ctx, config.WatchFilter{Namespaces: []string{"wo"}})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	// Establish that the subscription is live before issuing the ordered
	// burst. Without this, the first few writes can land before the
	// backend's listener is registered and silently vanish — turning a
	// real ordering bug into a flaky "missing event" failure. We retry a
	// probe write until one of its events is delivered.
	if !watchReady(ctx, t, store, ch) {
		t.Fatal("watch channel never became ready (no probe event delivered)")
	}

	const n = 20
	for i := 1; i <= n; i++ {
		// Encode the sequence number as the value so the consumer can
		// recover write order from the event payload.
		if _, err := store.Set(ctx, "wo", "seq",
			config.NewValue(i, config.WithValueType(config.TypeInt))); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}

	// Collect events for "seq" until we have seen the final value (n) or
	// the deadline elapses. Assert the recovered sequence is strictly
	// increasing — a backend may coalesce, but it must never deliver an
	// older value after a newer one.
	deadline := time.Now().Add(15 * time.Second)
	var last int64
	sawFinal := false
	for time.Now().Before(deadline) && !sawFinal {
		select {
		case ev, ok := <-ch:
			if !ok {
				t.Fatal("watch channel closed before final event")
			}
			if ev.Key != "seq" || ev.Type != config.ChangeTypeSet || ev.Value == nil {
				continue
			}
			cur, err := ev.Value.Int64()
			if err != nil {
				t.Fatalf("event value not int64: %v", err)
			}
			if cur <= last {
				t.Fatalf("out-of-order event: got value %d after %d (events must be monotonic)", cur, last)
			}
			last = cur
			if cur == n {
				sawFinal = true
			}
		case <-time.After(2 * time.Second):
			// No event within the idle window; loop re-checks the deadline.
		}
	}

	if !sawFinal {
		t.Fatalf("never observed final ordered write (value %d); last seen %d", n, last)
	}
}

// watchReady issues probe writes to a throwaway key until one of its
// events is observed on ch, proving the subscription is live. Returns
// false if no probe event arrives before ctx expires. Probe events are
// drained so they cannot be mistaken for the ordered burst.
func watchReady(ctx context.Context, t *testing.T, store config.Store, ch <-chan config.ChangeEvent) bool {
	t.Helper()
	for attempt := 0; attempt < 50; attempt++ {
		probe := fmt.Sprintf("__ready_%d", attempt)
		if _, err := store.Set(ctx, "wo", probe, config.NewValue(attempt)); err != nil {
			t.Fatalf("readiness probe Set: %v", err)
		}
		select {
		case ev, ok := <-ch:
			if !ok {
				return false
			}
			if ev.Key == probe {
				return true
			}
			// An event for an earlier probe also proves liveness.
			if len(ev.Key) > len("__ready_") && ev.Key[:len("__ready_")] == "__ready_" {
				return true
			}
		case <-time.After(200 * time.Millisecond):
			// retry with a fresh probe
		case <-ctx.Done():
			return false
		}
	}
	return false
}
