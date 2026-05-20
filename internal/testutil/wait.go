// Package testutil collects small helpers shared across the repository's
// test suites. The goal is to keep individual test files focused on the
// assertion they own rather than re-implementing wait/retry/probe loops
// that flake under load.
package testutil

import (
	"testing"
	"time"
)

// WaitUntil polls pred until it returns true or timeout elapses. It uses
// a short interior sleep (5ms) so the wakeup latency stays low even under
// a loaded CI runner, and reports a clear failure via [testing.T.Fatalf]
// when the predicate never satisfies.
//
// Use WaitUntil to replace bare `time.Sleep(d)` followed by a "was the
// goroutine quick enough?" check. The classic flake pattern looks like:
//
//	_ = store.Set(ctx, "ns", "k", v)
//	time.Sleep(100 * time.Millisecond) // racy
//	if store.DroppedEvents() == 0 {
//	    t.Error("expected drops")
//	}
//
// Replace it with:
//
//	_ = store.Set(ctx, "ns", "k", v)
//	testutil.WaitUntil(t, 2*time.Second,
//	    func() bool { return store.DroppedEvents() > 0 },
//	    "expected dropped events after overflowing buffer")
//
// The bound is now an upper limit; the test exits as soon as the
// condition holds.
func WaitUntil(t *testing.T, timeout time.Duration, pred func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pred() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("waitUntil timed out after %s: %s", timeout, msg)
}

// WaitFor is the non-fatal variant of [WaitUntil]: it returns true on
// success and false on timeout, leaving the test to decide whether the
// failure is fatal. Use this when the caller wants to log additional
// state on timeout before failing, or when a soft probe (e.g. "is the
// pub/sub subscription live yet?") should not by itself terminate the
// test.
func WaitFor(timeout time.Duration, pred func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pred() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}
