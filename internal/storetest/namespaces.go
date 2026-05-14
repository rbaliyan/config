// Package storetest provides shared conformance suites for the optional
// store interfaces declared in package config (e.g. [config.NamespaceLister],
// with more to follow). Backends that opt in to an optional interface call
// the matching Run*Suite function from their own test package; the suite
// drives the contract against the backend.
//
// The suites are scoped to optional interfaces: a backend that does not
// implement the interface under test will see [testing.T.Fatalf] from the
// suite's setup helper, because there is nothing meaningful to test.
// Backends that have not yet opted in simply do not call the suite.
//
// To use, each backend writes a small adapter test that supplies a factory
// returning a fresh, connected, empty store, then calls into the suite:
//
//	func TestMemory_NamespaceLister(t *testing.T) {
//	    storetest.RunNamespaceListerSuite(t, func(t *testing.T) config.Store {
//	        s := memory.NewStore()
//	        ctx := context.Background()
//	        if err := s.Connect(ctx); err != nil {
//	            t.Fatalf("connect: %v", err)
//	        }
//	        t.Cleanup(func() { _ = s.Close(ctx) })
//	        return s
//	    })
//	}
//
// Each backend gets identical contract coverage; bugs surface in the
// backend that owns the regression rather than in test code that
// drifted out of date.
package storetest

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/rbaliyan/config"
)

// Factory builds a fresh, connected, empty [config.Store] for a single
// subtest. Backends register any teardown via [testing.T.Cleanup] so the
// suite never has to know how to close the underlying resources.
type Factory func(t *testing.T) config.Store

// seedKey is the placeholder key inserted into every conformance namespace.
// The literal "__storetest_seed" satisfies the key-validation regex
// (alphanumeric / underscore / dot / dash / slash) and is intentionally
// uncommon so it does not collide with real configuration data when a
// suite is run against a shared backend.
const seedKey = "__storetest_seed"

// RunNamespaceListerSuite executes the full [config.NamespaceLister]
// conformance contract against a backend. Each subtest gets a fresh store
// from factory so they remain independent and parallelisable.
func RunNamespaceListerSuite(t *testing.T, factory Factory) {
	t.Helper()

	t.Run("EmptyStoreReturnsEmptyPage", func(t *testing.T) {
		t.Parallel()
		lister := newLister(t, factory)
		names, next, err := lister.ListNamespaces(context.Background(), "", 10, "")
		if err != nil {
			t.Fatalf("ListNamespaces: %v", err)
		}
		if len(names) != 0 {
			t.Errorf("empty store returned %d namespaces: %v", len(names), names)
		}
		if next != "" {
			t.Errorf("empty store returned non-empty cursor %q", next)
		}
	})

	t.Run("SinglePageHasNoCursor", func(t *testing.T) {
		t.Parallel()
		store := factory(t)
		ctx := context.Background()
		seed(t, store, "alpha", "beta", "gamma")

		names, next, err := mustList(t, store).ListNamespaces(ctx, "", 10, "")
		if err != nil {
			t.Fatalf("ListNamespaces: %v", err)
		}
		assertSortedEqual(t, names, []string{"alpha", "beta", "gamma"})
		if next != "" {
			t.Errorf("single-page result returned non-empty cursor %q", next)
		}
	})

	t.Run("MultiPageOrderingAndTermination", func(t *testing.T) {
		t.Parallel()
		store := factory(t)
		ctx := context.Background()
		want := []string{"a", "b", "c", "d", "e", "f", "g"}
		seed(t, store, want...)

		lister := mustList(t, store)
		got, err := paginate(ctx, lister, "", 2)
		if err != nil {
			t.Fatalf("paginate: %v", err)
		}
		assertSortedEqual(t, got, want)
	})

	t.Run("PrefixFilter", func(t *testing.T) {
		t.Parallel()
		store := factory(t)
		ctx := context.Background()
		// Namespace names obey config.ValidateNamespace: alphanumeric plus
		// the connector chars [_.-:]. Slash is NOT permitted, so the
		// suite uses dot-separated hierarchical names.
		seed(t, store, "prod.us", "prod.eu", "stage.us", "dev")

		lister := mustList(t, store)
		got, err := paginate(ctx, lister, "prod.", 10)
		if err != nil {
			t.Fatalf("paginate: %v", err)
		}
		assertSortedEqual(t, got, []string{"prod.eu", "prod.us"})
	})

	t.Run("PrefixMatchesNothing", func(t *testing.T) {
		t.Parallel()
		store := factory(t)
		ctx := context.Background()
		seed(t, store, "alpha", "beta")

		names, next, err := mustList(t, store).ListNamespaces(ctx, "zzz-missing", 10, "")
		if err != nil {
			t.Fatalf("ListNamespaces: %v", err)
		}
		if len(names) != 0 {
			t.Errorf("expected empty result for non-matching prefix, got %v", names)
		}
		if next != "" {
			t.Errorf("expected empty cursor, got %q", next)
		}
	})

	t.Run("PrefixWithPagination", func(t *testing.T) {
		t.Parallel()
		store := factory(t)
		ctx := context.Background()
		seed(t, store,
			"prod-1", "prod-2", "prod-3", "prod-4", "prod-5",
			"stage-1", "stage-2",
		)
		lister := mustList(t, store)
		got, err := paginate(ctx, lister, "prod-", 2)
		if err != nil {
			t.Fatalf("paginate: %v", err)
		}
		assertSortedEqual(t, got, []string{"prod-1", "prod-2", "prod-3", "prod-4", "prod-5"})
	})

	t.Run("InvalidCursorReturnsErrInvalidCursor", func(t *testing.T) {
		t.Parallel()
		store := factory(t)
		ctx := context.Background()
		seed(t, store, "alpha", "beta")

		_, _, err := mustList(t, store).ListNamespaces(ctx, "", 10, "!!! definitely not a real cursor !!!")
		if err == nil {
			t.Fatal("expected error for malformed cursor")
		}
		if !config.IsInvalidCursor(err) {
			t.Errorf("expected ErrInvalidCursor, got %v (%T)", err, err)
		}
	})

	t.Run("ByteWiseSortOrder", func(t *testing.T) {
		t.Parallel()
		store := factory(t)
		ctx := context.Background()
		// Byte-wise UTF-8 sort puts uppercase before lowercase
		// ('A' = 0x41 < 'a' = 0x61), digits before letters
		// ('0' = 0x30 < 'A'), and dots before dashes ('.' = 0x2E,
		// '-' = 0x2D, so '-' actually sorts BEFORE '.'). The test
		// inputs intentionally span those ranges so a backend that
		// uses locale-aware collation (e.g. Postgres' default
		// `varchar` LC_COLLATE) will fail loudly.
		//
		// Namespace names obey config.ValidateNamespace, which excludes
		// non-ASCII bytes. The byte-wise contract is therefore
		// exercised exclusively through ASCII variants.
		input := []string{"Zebra", "abc", "ABC", "a.b", "a-b", "10", "9"}
		for _, s := range input {
			if !utf8.ValidString(s) {
				t.Fatalf("test input %q is not valid UTF-8", s)
			}
		}
		seed(t, store, input...)

		got, err := paginate(ctx, mustList(t, store), "", 10)
		if err != nil {
			t.Fatalf("paginate: %v", err)
		}
		// Byte-wise sort the expected list for comparison.
		want := slices.Clone(input)
		sort.Strings(want)
		if !slices.Equal(got, want) {
			t.Errorf("byte-wise sort mismatch:\n got:  %v\n want: %v", got, want)
		}
	})

	t.Run("ConcurrentWritesProduceNoDuplicatesWithinPage", func(t *testing.T) {
		t.Parallel()
		store := factory(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Pre-seed a baseline so the lister always has work to do.
		baseline := make([]string, 20)
		for i := range baseline {
			baseline[i] = paddedName("base", i)
		}
		seed(t, store, baseline...)

		lister := mustList(t, store)

		// Bounded background writer: enough churn to widen the race window
		// without out-running the reader's pagination indefinitely. The
		// suite's invariant (no duplicate within a single page) holds for
		// any write pattern, so we don't need the writer to run forever.
		const writes = 200
		var wg sync.WaitGroup
		wg.Go(func() {
			for i := range writes {
				if ctx.Err() != nil {
					return
				}
				ns := paddedName("live", i)
				_, _ = store.Set(ctx, ns, seedKey, config.NewValue("seed"))
			}
		})

		// While the writer is active, paginate full sweeps and assert the
		// per-page uniqueness invariant. We bound each sweep so a slow or
		// adversarial backend cannot hang the test, and we track the
		// total number of pages observed so a backend that returns the
		// entire baseline in one page (ignoring the requested limit) is
		// caught — "didn't hang" is not the same as "actually paginated."
		const maxPagesPerSweep = 256
		const pageLimit = 5
		// With a 20-entry baseline at limit=5, each sweep must walk at
		// least 20/5 = 4 pages plus the terminal empty-cursor page = 5
		// pages. Across 8 rounds, expect ≥ 8*5 = 40 pages observed.
		// We assert >= baselinePages*rounds to leave slack for the
		// writer's adds skewing later sweeps to even more pages. Keyed
		// off the actual baseline length so resizing the seed slice
		// above doesn't silently weaken the assertion.
		minPagesObserved := (len(baseline) / pageLimit) * 8
		totalPagesObserved := 0
		for round := range 8 {
			cursor := ""
			for page := range maxPagesPerSweep {
				if ctx.Err() != nil {
					t.Fatalf("context expired during round %d, page %d: %v", round, page, ctx.Err())
				}
				names, next, err := lister.ListNamespaces(ctx, "", pageLimit, cursor)
				if err != nil {
					t.Fatalf("ListNamespaces page (round %d): %v", round, err)
				}
				if !areUnique(names) {
					t.Fatalf("duplicate namespace within a single page (round %d): %v", round, names)
				}
				totalPagesObserved++
				if next == "" {
					break
				}
				cursor = next
			}
		}
		if totalPagesObserved < minPagesObserved {
			t.Fatalf("observed %d pages across 8 sweeps; expected ≥ %d. "+
				"A backend that ignores limit and returns everything in one page would pass "+
				"the no-duplicate check but fail this assertion.",
				totalPagesObserved, minPagesObserved)
		}
		wg.Wait()
	})

	t.Run("CursorRoundTripsAcrossPageSizes", func(t *testing.T) {
		t.Parallel()
		store := factory(t)
		ctx := context.Background()
		want := []string{
			"alpha", "beta", "delta", "epsilon",
			"gamma", "iota", "kappa", "lambda",
		}
		seed(t, store, want...)

		lister := mustList(t, store)
		for _, limit := range []int{1, 2, 3, 5, len(want), len(want) + 1} {
			got, err := paginate(ctx, lister, "", limit)
			if err != nil {
				t.Fatalf("paginate limit=%d: %v", limit, err)
			}
			assertSortedEqual(t, got, want)
		}
	})

	t.Run("LimitZeroIsHandled", func(t *testing.T) {
		t.Parallel()
		store := factory(t)
		ctx := context.Background()
		seed(t, store, "alpha", "beta", "gamma")

		// The contract permits backends to treat `limit <= 0` either as
		// a caller error (returning a non-nil err) or as a backend-default
		// signal (returning ok with whatever default they document). The
		// only mechanical guarantee this suite can enforce is "MUST NOT
		// panic" — and, if the backend returns successfully, the page
		// invariants still hold.
		//
		// Backends that error MUST document the error mode; the suite
		// cannot verify that obligation, so the doc-level requirement
		// lives in the interface godoc, not in this assertion.
		names, _, err := mustList(t, store).ListNamespaces(ctx, "", 0, "")
		if err != nil {
			// Erroring is permitted; the backend just opted out of the
			// default-fallback behaviour. No further invariants to check.
			return
		}
		// Backend returned successfully — apply universal invariants.
		if !areUnique(names) {
			t.Errorf("limit=0 result contained duplicates: %v", names)
		}
	})

	t.Run("CursorIsOpaque", func(t *testing.T) {
		t.Parallel()
		store := factory(t)
		ctx := context.Background()
		// Pre-seed enough entries to force a non-terminal page so we
		// definitely get a non-empty cursor back.
		base := []string{"a-one", "b-two", "c-three", "d-four", "e-five", "f-six"}
		seed(t, store, base...)

		names, next, err := mustList(t, store).ListNamespaces(ctx, "", 2, "")
		if err != nil {
			t.Fatalf("ListNamespaces: %v", err)
		}
		if next == "" {
			t.Fatal("setup error: expected non-empty cursor with limit=2 and 6 seeded namespaces")
		}
		// Cursors are opaque per the contract. A backend that just
		// returns the last-emitted name as the cursor leaks an
		// implementation detail and breaks the "non-portable, opaque"
		// guarantee callers rely on. The cheapest enforceable test:
		// the cursor must NOT equal any name on the page that
		// produced it.
		for _, n := range names {
			if next == n {
				t.Fatalf("cursor %q matches a name on the page that produced it (%v); "+
					"contract requires cursors to be opaque tokens, not raw last-emitted values", next, names)
			}
		}
	})
}

// newLister fetches a fresh store from factory and asserts that it
// satisfies [config.NamespaceLister]. Subtests that do not need to
// pre-seed data use this directly to keep their bodies short.
func newLister(t *testing.T, factory Factory) config.NamespaceLister {
	t.Helper()
	return mustList(t, factory(t))
}

// mustList asserts that store implements [config.NamespaceLister]. Backends
// that fail this check have not opted in to the optional interface, so the
// suite cannot run against them.
func mustList(t *testing.T, store config.Store) config.NamespaceLister {
	t.Helper()
	nl, ok := store.(config.NamespaceLister)
	if !ok {
		t.Fatalf("store %T does not implement config.NamespaceLister", store)
	}
	return nl
}

// seed inserts a placeholder entry into each namespace so the lister has
// something to enumerate. Any failure here is a test setup error, not a
// conformance failure.
func seed(t *testing.T, store config.Store, namespaces ...string) {
	t.Helper()
	ctx := context.Background()
	for _, ns := range namespaces {
		if _, err := store.Set(ctx, ns, seedKey, config.NewValue("seed")); err != nil {
			t.Fatalf("seed namespace %q: %v", ns, err)
		}
	}
}

// paginate exhausts the cursor sequence and returns all collected names.
// It also enforces the universal invariants — no per-page duplicates,
// pages never exceed the requested limit, terminal page has an empty
// cursor — so individual subtests stay focused on their property.
func paginate(ctx context.Context, lister config.NamespaceLister, prefix string, limit int) ([]string, error) {
	var (
		all    []string
		cursor string
	)
	for safety := 0; ; safety++ {
		if safety > 1024 {
			return nil, &paginationLoopError{}
		}
		names, next, err := lister.ListNamespaces(ctx, prefix, limit, cursor)
		if err != nil {
			return nil, err
		}
		if len(names) > limit {
			return nil, &pageTooLargeError{got: len(names), limit: limit}
		}
		if !areUnique(names) {
			return nil, &duplicateInPageError{names: names}
		}
		all = append(all, names...)
		if next == "" {
			return all, nil
		}
		cursor = next
	}
}

func assertSortedEqual(t *testing.T, got, want []string) {
	t.Helper()
	want = slices.Clone(want)
	sort.Strings(want)
	if !slices.Equal(got, want) {
		t.Errorf("results mismatch:\n got:  %v\n want: %v", got, want)
	}
}

func areUnique(names []string) bool {
	seen := make(map[string]struct{}, len(names))
	for _, n := range names {
		if _, dup := seen[n]; dup {
			return false
		}
		seen[n] = struct{}{}
	}
	return true
}

// paddedName produces a fixed-width name so lexicographic order matches
// numeric order — important for deterministic baselines in concurrent
// tests where we want predictable ranges.
func paddedName(prefix string, i int) string {
	return fmt.Sprintf("%s-%05d", prefix, i)
}

// paginationLoopError signals a runaway loop in [paginate], typically caused
// by a backend that never returns an empty cursor.
type paginationLoopError struct{}

func (*paginationLoopError) Error() string {
	return "storetest: pagination did not terminate after 1024 pages"
}

// pageTooLargeError signals a backend that returned more results than the
// caller-requested limit.
type pageTooLargeError struct {
	got, limit int
}

func (e *pageTooLargeError) Error() string {
	return fmt.Sprintf("storetest: page returned %d entries, exceeding limit of %d", e.got, e.limit)
}

// duplicateInPageError reports a page that contains the same namespace name
// more than once.
type duplicateInPageError struct {
	names []string
}

func (e *duplicateInPageError) Error() string {
	return "storetest: page contains duplicate namespace names"
}
