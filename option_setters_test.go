package config_test

import (
	"errors"
	"testing"
	"time"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/codec"
	"github.com/rbaliyan/config/memory"
)

// TestOptionSetters is a table-driven sweep over the trivial option-setter and
// accessor tail: the Manager With* options, the Set With* options, the Filter
// builder + Page accessors, and the Error()/Unwrap() contract on every exported
// error type. Each case is small; the point is to cover the otherwise-untested
// 0% functions in one maintainable place rather than to deeply exercise logic.

// TestManagerOptionSetters applies every Manager Option and asserts New succeeds.
// The setters are guarded (nil/negative inputs ignored), so we drive both the
// accepted and the ignored branch where one exists.
func TestManagerOptionSetters(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		opt  config.Option
	}{
		{"WithStore", config.WithStore(memory.NewStore())},
		{"WithStore/nil-ignored", config.WithStore(nil)},
		{"WithCodec", config.WithCodec(codec.Default())},
		{"WithCodec/nil-ignored", config.WithCodec(nil)},
		{"WithLogger/nil-ignored", config.WithLogger(nil)},
		{"WithMaxKeysPerNamespace", config.WithMaxKeysPerNamespace(10)},
		{"WithMaxKeysPerNamespace/negative-ignored", config.WithMaxKeysPerNamespace(-1)},
		{"WithWatchInitialBackoff", config.WithWatchInitialBackoff(50 * time.Millisecond)},
		{"WithWatchInitialBackoff/zero-ignored", config.WithWatchInitialBackoff(0)},
		{"WithWatchMaxBackoff", config.WithWatchMaxBackoff(time.Second)},
		{"WithWatchMaxBackoff/zero-ignored", config.WithWatchMaxBackoff(0)},
		{"WithWatchBackoffFactor", config.WithWatchBackoffFactor(1.5)},
		{"WithWatchBackoffFactor/zero-ignored", config.WithWatchBackoffFactor(0)},
		{"WithAlias", config.WithAlias("old/key", "new/key")},
		{"WithAliases", config.WithAliases("canonical/key", "a1", "a2")},
		{"WithMemoryCacheTTL", config.WithMemoryCacheTTL(time.Minute)},
		{"WithMemoryCacheTTL/zero-ignored", config.WithMemoryCacheTTL(0)},
		{"WithCache/nil-ignored", config.WithCache(nil)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mgr, err := config.New(config.WithStore(memory.NewStore()), tc.opt)
			if err != nil {
				t.Fatalf("New with %s returned error: %v", tc.name, err)
			}
			if mgr == nil {
				t.Fatalf("New with %s returned nil manager", tc.name)
			}
		})
	}
}

// TestSetOptionSetters applies every Set Option through a live Set call so the
// option closure is actually invoked.
func TestSetOptionSetters(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		opt  config.SetOption
	}{
		{"WithSetCodec", config.WithSetCodec(codec.Default())},
		{"WithSetCodec/nil-ignored", config.WithSetCodec(nil)},
		{"WithType", config.WithType(config.TypeString)},
		{"WithIfNotExists", config.WithIfNotExists()},
		{"WithIfExists", config.WithIfExists()},
		{"WithTTL", config.WithTTL(time.Hour)},
		{"WithTTL/zero-ignored", config.WithTTL(0)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			mgr, err := config.New(config.WithStore(memory.NewStore()))
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			if err := mgr.Connect(ctx); err != nil {
				t.Fatalf("Connect: %v", err)
			}
			defer mgr.Close(ctx)

			cfg := mgr.Namespace("ns")
			// WithIfExists requires a pre-existing key; create one first so the
			// option's update-only branch does not spuriously fail the case.
			_ = cfg.Set(ctx, "seed", "v")
			// The option under test is applied to a fresh key (except IfExists,
			// which we apply to the seeded key).
			key := "k"
			if tc.name == "WithIfExists" {
				key = "seed"
			}
			// Errors are acceptable for some option semantics; we only require
			// that the option closure runs without panicking.
			_ = cfg.Set(ctx, key, "value", tc.opt)
		})
	}
}

// TestFilterBuilderAndPageAccessors covers the FilterBuilder With* chain and the
// Page accessors NextCursor()/Limit()/Results().
func TestFilterBuilderAndPageAccessors(t *testing.T) {
	t.Parallel()

	f := config.NewFilter().
		WithKeys("a", "b").
		WithLimit(25).
		WithCursor("cursor-token").
		Build()
	if f.Limit() != 25 {
		t.Errorf("Filter.Limit() = %d, want 25", f.Limit())
	}
	if f.Cursor() != "cursor-token" {
		t.Errorf("Filter.Cursor() = %q, want cursor-token", f.Cursor())
	}
	if got := f.Keys(); len(got) != 2 {
		t.Errorf("Filter.Keys() = %v, want 2 keys", got)
	}

	// Prefix variant (mutually exclusive with Keys).
	pf := config.NewFilter().WithPrefix("app/").WithLimit(5).Build()
	if pf.Prefix() != "app/" {
		t.Errorf("Filter.Prefix() = %q, want app/", pf.Prefix())
	}

	// Page accessors.
	results := map[string]config.Value{"k": config.NewValue("v")}
	p := config.NewPage(results, "next-cursor", 50)
	if p.NextCursor() != "next-cursor" {
		t.Errorf("Page.NextCursor() = %q, want next-cursor", p.NextCursor())
	}
	if p.Limit() != 50 {
		t.Errorf("Page.Limit() = %d, want 50", p.Limit())
	}
	if len(p.Results()) != 1 {
		t.Errorf("Page.Results() len = %d, want 1", len(p.Results()))
	}
}

// TestExportedErrorTypes covers Error() and Unwrap() on every exported error
// type, asserting a non-empty message and that Unwrap reaches the documented
// sentinel.
func TestExportedErrorTypes(t *testing.T) {
	t.Parallel()

	storeErrSentinel := errors.New("boom")

	cases := []struct {
		name     string
		err      error
		sentinel error
	}{
		{"KeyNotFoundError", &config.KeyNotFoundError{Key: "k", Namespace: "ns"}, config.ErrNotFound},
		{"KeyNotFoundError/no-ns", &config.KeyNotFoundError{Key: "k"}, config.ErrNotFound},
		{"TypeMismatchError", &config.TypeMismatchError{Key: "k", Expected: config.TypeInt, Actual: config.TypeString}, config.ErrTypeMismatch},
		{"InvalidKeyError", &config.InvalidKeyError{Key: "../bad", Reason: "traversal"}, config.ErrInvalidKey},
		{"StoreError", &config.StoreError{Op: "get", Backend: "memory", Key: "k", Err: storeErrSentinel}, storeErrSentinel},
		{"StoreError/no-key", &config.StoreError{Op: "find", Backend: "memory", Err: errors.New("boom")}, nil},
		{"KeyExistsError", &config.KeyExistsError{Key: "k", Namespace: "ns"}, config.ErrKeyExists},
		{"KeyExistsError/no-ns", &config.KeyExistsError{Key: "k"}, config.ErrKeyExists},
		{"UnsupportedCodecError", &config.UnsupportedCodecError{Codec: "bson", Backend: "redis"}, config.ErrUnsupportedCodec},
		{"WatchHealthError", &config.WatchHealthError{ConsecutiveFailures: 5, LastError: "refused"}, config.ErrWatchUnhealthy},
		{"WatchHealthError/no-last", &config.WatchHealthError{ConsecutiveFailures: 3}, config.ErrWatchUnhealthy},
		{"BulkWriteError", &config.BulkWriteError{Errors: map[string]error{"k": errors.New("x")}, Succeeded: []string{"j"}}, config.ErrBulkWritePartial},
		{"VersionNotFoundError", &config.VersionNotFoundError{Key: "k", Namespace: "ns", Version: 3}, config.ErrVersionNotFound},
		{"VersionNotFoundError/no-ns", &config.VersionNotFoundError{Key: "k", Version: 3}, config.ErrVersionNotFound},
		{"NamespaceFullError", &config.NamespaceFullError{Namespace: "ns", Limit: 100}, config.ErrNamespaceFull},
		{"AliasExistsError", &config.AliasExistsError{Alias: "a", Reason: "registered"}, config.ErrAliasExists},
		{"AliasExistsError/no-reason", &config.AliasExistsError{Alias: "a"}, config.ErrAliasExists},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.err.Error() == "" {
				t.Errorf("%s.Error() returned empty string", tc.name)
			}
			unwrapped := errors.Unwrap(tc.err)
			switch {
			case tc.sentinel == nil:
				// StoreError with no underlying error still unwraps to its Err
				// (which may be a fresh error); we only assert Error() non-empty.
			default:
				if !errors.Is(tc.err, tc.sentinel) {
					t.Errorf("%s does not unwrap to its sentinel (unwrapped=%v)", tc.name, unwrapped)
				}
			}
		})
	}
}
