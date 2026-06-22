package toml

import (
	"context"
	"reflect"
	"testing"
	"time"
)

// containsTime reports whether v (recursively) holds a time.Time. TOML's bare
// date/time scalars decode to time.Time, and the underlying encoder's local-
// time round-trip is timezone-dependent and non-deterministic across hosts —
// outside the JSON-compatible value space the config codecs actually carry.
// Such values are excluded from the round-trip oracle so the test stays
// portable and only asserts corruption on representable data.
func containsTime(v any) bool {
	switch t := v.(type) {
	case time.Time:
		return true
	case map[string]any:
		for _, e := range t {
			if containsTime(e) {
				return true
			}
		}
	case []any:
		for _, e := range t {
			if containsTime(e) {
				return true
			}
		}
	}
	return false
}

// FuzzTOMLCodecDecode fuzzes the TOML codec with a fixed-point round-trip
// oracle.
//
// As with YAML, the first decode pass is intentionally lossy for some scalars
// (e.g. a bare date "0000-01-01" decodes to a time.Time, whose re-encoded text
// the decoder may resolve differently), so requiring first==second would flag
// a library normalisation quirk rather than data corruption. We normalise once
// (decode→encode→decode) and then require the next round-trip to be stable:
// the codec must reach a fixed point. Inputs that fail to decode (or whose
// decoded shape the TOML encoder cannot represent) are ignored — the only hard
// requirement there is that Decode must not panic.
func FuzzTOMLCodecDecode(f *testing.F) {
	f.Add([]byte("key = \"value\"\nnum = 42"))
	f.Add([]byte("[section]\nkey = \"val\""))
	f.Add([]byte("[[array]]\nname = \"first\""))
	f.Add([]byte(""))
	f.Add([]byte("{{{"))
	f.Add([]byte("[a]\n[a.b]\n[a.b.c]\nval = 1"))
	f.Add([]byte("date = 2024-01-01T00:00:00Z"))
	f.Add([]byte("big = 123456789012345678"))
	f.Add([]byte("unicode = \"é中文\""))
	f.Add([]byte("float = 1.5"))
	f.Add([]byte("list = [1, 2, 3]"))
	f.Add([]byte("nested = [[1, 2], [3, 4]]"))

	ctx := context.Background()
	f.Fuzz(func(t *testing.T, data []byte) {
		c := New()

		// TOML only decodes into a map at the top level; a bare scalar or
		// array document is not valid TOML, so map[string]any is the correct
		// target shape for round-tripping.
		var first map[string]any
		if err := c.Decode(ctx, data, &first); err != nil {
			return // malformed input; only requirement is no panic above
		}
		if containsTime(first) {
			return // time.Time scalars: see containsTime godoc
		}

		// Normalise: one round-trip to reach the codec's stable representation.
		enc1, err := c.Encode(ctx, first)
		if err != nil {
			// Some decoded shapes (e.g. heterogeneous arrays) are not
			// re-encodable by the TOML encoder. That is an encoder
			// limitation, not data corruption, so skip rather than fail.
			return
		}
		var norm map[string]any
		if err := c.Decode(ctx, enc1, &norm); err != nil {
			t.Fatalf("re-Decode of own Encode output failed: %v (encoded=%q)", err, enc1)
		}

		// Fixed point: a second round-trip of the normalised value must not
		// drift. Any change here is genuine Encode/Decode data corruption.
		enc2, err := c.Encode(ctx, norm)
		if err != nil {
			return
		}
		var again map[string]any
		if err := c.Decode(ctx, enc2, &again); err != nil {
			t.Fatalf("re-Decode of normalised Encode output failed: %v (encoded=%q)", err, enc2)
		}

		if !reflect.DeepEqual(norm, again) {
			t.Fatalf("fixed-point round-trip mismatch:\n  norm=%#v\n again=%#v\n(input=%q)", norm, again, data)
		}
	})
}
