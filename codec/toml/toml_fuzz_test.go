package toml

import (
	"context"
	"reflect"
	"testing"
)

// FuzzTOMLCodecDecode fuzzes the TOML codec with sound oracles that do not
// depend on encode/decode round-trip equality.
//
// A decode→encode→decode equality check is NOT used here: the underlying TOML
// library legitimately normalises values (dates/times to library-specific
// types, number and whitespace canonicalisation, table-array key ordering), so
// requiring the re-encoded form to match would flag library normalisation, not
// data corruption — and that normalisation can even be host/timezone dependent.
// Instead we assert the invariants that must always hold:
//   - Decode never panics on arbitrary bytes.
//   - Decode is deterministic: decoding the same bytes twice yields equal values.
//   - Encode never panics on a successfully decoded value.
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

		// TOML only decodes into a map at the top level.
		var first map[string]any
		if err := c.Decode(ctx, data, &first); err != nil {
			return // malformed input; the only hard requirement is no panic
		}

		// Determinism: the same bytes must decode to the same value.
		var second map[string]any
		if err := c.Decode(ctx, data, &second); err != nil {
			t.Fatalf("decode non-deterministic: first succeeded, second failed: %v", err)
		}
		if !reflect.DeepEqual(first, second) {
			t.Fatalf("decode non-deterministic:\n first=%#v\n second=%#v\n(input=%q)", first, second, data)
		}

		// Encode of a decoded value must not panic (its textual form is
		// library-defined and intentionally not asserted here).
		_, _ = c.Encode(ctx, first)
	})
}
