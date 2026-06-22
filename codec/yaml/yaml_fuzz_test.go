package yaml

import (
	"context"
	"reflect"
	"testing"
)

// FuzzYAMLCodecDecode fuzzes the YAML codec with sound oracles that do not
// depend on encode/decode round-trip equality.
//
// A decode→encode→decode equality check is NOT used: gopkg.in/yaml.v3
// legitimately normalises values (untyped scalar resolution like "08"→"8",
// whitespace folding, typed map keys), so requiring the re-encoded form to
// match would flag library normalisation rather than data corruption. Instead
// we assert the invariants that must always hold:
//   - Decode never panics on arbitrary bytes.
//   - Decode is deterministic: decoding the same bytes twice yields equal values.
//   - Encode never panics on a successfully decoded value.
func FuzzYAMLCodecDecode(f *testing.F) {
	f.Add([]byte("key: value\nnum: 42"))
	f.Add([]byte("---\n- one\n- two"))
	f.Add([]byte("anchor: &a\n  key: val\nref: *a"))
	f.Add([]byte(""))
	f.Add([]byte("{{{"))
	f.Add([]byte("null"))
	f.Add([]byte("nested:\n  a:\n    b:\n      c: 1"))
	f.Add([]byte("bool_like: yes"))
	f.Add([]byte("big: 123456789012345678"))
	f.Add([]byte("unicode: \"é中文\""))
	f.Add([]byte("dup: 1\ndup: 2"))
	f.Add([]byte("deep:\n  a:\n    b:\n      c:\n        d:\n          e: 1"))
	f.Add([]byte("list:\n  - a\n  - b\n  - c"))
	f.Add([]byte("float: 1.5e308"))

	ctx := context.Background()
	f.Fuzz(func(t *testing.T, data []byte) {
		c := New()

		var first any
		if err := c.Decode(ctx, data, &first); err != nil {
			return // malformed input; the only hard requirement is no panic
		}

		// Determinism: the same bytes must decode to the same value.
		var second any
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
