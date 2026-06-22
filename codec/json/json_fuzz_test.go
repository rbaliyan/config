package json

import (
	"context"
	"reflect"
	"testing"
)

// FuzzJSONCodecDecode fuzzes the JSON codec with sound oracles:
//   - Decode never panics on arbitrary bytes.
//   - Decode is deterministic: decoding the same bytes twice yields equal values.
//   - Encode never panics on a successfully decoded value.
//
// Encode/decode round-trip equality is intentionally not asserted: while
// encoding/json is well-behaved for JSON-shaped values, asserting textual
// round-trip fidelity across codecs is fragile (number/normalisation
// representation), so the oracle is kept to invariants that always hold.
func FuzzJSONCodecDecode(f *testing.F) {
	f.Add([]byte(`{"key":"value","num":42}`))
	f.Add([]byte(`null`))
	f.Add([]byte(`"hello"`))
	f.Add([]byte(`[1,2,3]`))
	f.Add([]byte(`{}`))
	f.Add([]byte(``))
	f.Add([]byte(`{{{`))
	f.Add([]byte(`{"nested":{"a":{"b":{"c":1}}}}`))
	f.Add([]byte(`{"big":123456789012345678}`))
	f.Add([]byte(`{"dup":1,"dup":2}`))
	f.Add([]byte("\xef\xbb\xbf{\"bom\":true}"))
	f.Add([]byte(`{"unicode":"é中文"}`))
	f.Add([]byte(`[[[[[[[[[[1]]]]]]]]]]`))
	f.Add([]byte(`{"float":1.5e308}`))

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

		// Encode of a decoded value must not panic.
		_, _ = c.Encode(ctx, first)
	})
}
