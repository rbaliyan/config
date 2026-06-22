package json

import (
	"context"
	"reflect"
	"testing"
)

// FuzzJSONCodecDecode fuzzes the JSON codec with a re-encode/re-decode oracle.
//
// On any input that decodes successfully, the decoded value is re-encoded and
// decoded a second time. The two decoded values must be deeply equal: if they
// differ, the Encode/Decode pair is silently corrupting data (losing fields,
// reordering in a way that changes semantics, mangling numbers, etc.). Inputs
// that fail to decode are ignored — the only hard requirement there is that
// Decode must not panic.
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
			return // malformed input; only requirement is no panic above
		}

		// Round-trip: re-encode the decoded value, then decode again. The
		// second decode must reproduce the first exactly.
		encoded, err := c.Encode(ctx, first)
		if err != nil {
			t.Fatalf("Encode failed for value decoded from %q: %v", data, err)
		}

		var second any
		if err := c.Decode(ctx, encoded, &second); err != nil {
			t.Fatalf("re-Decode of own Encode output failed: %v (encoded=%q)", err, encoded)
		}

		if !reflect.DeepEqual(first, second) {
			t.Fatalf("round-trip mismatch:\n first=%#v\nsecond=%#v\n(input=%q encoded=%q)", first, second, data, encoded)
		}
	})
}
