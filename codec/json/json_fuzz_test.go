package json

import (
	"context"
	"testing"
)

// FuzzJSONCodecDecode fuzzes the JSON codec for memory safety and robustness.
//
// The oracle is intentionally no-panic-only: Decode must not panic on arbitrary
// bytes, and a successfully decoded value must Encode without panicking. Under
// ClusterFuzzLite this runs with AddressSanitizer, so memory-corruption faults
// are also caught.
//
// Value-equality oracles are deliberately NOT used here, for consistency with
// the YAML/TOML codec targets where such oracles are unsound (library
// normalisation and NaN != NaN under reflect.DeepEqual).
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

		var v any
		if err := c.Decode(ctx, data, &v); err != nil {
			return // malformed input; the only requirement is that Decode not panic
		}
		// A decoded value must Encode without panicking.
		_, _ = c.Encode(ctx, v)
	})
}
