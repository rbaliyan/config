package toml

import (
	"context"
	"testing"
)

// FuzzTOMLCodecDecode fuzzes the TOML codec for memory safety and robustness.
//
// The oracle is intentionally no-panic-only: Decode must not panic on arbitrary
// bytes, and a successfully decoded value must Encode without panicking. Under
// ClusterFuzzLite this runs with AddressSanitizer, so memory-corruption,
// out-of-bounds, and similar faults are also caught.
//
// Value-equality oracles (round-trip, fixed-point, or decode-determinism) are
// deliberately NOT used: the third-party TOML library legitimately normalises
// values (dates/times, number formats, whitespace, table-array ordering), and
// float values such as nan/inf make reflect.DeepEqual report inequality even
// for identical decodes (NaN != NaN). Such oracles produce false-positive
// "crashes" on library behaviour rather than real defects.
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
	f.Add([]byte("k = nan"))           // float NaN (regression: DeepEqual(NaN,NaN)==false)
	f.Add([]byte("k = inf\nm = -inf")) // float Inf

	ctx := context.Background()
	f.Fuzz(func(t *testing.T, data []byte) {
		c := New()

		var v map[string]any
		if err := c.Decode(ctx, data, &v); err != nil {
			return // malformed input; the only requirement is that Decode not panic
		}
		// A decoded value must Encode without panicking; its textual form is
		// library-defined and intentionally not asserted.
		_, _ = c.Encode(ctx, v)
	})
}
