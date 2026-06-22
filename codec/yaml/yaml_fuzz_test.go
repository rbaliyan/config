package yaml

import (
	"context"
	"testing"
)

// FuzzYAMLCodecDecode fuzzes the YAML codec for memory safety and robustness.
//
// The oracle is intentionally no-panic-only: Decode must not panic on arbitrary
// bytes, and a successfully decoded value must Encode without panicking. Under
// ClusterFuzzLite this runs with AddressSanitizer, so memory-corruption faults
// are also caught.
//
// Value-equality oracles (round-trip, fixed-point, or decode-determinism) are
// deliberately NOT used: gopkg.in/yaml.v3 legitimately normalises values
// (untyped scalar resolution, whitespace folding, typed map keys), and float
// values such as .nan/.inf make reflect.DeepEqual report inequality even for
// identical decodes (NaN != NaN). Such oracles produce false-positive
// "crashes" on library behaviour rather than real defects.
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
	f.Add([]byte("k: .nan"))            // float NaN (regression: DeepEqual(NaN,NaN)==false)
	f.Add([]byte("k: .inf\nm: -.inf")) // float Inf

	ctx := context.Background()
	f.Fuzz(func(t *testing.T, data []byte) {
		c := New()

		var v any
		if err := c.Decode(ctx, data, &v); err != nil {
			return // malformed input; the only requirement is that Decode not panic
		}
		// A decoded value must Encode without panicking; its textual form is
		// library-defined and intentionally not asserted.
		_, _ = c.Encode(ctx, v)
	})
}
