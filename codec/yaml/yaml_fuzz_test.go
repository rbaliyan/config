package yaml

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

// yamlUnsafe reports whether v (recursively) sits outside the JSON-compatible
// value space the config codec layer actually carries, in a way that makes
// gopkg.in/yaml.v3's encode step non-idempotent. Two upstream quirks are
// excluded so the fixed-point oracle stays strong on realistic config data
// while remaining portable:
//
//   - Whitespace-folded strings: scalars holding newlines or leading/trailing
//     whitespace fold non-idempotently on encode (e.g. "\n\n\n" -> "\n" -> "").
//   - Non-string map keys: yaml.v3 decodes mappings into map[any]any with
//     typed keys (int 0, "0", true, 1.5), which can collide into a duplicate
//     key on re-encode and then fail to re-decode. Config values are always
//     string-keyed (JSON-shaped), so these are not representative.
func yamlUnsafe(v any) bool {
	switch t := v.(type) {
	case string:
		return strings.ContainsAny(t, "\n\r") || t != strings.TrimSpace(t)
	case map[string]any:
		for k, e := range t {
			if yamlUnsafe(k) || yamlUnsafe(e) {
				return true
			}
		}
	case map[any]any:
		return true // non-string keys: see godoc
	case []any:
		for _, e := range t {
			if yamlUnsafe(e) {
				return true
			}
		}
	}
	return false
}

// FuzzYAMLCodecDecode fuzzes the YAML codec with a re-encode/re-decode oracle.
//
// The oracle is a fixed-point check rather than a strict first==second
// equality. YAML's untyped scalar resolution is intentionally lossy on the
// first pass (e.g. "08" is an invalid octal and decodes as a string, but its
// re-encoded form "8" decodes as an int), so requiring first==second would
// flag a library normalisation quirk rather than data corruption. Instead we
// normalise once (decode→encode→decode) and then require that a further
// round-trip is stable: the codec must reach a fixed point and stay there. A
// drift at that stage is genuine corruption. Inputs that fail to decode are
// ignored — the only requirement there is that Decode must not panic.
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
			return // malformed input; only requirement is no panic above
		}
		if yamlUnsafe(first) {
			return // non-idempotent yaml.v3 shapes: see yamlUnsafe godoc
		}

		// Normalise: one round-trip to reach the codec's stable representation.
		enc1, err := c.Encode(ctx, first)
		if err != nil {
			t.Fatalf("Encode failed for value decoded from %q: %v", data, err)
		}
		var norm any
		if err := c.Decode(ctx, enc1, &norm); err != nil {
			t.Fatalf("re-Decode of own Encode output failed: %v (encoded=%q)", err, enc1)
		}

		// Fixed point: a second round-trip of the normalised value must not
		// drift. Any change here is genuine Encode/Decode data corruption.
		enc2, err := c.Encode(ctx, norm)
		if err != nil {
			t.Fatalf("Encode of normalised value failed: %v", err)
		}
		var again any
		if err := c.Decode(ctx, enc2, &again); err != nil {
			t.Fatalf("re-Decode of normalised Encode output failed: %v (encoded=%q)", err, enc2)
		}

		if !reflect.DeepEqual(norm, again) {
			t.Fatalf("fixed-point round-trip mismatch:\n  norm=%#v\n again=%#v\n(input=%q)", norm, again, data)
		}
	})
}
