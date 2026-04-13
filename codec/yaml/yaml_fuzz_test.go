package yaml

import (
	"context"
	"testing"
)

func FuzzYAMLCodecDecode(f *testing.F) {
	f.Add([]byte("key: value\nnum: 42"))
	f.Add([]byte("---\n- one\n- two"))
	f.Add([]byte("anchor: &a\n  key: val\nref: *a"))
	f.Add([]byte(""))
	f.Add([]byte("{{{"))
	f.Add([]byte("null"))
	f.Add([]byte("nested:\n  a:\n    b:\n      c: 1"))
	f.Add([]byte("bool_like: yes"))

	f.Fuzz(func(t *testing.T, data []byte) {
		var v any
		_ = New().Decode(context.Background(), data, &v)
	})
}
