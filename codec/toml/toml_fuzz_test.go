package toml

import "testing"

func FuzzTOMLCodecDecode(f *testing.F) {
	f.Add([]byte("key = \"value\"\nnum = 42"))
	f.Add([]byte("[section]\nkey = \"val\""))
	f.Add([]byte("[[array]]\nname = \"first\""))
	f.Add([]byte(""))
	f.Add([]byte("{{{"))
	f.Add([]byte("[a]\n[a.b]\n[a.b.c]\nval = 1"))
	f.Add([]byte("date = 2024-01-01T00:00:00Z"))

	f.Fuzz(func(t *testing.T, data []byte) {
		var v any
		_ = New().Decode(data, &v)
	})
}
