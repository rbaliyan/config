package json

import (
	"context"
	"testing"
)

func FuzzJSONCodecDecode(f *testing.F) {
	f.Add([]byte(`{"key":"value","num":42}`))
	f.Add([]byte(`null`))
	f.Add([]byte(`"hello"`))
	f.Add([]byte(`[1,2,3]`))
	f.Add([]byte(`{}`))
	f.Add([]byte(``))
	f.Add([]byte(`{{{`))
	f.Add([]byte(`{"nested":{"a":{"b":{"c":1}}}}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		var v any
		_ = New().Decode(context.Background(), data, &v)
	})
}
