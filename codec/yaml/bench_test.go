package yaml_test

import (
	"context"
	"strconv"
	"testing"

	cyaml "github.com/rbaliyan/config/codec/yaml"
)

// payloads returns deterministic encode targets at three sizes:
// small (a few scalars), medium (nested struct-like map), and large
// (a slice of records). No randomness, so runs are comparable over time.
func payloads() map[string]any {
	medium := map[string]any{
		"service":  "auth",
		"replicas": 3,
		"db": map[string]any{
			"host": "db.internal", "port": 5432, "tls": true, "pool": 16,
		},
		"flags": []any{"a", "b", "c"},
	}

	const records = 200
	large := make([]any, 0, records)
	for i := 0; i < records; i++ {
		large = append(large, map[string]any{
			"id":      i,
			"name":    "record-" + strconv.Itoa(i),
			"enabled": i%2 == 0,
			"weight":  float64(i) * 1.5,
		})
	}

	return map[string]any{
		"small":  map[string]any{"host": "localhost", "port": 5432, "tls": true},
		"medium": medium,
		"large":  large,
	}
}

func BenchmarkEncode(b *testing.B) {
	ctx := context.Background()
	c := cyaml.New()
	for name, payload := range payloads() {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_, _ = c.Encode(ctx, payload)
			}
		})
	}
}

func BenchmarkDecode(b *testing.B) {
	ctx := context.Background()
	c := cyaml.New()
	for name, payload := range payloads() {
		data, err := c.Encode(ctx, payload)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				var out any
				_ = c.Decode(ctx, data, &out)
			}
		})
	}
}
