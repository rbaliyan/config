package toml_test

import (
	"context"
	"strconv"
	"testing"

	ctoml "github.com/rbaliyan/config/codec/toml"
)

// payloads returns deterministic encode targets at three sizes. TOML requires
// a table (map) at the top level, so "large" wraps its records under a key
// rather than being a bare slice. No randomness, so runs compare over time.
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
	rows := make([]map[string]any, 0, records)
	for i := 0; i < records; i++ {
		rows = append(rows, map[string]any{
			"id":      i,
			"name":    "record-" + strconv.Itoa(i),
			"enabled": i%2 == 0,
			"weight":  float64(i) * 1.5,
		})
	}
	large := map[string]any{"rows": rows}

	return map[string]any{
		"small":  map[string]any{"host": "localhost", "port": 5432, "tls": true},
		"medium": medium,
		"large":  large,
	}
}

func BenchmarkEncode(b *testing.B) {
	ctx := context.Background()
	c := ctoml.New()
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
	c := ctoml.New()
	for name, payload := range payloads() {
		data, err := c.Encode(ctx, payload)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				var out map[string]any
				_ = c.Decode(ctx, data, &out)
			}
		})
	}
}
