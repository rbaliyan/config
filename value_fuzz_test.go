package config

import (
	"context"
	"testing"

	// Register the json codec so NewValueFromBytes("json", ...) decodes
	// rather than falling through to the raw pass-through path.
	_ "github.com/rbaliyan/config/codec/json"
)

// FuzzNewValueFromBytes exercises the higher-level decode + detectType +
// typed-accessor path. For any input that produces a Value, the typed
// accessors (Int64/Float64/String/Bool) and Marshal must never panic, and a
// successful Marshal must round-trip back through NewValueFromBytes.
func FuzzNewValueFromBytes(f *testing.F) {
	f.Add([]byte(`42`))
	f.Add([]byte(`"hello"`))
	f.Add([]byte(`true`))
	f.Add([]byte(`3.14`))
	f.Add([]byte(`null`))
	f.Add([]byte(`{"a":1}`))
	f.Add([]byte(`[1,2,3]`))
	f.Add([]byte(``))
	f.Add([]byte(`{{{`))
	f.Add([]byte(`123456789012345678`))
	f.Add([]byte(`"é中文"`))
	f.Add([]byte(`1e308`))

	ctx := context.Background()
	f.Fuzz(func(t *testing.T, data []byte) {
		v, err := NewValueFromBytes(ctx, data, "json")
		if err != nil {
			return // undecodable input; the only requirement is no panic
		}
		if v == nil {
			t.Fatalf("NewValueFromBytes returned nil value, nil error for %q", data)
		}

		// Typed accessors must never panic regardless of the underlying type;
		// they return errors on mismatch. We deliberately ignore the values.
		_, _ = v.Int64()
		_, _ = v.Float64()
		_, _ = v.String()
		_, _ = v.Bool()

		// Marshal must not panic. On success, the bytes must round-trip back
		// into a Value through the same constructor.
		out, err := v.Marshal(ctx)
		if err != nil {
			return
		}
		if _, err := NewValueFromBytes(ctx, out, "json"); err != nil {
			t.Fatalf("Marshal output failed to round-trip: %v (marshalled=%q from input=%q)", err, out, data)
		}
	})
}
