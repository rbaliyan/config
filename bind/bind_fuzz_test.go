package bind

import (
	"encoding/json"
	"errors"
	"testing"
)

// fuzzTarget is the struct the bind fuzzer maps untrusted data into. The
// validate tags exercise every rule branch (required/min/max/enum/pattern)
// so malformed values surface as ValidationErrors rather than panics.
type fuzzTarget struct {
	Name   string `json:"name" validate:"required,min=1,max=64"`
	Port   int    `json:"port" validate:"min=1,max=65535"`
	Level  string `json:"level" validate:"enum=debug|info|warn|error"`
	Host   string `json:"host" validate:"pattern=^[a-z0-9.-]+$"`
	Ratio  float64
	Nested struct {
		Enabled bool   `json:"enabled"`
		Tag     string `json:"tag" validate:"max=8"`
	} `json:"nested"`
}

// FuzzBindFlatMap drives untrusted input through the two halves of the bind
// pipeline that face external data: FieldMapper.FlatMapToStruct (reflection
// over arbitrary path-keyed values) and TagValidator.Validate (rule parsing +
// application). Neither may panic; validation failures must come back as
// errors that wrap ErrValidationFailed.
//
// The fuzzed bytes are interpreted as a JSON object of flat config keys
// (the same shape boundConfig.GetStruct builds from a store page), so the
// fuzzer reaches buildNestedMap and mapToStructRecursive with adversarial
// key paths, nesting, and value types.
func FuzzBindFlatMap(f *testing.F) {
	f.Add([]byte(`{"name":"svc","port":8080,"level":"info","host":"a.b"}`))
	f.Add([]byte(`{"port":"not-an-int"}`))
	f.Add([]byte(`{"nested/enabled":true,"nested/tag":"x"}`))
	f.Add([]byte(`{"name":"","level":"bogus"}`))
	f.Add([]byte(`{"a/b/c/d/e/f":1}`))
	f.Add([]byte(`{"":1,"/":2,"//":3}`))
	f.Add([]byte(`{"name":123,"ratio":"1.5"}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"port":999999}`))
	f.Add([]byte(`{"host":"NOT VALID HOST!!"}`))

	mapper := NewFieldMapper("json")
	validator := NewTagValidator("validate")

	f.Fuzz(func(t *testing.T, data []byte) {
		var flat map[string]any
		if err := json.Unmarshal(data, &flat); err != nil {
			return // not a JSON object; nothing to bind
		}

		var target fuzzTarget
		// FlatMapToStruct must never panic on arbitrary key paths / value
		// types. It may legitimately return an error.
		if err := mapper.FlatMapToStruct(flat, "", &target); err != nil {
			return
		}

		// Validation must never panic and must report failures as errors that
		// wrap ErrValidationFailed (not via panic, not silently).
		if err := validator.Validate(&target); err != nil {
			if !errors.Is(err, ErrValidationFailed) {
				t.Fatalf("validation error does not wrap ErrValidationFailed: %v", err)
			}
		}
	})
}
