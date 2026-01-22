package bind

import (
	"bytes"
	"encoding/json"

	"github.com/santhosh-tekuri/jsonschema/v5"
)

// JSONSchemaValidator validates values against a JSON Schema.
type JSONSchemaValidator struct {
	schema *jsonschema.Schema
}

// NewJSONSchemaValidator creates a new JSON Schema validator.
// The schema should be valid JSON Schema bytes.
func NewJSONSchemaValidator(schemaBytes []byte) (*JSONSchemaValidator, error) {
	compiler := jsonschema.NewCompiler()

	// Add schema to compiler using reader
	if err := compiler.AddResource("schema.json", bytes.NewReader(schemaBytes)); err != nil {
		return nil, &ValidationError{
			Reason: "failed to add schema resource",
			Err:    err,
		}
	}

	// Compile schema
	schema, err := compiler.Compile("schema.json")
	if err != nil {
		return nil, &ValidationError{
			Reason: "failed to compile schema",
			Err:    err,
		}
	}

	return &JSONSchemaValidator{schema: schema}, nil
}

// Validate validates a value against the JSON Schema.
func (v *JSONSchemaValidator) Validate(value any) error {
	// Convert value to JSON and back to ensure proper types
	jsonBytes, err := json.Marshal(value)
	if err != nil {
		return &ValidationError{
			Value:  value,
			Reason: "failed to marshal value",
			Err:    err,
		}
	}

	var jsonValue any
	if err := json.Unmarshal(jsonBytes, &jsonValue); err != nil {
		return &ValidationError{
			Value:  value,
			Reason: "failed to unmarshal value",
			Err:    err,
		}
	}

	// Validate against schema
	if err := v.schema.Validate(jsonValue); err != nil {
		return &ValidationError{
			Value:  value,
			Reason: "schema validation failed",
			Err:    err,
		}
	}

	return nil
}

// MustNewJSONSchemaValidator creates a new JSON Schema validator and panics on error.
func MustNewJSONSchemaValidator(schemaBytes []byte) *JSONSchemaValidator {
	v, err := NewJSONSchemaValidator(schemaBytes)
	if err != nil {
		panic(err)
	}
	return v
}
