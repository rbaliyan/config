package bind

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/rbaliyan/config/codec"
)

func TestTagValidatorRequiredString(t *testing.T) {
	v := NewTagValidator("validate")

	type S struct {
		Name string `validate:"required"`
	}

	if err := v.Validate(S{Name: "ok"}); err != nil {
		t.Errorf("unexpected error for valid string: %v", err)
	}
	if err := v.Validate(S{Name: ""}); err == nil {
		t.Error("expected error for empty required string")
	}
}

func TestTagValidatorRequiredInt(t *testing.T) {
	v := NewTagValidator("validate")

	type S struct {
		Count int `validate:"required"`
	}

	if err := v.Validate(S{Count: 1}); err != nil {
		t.Errorf("unexpected error for valid int: %v", err)
	}
	if err := v.Validate(S{Count: 0}); err == nil {
		t.Error("expected error for zero required int")
	}
}

func TestTagValidatorRequiredBool(t *testing.T) {
	v := NewTagValidator("validate")

	type S struct {
		Enabled bool `validate:"required"`
	}

	if err := v.Validate(S{Enabled: true}); err != nil {
		t.Errorf("unexpected error for true bool: %v", err)
	}
	if err := v.Validate(S{Enabled: false}); err == nil {
		t.Error("expected error for false required bool")
	}
}

func TestTagValidatorRequiredPointer(t *testing.T) {
	v := NewTagValidator("validate")

	type S struct {
		Name *string `validate:"required"`
	}

	s := "ok"
	if err := v.Validate(S{Name: &s}); err != nil {
		t.Errorf("unexpected error for non-nil pointer: %v", err)
	}
	if err := v.Validate(S{Name: nil}); err == nil {
		t.Error("expected error for nil required pointer")
	}
}

func TestTagValidatorMinInt(t *testing.T) {
	v := NewTagValidator("validate")

	type S struct {
		Port int `validate:"min=1"`
	}

	if err := v.Validate(S{Port: 1}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := v.Validate(S{Port: 100}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := v.Validate(S{Port: 0}); err == nil {
		t.Error("expected error for value below min")
	}
}

func TestTagValidatorMinUint(t *testing.T) {
	v := NewTagValidator("validate")

	type S struct {
		Count uint `validate:"min=5"`
	}

	if err := v.Validate(S{Count: 5}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := v.Validate(S{Count: 10}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := v.Validate(S{Count: 1}); err == nil {
		t.Error("expected error for uint below min")
	}
}

func TestTagValidatorMinFloat(t *testing.T) {
	v := NewTagValidator("validate")

	type S struct {
		Rate float64 `validate:"min=0.5"`
	}

	if err := v.Validate(S{Rate: 0.5}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := v.Validate(S{Rate: 1.0}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := v.Validate(S{Rate: 0.1}); err == nil {
		t.Error("expected error for float below min")
	}
}

func TestTagValidatorMinString(t *testing.T) {
	v := NewTagValidator("validate")

	type S struct {
		Name string `validate:"min=3"`
	}

	if err := v.Validate(S{Name: "abc"}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := v.Validate(S{Name: "abcdef"}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := v.Validate(S{Name: "ab"}); err == nil {
		t.Error("expected error for string shorter than min")
	}
}

func TestTagValidatorMinSlice(t *testing.T) {
	v := NewTagValidator("validate")

	type S struct {
		Items []string `validate:"min=2"`
	}

	if err := v.Validate(S{Items: []string{"a", "b"}}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := v.Validate(S{Items: []string{"a"}}); err == nil {
		t.Error("expected error for slice shorter than min")
	}
}

func TestTagValidatorMinMap(t *testing.T) {
	v := NewTagValidator("validate")

	type S struct {
		Labels map[string]string `validate:"min=1"`
	}

	if err := v.Validate(S{Labels: map[string]string{"k": "v"}}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := v.Validate(S{Labels: map[string]string{}}); err == nil {
		t.Error("expected error for map smaller than min")
	}
}

func TestTagValidatorMinInvalidValue(t *testing.T) {
	v := NewTagValidator("validate")

	type S struct {
		Name string `validate:"min=abc"`
	}

	// Invalid min value should be silently skipped
	if err := v.Validate(S{Name: "x"}); err != nil {
		t.Errorf("expected no error for invalid min value, got: %v", err)
	}
}

func TestTagValidatorMaxInt(t *testing.T) {
	v := NewTagValidator("validate")

	type S struct {
		Port int `validate:"max=65535"`
	}

	if err := v.Validate(S{Port: 65535}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := v.Validate(S{Port: 100}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := v.Validate(S{Port: 70000}); err == nil {
		t.Error("expected error for value above max")
	}
}

func TestTagValidatorMaxUint(t *testing.T) {
	v := NewTagValidator("validate")

	type S struct {
		Count uint `validate:"max=10"`
	}

	if err := v.Validate(S{Count: 10}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := v.Validate(S{Count: 11}); err == nil {
		t.Error("expected error for uint above max")
	}
}

func TestTagValidatorMaxFloat(t *testing.T) {
	v := NewTagValidator("validate")

	type S struct {
		Rate float64 `validate:"max=1.0"`
	}

	if err := v.Validate(S{Rate: 1.0}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := v.Validate(S{Rate: 1.5}); err == nil {
		t.Error("expected error for float above max")
	}
}

func TestTagValidatorMaxString(t *testing.T) {
	v := NewTagValidator("validate")

	type S struct {
		Name string `validate:"max=5"`
	}

	if err := v.Validate(S{Name: "abc"}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := v.Validate(S{Name: "abcdef"}); err == nil {
		t.Error("expected error for string longer than max")
	}
}

func TestTagValidatorMaxSlice(t *testing.T) {
	v := NewTagValidator("validate")

	type S struct {
		Items []string `validate:"max=2"`
	}

	if err := v.Validate(S{Items: []string{"a", "b"}}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := v.Validate(S{Items: []string{"a", "b", "c"}}); err == nil {
		t.Error("expected error for slice longer than max")
	}
}

func TestTagValidatorMaxMap(t *testing.T) {
	v := NewTagValidator("validate")

	type S struct {
		Labels map[string]string `validate:"max=1"`
	}

	if err := v.Validate(S{Labels: map[string]string{"k": "v"}}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := v.Validate(S{Labels: map[string]string{"a": "1", "b": "2"}}); err == nil {
		t.Error("expected error for map larger than max")
	}
}

func TestTagValidatorMaxInvalidValue(t *testing.T) {
	v := NewTagValidator("validate")

	type S struct {
		Name string `validate:"max=abc"`
	}

	// Invalid max value should be silently skipped
	if err := v.Validate(S{Name: "x"}); err != nil {
		t.Errorf("expected no error for invalid max value, got: %v", err)
	}
}

func TestTagValidatorEnumString(t *testing.T) {
	v := NewTagValidator("validate")

	type S struct {
		Level string `validate:"enum=debug|info|warn|error"`
	}

	for _, level := range []string{"debug", "info", "warn", "error"} {
		if err := v.Validate(S{Level: level}); err != nil {
			t.Errorf("unexpected error for valid enum %q: %v", level, err)
		}
	}
	if err := v.Validate(S{Level: "trace"}); err == nil {
		t.Error("expected error for invalid enum value")
	}
}

func TestTagValidatorEnumInt(t *testing.T) {
	v := NewTagValidator("validate")

	type S struct {
		Code int `validate:"enum=1|2|3"`
	}

	if err := v.Validate(S{Code: 1}); err != nil {
		t.Errorf("unexpected error for valid int enum: %v", err)
	}
	if err := v.Validate(S{Code: 2}); err != nil {
		t.Errorf("unexpected error for valid int enum: %v", err)
	}
	if err := v.Validate(S{Code: 4}); err == nil {
		t.Error("expected error for invalid int enum value")
	}
}

func TestTagValidatorEnumUint(t *testing.T) {
	v := NewTagValidator("validate")

	type S struct {
		Code uint `validate:"enum=10|20"`
	}

	if err := v.Validate(S{Code: 10}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := v.Validate(S{Code: 30}); err == nil {
		t.Error("expected error for invalid uint enum")
	}
}

func TestTagValidatorEnumNonComparable(t *testing.T) {
	v := NewTagValidator("validate")

	type S struct {
		Items []string `validate:"enum=a|b"`
	}

	// Slices are non-comparable, should be silently skipped
	if err := v.Validate(S{Items: []string{"a"}}); err != nil {
		t.Errorf("expected no error for non-comparable enum type: %v", err)
	}
}

func TestTagValidatorPatternValid(t *testing.T) {
	v := NewTagValidator("validate")

	type S struct {
		Email string `validate:"pattern=^[a-z]+@[a-z]+$"`
	}

	if err := v.Validate(S{Email: "test@example"}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := v.Validate(S{Email: "invalid"}); err == nil {
		t.Error("expected error for non-matching pattern")
	}
}

func TestTagValidatorPatternInvalidRegex(t *testing.T) {
	v := NewTagValidator("validate")

	type S struct {
		Name string `validate:"pattern=[invalid"`
	}

	// Invalid regex should be silently skipped
	if err := v.Validate(S{Name: "anything"}); err != nil {
		t.Errorf("expected no error for invalid regex: %v", err)
	}
}

func TestTagValidatorPatternNonString(t *testing.T) {
	v := NewTagValidator("validate")

	type S struct {
		Count int `validate:"pattern=^\\d+$"`
	}

	// Pattern on non-string should be silently skipped
	if err := v.Validate(S{Count: 42}); err != nil {
		t.Errorf("expected no error for pattern on non-string: %v", err)
	}
}

func TestTagValidatorCombinedRules(t *testing.T) {
	v := NewTagValidator("validate")

	type S struct {
		Port int `validate:"required,min=1,max=65535"`
	}

	if err := v.Validate(S{Port: 8080}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := v.Validate(S{Port: 0}); err == nil {
		t.Error("expected error for zero required port")
	}
	if err := v.Validate(S{Port: 70000}); err == nil {
		t.Error("expected error for port above max")
	}
}

func TestTagValidatorNestedStruct(t *testing.T) {
	v := NewTagValidator("validate")

	type Inner struct {
		Host string `validate:"required"`
	}
	type Outer struct {
		Inner Inner
	}

	if err := v.Validate(Outer{Inner: Inner{Host: "localhost"}}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := v.Validate(Outer{Inner: Inner{Host: ""}}); err == nil {
		t.Error("expected error for empty required nested field")
	}
}

func TestTagValidatorNestedStructPointer(t *testing.T) {
	v := NewTagValidator("validate")

	type Inner struct {
		Host string `validate:"required"`
	}
	type Outer struct {
		Inner *Inner
	}

	if err := v.Validate(Outer{Inner: &Inner{Host: "localhost"}}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := v.Validate(Outer{Inner: &Inner{Host: ""}}); err == nil {
		t.Error("expected error for empty required nested pointer field")
	}
	// Nil pointer should be OK (no validation on nil)
	if err := v.Validate(Outer{Inner: nil}); err != nil {
		t.Errorf("expected no error for nil nested pointer: %v", err)
	}
}

func TestTagValidatorPointerInput(t *testing.T) {
	v := NewTagValidator("validate")

	type S struct {
		Name string `validate:"required"`
	}

	if err := v.Validate(&S{Name: "ok"}); err != nil {
		t.Errorf("unexpected error for pointer input: %v", err)
	}
	if err := v.Validate(&S{Name: ""}); err == nil {
		t.Error("expected error for empty required field via pointer")
	}
}

func TestTagValidatorNilPointerInput(t *testing.T) {
	v := NewTagValidator("validate")

	if err := v.Validate((*struct{})(nil)); err != nil {
		t.Errorf("expected no error for nil pointer input: %v", err)
	}
}

func TestTagValidatorNonStructInput(t *testing.T) {
	v := NewTagValidator("validate")

	if err := v.Validate("not a struct"); err != nil {
		t.Errorf("expected no error for non-struct input: %v", err)
	}
	if err := v.Validate(42); err != nil {
		t.Errorf("expected no error for int input: %v", err)
	}
}

func TestTagValidatorSkipDash(t *testing.T) {
	v := NewTagValidator("validate")

	type S struct {
		Name    string `validate:"required"`
		Ignored string `validate:"-"`
	}

	// Even though Ignored is empty, it should be skipped
	if err := v.Validate(S{Name: "ok", Ignored: ""}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestTagValidatorUnknownRule(t *testing.T) {
	v := NewTagValidator("validate")

	type S struct {
		Name string `validate:"custom_rule=value"`
	}

	// Unknown rules should be silently ignored
	if err := v.Validate(S{Name: "test"}); err != nil {
		t.Errorf("expected no error for unknown rule: %v", err)
	}
}

func TestTagValidatorNestedFieldPrefix(t *testing.T) {
	v := NewTagValidator("validate")

	type Inner struct {
		Port int `validate:"min=1"`
	}
	type Outer struct {
		DB Inner
	}

	err := v.Validate(Outer{DB: Inner{Port: 0}})
	if err == nil {
		t.Fatal("expected validation error")
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected ValidationError, got %T", err)
	}
	if ve.Field != "DB.Port" {
		t.Errorf("expected field 'DB.Port', got %q", ve.Field)
	}
}

func TestTagValidatorCustomTagName(t *testing.T) {
	v := NewTagValidator("check")

	type S struct {
		Name string `check:"required"`
		Age  int    `validate:"required"` // Should NOT be checked
	}

	if err := v.Validate(S{Name: "ok", Age: 0}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := v.Validate(S{Name: "", Age: 10}); err == nil {
		t.Error("expected error for empty required field with custom tag")
	}
}

func TestParseTagRules(t *testing.T) {
	tests := []struct {
		tag   string
		count int
		first string
	}{
		{"required", 1, "required"},
		{"required,min=1", 2, "required"},
		{"required,min=1,max=100", 3, "required"},
		{"", 0, ""},
		{" , ", 0, ""},
	}

	for _, tt := range tests {
		t.Run(tt.tag, func(t *testing.T) {
			rules := parseTagRules(tt.tag)
			if len(rules) != tt.count {
				t.Errorf("expected %d rules, got %d", tt.count, len(rules))
			}
			if tt.count > 0 && rules[0].name != tt.first {
				t.Errorf("expected first rule %q, got %q", tt.first, rules[0].name)
			}
		})
	}
}

func TestParseTagRulesWithValues(t *testing.T) {
	rules := parseTagRules("min=1,max=100,enum=a|b|c")
	if len(rules) != 3 {
		t.Fatalf("expected 3 rules, got %d", len(rules))
	}
	if rules[0].name != "min" || rules[0].value != "1" {
		t.Errorf("unexpected first rule: %+v", rules[0])
	}
	if rules[1].name != "max" || rules[1].value != "100" {
		t.Errorf("unexpected second rule: %+v", rules[1])
	}
	if rules[2].name != "enum" || rules[2].value != "a|b|c" {
		t.Errorf("unexpected third rule: %+v", rules[2])
	}
}

func TestValidationErrorFormatting(t *testing.T) {
	tests := []struct {
		name     string
		err      *ValidationError
		expected string
	}{
		{
			name:     "key and field with reason",
			err:      &ValidationError{Key: "db", Field: "Host", Reason: "is required"},
			expected: `validation failed for "db" field "Host": is required`,
		},
		{
			name:     "key only with reason",
			err:      &ValidationError{Key: "db", Reason: "invalid"},
			expected: `validation failed for "db": invalid`,
		},
		{
			name:     "no key no field with reason",
			err:      &ValidationError{Reason: "bad value"},
			expected: `validation failed: bad value`,
		},
		{
			name:     "no reason with err",
			err:      &ValidationError{Key: "db", Err: fmt.Errorf("some error")},
			expected: `validation failed for "db": some error`,
		},
		{
			name:     "no reason no err",
			err:      &ValidationError{Key: "db"},
			expected: `validation failed for "db": unknown error`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.err.Error()
			if got != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, got)
			}
		})
	}
}

func TestValidationErrorValue(t *testing.T) {
	err := &ValidationError{
		Key:    "config",
		Field:  "Port",
		Value:  99999,
		Reason: "value too large",
	}

	if err.Value != 99999 {
		t.Errorf("expected Value 99999, got %v", err.Value)
	}
}

func TestValidationErrorUnwrap(t *testing.T) {
	underlying := fmt.Errorf("custom error")
	err := &ValidationError{Err: underlying}
	if err.Unwrap() != underlying {
		t.Error("expected Unwrap to return underlying error")
	}

	err2 := &ValidationError{Reason: "test"}
	if err2.Unwrap() != ErrValidationFailed {
		t.Error("expected Unwrap to return ErrValidationFailed when Err is nil")
	}
}

func TestBindErrorFormatting(t *testing.T) {
	err := &BindError{
		Key: "database",
		Op:  "marshal",
		Err: fmt.Errorf("encoding failed"),
	}

	expected := `binding failed for "database" during marshal: encoding failed`
	if err.Error() != expected {
		t.Errorf("expected %q, got %q", expected, err.Error())
	}
}

func TestBindErrorUnwrap(t *testing.T) {
	underlying := fmt.Errorf("inner error")
	err := &BindError{Key: "k", Op: "op", Err: underlying}
	if err.Unwrap() != underlying {
		t.Error("expected Unwrap to return underlying error")
	}

	err2 := &BindError{Key: "k", Op: "op"}
	if err2.Unwrap() != ErrBindingFailed {
		t.Error("expected Unwrap to return ErrBindingFailed when Err is nil")
	}
}

func TestIsValidationErrorWrapped(t *testing.T) {
	inner := &ValidationError{Key: "test", Reason: "fail"}
	wrapped := fmt.Errorf("wrap: %w", inner)
	if !IsValidationError(wrapped) {
		t.Error("expected IsValidationError to find wrapped ValidationError")
	}
}

func TestIsBindErrorWrapped(t *testing.T) {
	inner := &BindError{Key: "test", Op: "op"}
	wrapped := fmt.Errorf("wrap: %w", inner)
	if !IsBindError(wrapped) {
		t.Error("expected IsBindError to find wrapped BindError")
	}
}

func TestIsValidationErrorNil(t *testing.T) {
	if IsValidationError(nil) {
		t.Error("expected false for nil error")
	}
}

func TestIsBindErrorNil(t *testing.T) {
	if IsBindError(nil) {
		t.Error("expected false for nil error")
	}
}

type testCodec struct {
	name string
}

func (c *testCodec) Name() string                  { return c.name }
func (c *testCodec) Encode(v any) ([]byte, error)  { return nil, nil }
func (c *testCodec) Decode(data []byte, v any) error { return nil }

func TestWithCodecOption(t *testing.T) {
	cfg := setupTestConfig(t)
	tc := &testCodec{name: "test-codec"}

	b := New(cfg, WithCodec(tc))
	if b.codec != tc {
		t.Error("expected custom codec to be set")
	}
	if b.codec.Name() != "test-codec" {
		t.Errorf("expected codec name 'test-codec', got %q", b.codec.Name())
	}
}

func TestWithFieldTagOption(t *testing.T) {
	cfg := setupTestConfig(t)

	b := New(cfg, WithFieldTag("config"))
	if b.fieldTag != "config" {
		t.Errorf("expected fieldTag 'config', got %q", b.fieldTag)
	}
	if b.mapper.tagName != "config" {
		t.Errorf("expected mapper tagName 'config', got %q", b.mapper.tagName)
	}
}

func TestWithFieldTagEmptyIgnored(t *testing.T) {
	cfg := setupTestConfig(t)

	b := New(cfg, WithFieldTag(""))
	if b.fieldTag != "json" {
		t.Errorf("expected default fieldTag 'json' when empty provided, got %q", b.fieldTag)
	}
}

func TestWithTagValidationOption(t *testing.T) {
	cfg := setupTestConfig(t)

	b := New(cfg, WithTagValidation())
	if b.validator == nil {
		t.Fatal("expected validator to be set")
	}
	if b.validator.tagName != "validate" {
		t.Errorf("expected validator tag 'validate', got %q", b.validator.tagName)
	}
}

func TestWithCustomTagValidationOption(t *testing.T) {
	cfg := setupTestConfig(t)

	b := New(cfg, WithCustomTagValidation("check"))
	if b.validator == nil {
		t.Fatal("expected validator to be set")
	}
	if b.validator.tagName != "check" {
		t.Errorf("expected validator tag 'check', got %q", b.validator.tagName)
	}
}

func TestWithCustomTagValidationIntegration(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	type S struct {
		Name string `json:"name" check:"required"`
	}

	b := New(cfg, WithCustomTagValidation("check"))
	bound := b.Bind()

	if err := bound.SetStruct(ctx, "app", S{Name: ""}); err == nil {
		t.Error("expected validation error for empty required field with custom tag")
	}
	if err := bound.SetStruct(ctx, "app", S{Name: "ok"}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestWithFieldTagIntegration(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	type S struct {
		Name string `config:"my_name"`
		Age  int    `config:"my_age"`
	}

	b := New(cfg, WithFieldTag("config"))
	bound := b.Bind()

	if err := bound.SetStruct(ctx, "app", S{Name: "test", Age: 25}); err != nil {
		t.Fatalf("SetStruct error: %v", err)
	}

	// Verify keys use the custom tag names
	nameVal, err := cfg.Get(ctx, "app/my_name")
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	var name string
	if err := nameVal.Unmarshal(&name); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if name != "test" {
		t.Errorf("expected name 'test', got %q", name)
	}
}

func TestGetStructDigest(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	type S struct {
		Host string `json:"host"`
		Port int    `json:"port"`
	}

	if err := cfg.Set(ctx, "db/host", "localhost"); err != nil {
		t.Fatalf("Set error: %v", err)
	}
	if err := cfg.Set(ctx, "db/port", 5432); err != nil {
		t.Fatalf("Set error: %v", err)
	}

	b := New(cfg)
	bound := b.Bind()

	var s S
	digest1, err := bound.GetStructDigest(ctx, "db", &s)
	if err != nil {
		t.Fatalf("GetStructDigest error: %v", err)
	}
	if s.Host != "localhost" || s.Port != 5432 {
		t.Errorf("unexpected struct values: %+v", s)
	}
	if digest1 == 0 {
		t.Error("expected non-zero digest")
	}

	// Same data should produce same digest
	var s2 S
	digest2, err := bound.GetStructDigest(ctx, "db", &s2)
	if err != nil {
		t.Fatalf("GetStructDigest error: %v", err)
	}
	if digest1 != digest2 {
		t.Errorf("expected same digest for same data, got %d and %d", digest1, digest2)
	}

	// Different data should produce different digest
	if err := cfg.Set(ctx, "db/port", 3306); err != nil {
		t.Fatalf("Set error: %v", err)
	}
	var s3 S
	digest3, err := bound.GetStructDigest(ctx, "db", &s3)
	if err != nil {
		t.Fatalf("GetStructDigest error: %v", err)
	}
	if digest1 == digest3 {
		t.Error("expected different digest for different data")
	}
	if s3.Port != 3306 {
		t.Errorf("expected port 3306, got %d", s3.Port)
	}
}

func TestGetStructDigestNotFound(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	b := New(cfg)
	bound := b.Bind()

	var s struct{}
	_, err := bound.GetStructDigest(ctx, "nonexistent", &s)
	if err == nil {
		t.Error("expected error for nonexistent key")
	}
}

func TestGetStructDigestWithValidation(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)

	type S struct {
		Name string `json:"name" validate:"required"`
	}

	if err := cfg.Set(ctx, "app/name", ""); err != nil {
		t.Fatalf("Set error: %v", err)
	}

	b := New(cfg, WithTagValidation())
	bound := b.Bind()

	var s S
	_, err := bound.GetStructDigest(ctx, "app", &s)
	if err == nil {
		t.Error("expected validation error for empty required field")
	}
}

func TestDefaultCodec(t *testing.T) {
	cfg := setupTestConfig(t)
	b := New(cfg)
	if b.codec == nil {
		t.Fatal("expected default codec to be set")
	}
	defaultCodec := codec.Default()
	if b.codec != defaultCodec {
		t.Error("expected default codec to be the JSON codec")
	}
}

func TestValidateWithoutValidator(t *testing.T) {
	cfg := setupTestConfig(t)
	b := New(cfg)

	type S struct {
		Name string `validate:"required"`
	}

	if err := b.Validate(S{Name: ""}); err != nil {
		t.Errorf("expected no error without validator, got: %v", err)
	}
}

func TestSetStructWithNonStructReturnsBindError(t *testing.T) {
	ctx := context.Background()
	cfg := setupTestConfig(t)
	b := New(cfg)
	bound := b.Bind()

	err := bound.SetStruct(ctx, "key", "not a struct")
	if err == nil {
		t.Error("expected error for non-struct value")
	}
	if !IsBindError(err) {
		t.Errorf("expected BindError, got %T: %v", err, err)
	}
}

func TestValidationErrorHasFieldInfo(t *testing.T) {
	v := NewTagValidator("validate")

	type S struct {
		Port int `validate:"min=1"`
	}

	err := v.Validate(S{Port: 0})
	if err == nil {
		t.Fatal("expected validation error")
	}

	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected ValidationError, got %T", err)
	}
	if ve.Field != "Port" {
		t.Errorf("expected field 'Port', got %q", ve.Field)
	}
	if ve.Value != int64(0) {
		t.Errorf("expected value 0, got %v", ve.Value)
	}
	if ve.Reason == "" {
		t.Error("expected non-empty reason")
	}
}

func TestTagValidatorStructWithNoValidateTag(t *testing.T) {
	v := NewTagValidator("validate")

	type S struct {
		Name  string
		Count int
	}

	if err := v.Validate(S{Name: "", Count: 0}); err != nil {
		t.Errorf("expected no error for struct without validate tags: %v", err)
	}
}

func TestTagValidatorNestedStructWithTag(t *testing.T) {
	v := NewTagValidator("validate")

	type Inner struct {
		Val string `validate:"required"`
	}
	type Outer struct {
		Inner Inner `validate:"required"`
	}

	// When the struct field itself has a validate tag, it uses
	// the field-level validation rather than recursing
	if err := v.Validate(Outer{Inner: Inner{Val: "ok"}}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestTagValidatorPointerToStructWithTag(t *testing.T) {
	v := NewTagValidator("validate")

	type Inner struct {
		Val string `validate:"required"`
	}
	type Outer struct {
		Inner *Inner `validate:"required"`
	}

	// Non-nil pointer to struct with its own tag
	if err := v.Validate(Outer{Inner: &Inner{Val: "ok"}}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Nil pointer should fail required check
	if err := v.Validate(Outer{Inner: nil}); err == nil {
		t.Error("expected error for nil required pointer")
	}
}
