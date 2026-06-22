package config_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/memory"
)

func TestValueTypeConversions(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected any
		check    func(config.Value) any
	}{
		{"int", 42, int64(42), func(v config.Value) any { i, _ := v.Int64(); return i }},
		{"float", 3.14, 3.14, func(v config.Value) any { f, _ := v.Float64(); return f }},
		{"string", "hello", "hello", func(v config.Value) any { s, _ := v.String(); return s }},
		{"bool", true, true, func(v config.Value) any { b, _ := v.Bool(); return b }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val := config.NewValue(tt.input)
			result := tt.check(val)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

// TestValueMetadata tests value metadata
func TestValueMetadata(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("test")

	// Set a value
	before := time.Now()
	_ = cfg.Set(ctx, "key", "value")
	after := time.Now()

	// Get and check metadata
	val, _ := cfg.Get(ctx, "key")
	meta := val.Metadata()

	if meta == nil {
		t.Fatal("Expected non-nil metadata")
	}

	if meta.Version() != 1 {
		t.Errorf("Expected version 1, got %d", meta.Version())
	}

	if meta.CreatedAt().Before(before) || meta.CreatedAt().After(after) {
		t.Error("CreatedAt should be between before and after")
	}

	// Update and check version increments
	time.Sleep(time.Millisecond) // Ensure time difference
	_ = cfg.Set(ctx, "key", "value2")

	val, _ = cfg.Get(ctx, "key")
	meta = val.Metadata()

	if meta.Version() != 2 {
		t.Errorf("Expected version 2 after update, got %d", meta.Version())
	}

	if !meta.UpdatedAt().After(meta.CreatedAt()) {
		t.Error("UpdatedAt should be after CreatedAt")
	}
}

// TestSetWithIfNotExists tests the WithIfNotExists option
func TestSetWithIfNotExists(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("test")

	// First set with IfNotExists should succeed
	err = cfg.Set(ctx, "lock/owner", "instance-1", config.WithIfNotExists())
	if err != nil {
		t.Fatalf("First Set with IfNotExists failed: %v", err)
	}

	// Second set with IfNotExists should fail
	err = cfg.Set(ctx, "lock/owner", "instance-2", config.WithIfNotExists())
	if !config.IsKeyExists(err) {
		t.Errorf("Expected KeyExists error, got: %v", err)
	}

	// Verify original value is unchanged
	val, _ := cfg.Get(ctx, "lock/owner")
	str, _ := val.String()
	if str != "instance-1" {
		t.Errorf("Value = %q, expected %q", str, "instance-1")
	}
}

// TestSetWithIfExists tests the WithIfExists option
func TestSetWithIfExists(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("test")

	// Set with IfExists should fail for non-existing key
	err = cfg.Set(ctx, "app/timeout", 60, config.WithIfExists())
	if !config.IsNotFound(err) {
		t.Errorf("Expected NotFound error, got: %v", err)
	}

	// Create the key normally
	_ = cfg.Set(ctx, "app/timeout", 30)

	// Now set with IfExists should succeed
	err = cfg.Set(ctx, "app/timeout", 60, config.WithIfExists())
	if err != nil {
		t.Fatalf("Set with IfExists failed: %v", err)
	}

	// Verify value was updated
	val, _ := cfg.Get(ctx, "app/timeout")
	i, _ := val.Int64()
	if i != 60 {
		t.Errorf("Value = %d, expected %d", i, 60)
	}
}

func TestTypeHelpers(t *testing.T) {
	// String()
	tests := []struct {
		typ  config.Type
		want string
	}{
		{config.TypeInt, "int"},
		{config.TypeFloat, "float"},
		{config.TypeString, "string"},
		{config.TypeBool, "bool"},
		{config.TypeMapStringInt, "map[string]int"},
		{config.TypeMapStringFloat, "map[string]float64"},
		{config.TypeMapStringString, "map[string]string"},
		{config.TypeListInt, "[]int"},
		{config.TypeListFloat, "[]float64"},
		{config.TypeListString, "[]string"},
		{config.TypeCustom, "custom"},
		{config.TypeUnknown, "unknown(0)"},
	}

	for _, tt := range tests {
		if got := tt.typ.String(); got != tt.want {
			t.Errorf("Type(%d).String() = %q, want %q", tt.typ, got, tt.want)
		}
	}

	// ParseType round-trips
	for _, tt := range tests {
		if tt.typ == config.TypeUnknown {
			continue // unknown doesn't round-trip
		}
		parsed := config.ParseType(tt.want)
		if parsed != tt.typ {
			t.Errorf("ParseType(%q) = %v, want %v", tt.want, parsed, tt.typ)
		}
	}

	// IsPrimitive
	if !config.TypeInt.IsPrimitive() {
		t.Error("TypeInt should be primitive")
	}
	if config.TypeMapStringInt.IsPrimitive() {
		t.Error("TypeMapStringInt should not be primitive")
	}

	// IsMap
	if !config.TypeMapStringString.IsMap() {
		t.Error("TypeMapStringString should be a map")
	}
	if config.TypeInt.IsMap() {
		t.Error("TypeInt should not be a map")
	}

	// IsList
	if !config.TypeListInt.IsList() {
		t.Error("TypeListInt should be a list")
	}
	if config.TypeString.IsList() {
		t.Error("TypeString should not be a list")
	}
}

func TestWriteModeString(t *testing.T) {
	tests := []struct {
		mode config.WriteMode
		want string
	}{
		{config.WriteModeUpsert, "upsert"},
		{config.WriteModeCreate, "create"},
		{config.WriteModeUpdate, "update"},
		{config.WriteMode(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.mode.String(); got != tt.want {
			t.Errorf("WriteMode(%d).String() = %q, want %q", tt.mode, got, tt.want)
		}
	}
}

func TestChangeTypeString(t *testing.T) {
	if config.ChangeTypeSet.String() != "set" {
		t.Errorf("ChangeTypeSet.String() = %q, want %q", config.ChangeTypeSet.String(), "set")
	}
	if config.ChangeTypeDelete.String() != "delete" {
		t.Errorf("ChangeTypeDelete.String() = %q, want %q", config.ChangeTypeDelete.String(), "delete")
	}
}

func TestValueUnmarshal(t *testing.T) {
	ctx := context.Background()
	val := config.NewValue(map[string]any{"name": "test", "count": float64(42)})

	var result map[string]any
	if err := val.Unmarshal(ctx, &result); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if result["name"] != "test" {
		t.Errorf("name = %v, want %q", result["name"], "test")
	}
}

func TestValueTypeConversionsExtended(t *testing.T) {
	// Bool conversion
	val := config.NewValue(true)
	b, err := val.Bool()
	if err != nil || !b {
		t.Errorf("Bool() = %v, %v; want true, nil", b, err)
	}

	// Float conversion
	val = config.NewValue(3.14)
	f, err := val.Float64()
	if err != nil || f != 3.14 {
		t.Errorf("Float64() = %v, %v; want 3.14, nil", f, err)
	}

	// Int from float (exact)
	val = config.NewValue(42.0)
	i, err := val.Int64()
	if err != nil {
		t.Errorf("Int64() from exact float failed: %v", err)
	}
	if i != 42 {
		t.Errorf("Int64() = %d, want 42", i)
	}

	// String from non-string
	val = config.NewValue(42)
	s, err := val.String()
	if err != nil {
		t.Errorf("String() from int failed: %v", err)
	}
	if s != "42" {
		t.Errorf("String() = %q, want %q", s, "42")
	}
}

func TestValueMarshal(t *testing.T) {
	val := config.NewValue("hello")
	data, err := val.Marshal(context.Background())
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	if len(data) == 0 {
		t.Error("Marshal returned empty data")
	}
}

func TestGetWriteMode(t *testing.T) {
	// Value without write mode
	val := config.NewValue("test")
	mode := config.GetWriteMode(val)
	if mode != config.WriteModeUpsert {
		t.Errorf("Default write mode = %v, want Upsert", mode)
	}

	// Value with explicit write mode
	val = config.NewValue("test", config.WithValueWriteMode(config.WriteModeCreate))
	mode = config.GetWriteMode(val)
	if mode != config.WriteModeCreate {
		t.Errorf("Write mode = %v, want Create", mode)
	}
}

func TestNewValueTypeDetection(t *testing.T) {
	tests := []struct {
		input any
		want  config.Type
	}{
		{42, config.TypeInt},
		{int64(42), config.TypeInt},
		{uint(42), config.TypeInt},
		{3.14, config.TypeFloat},
		{float32(3.14), config.TypeFloat},
		{42.0, config.TypeInt}, // exact float → int (JSON compat)
		{"hello", config.TypeString},
		{true, config.TypeBool},
		// Slices, maps, and nil are all TypeCustom
		{[]int{1, 2}, config.TypeCustom},
		{map[string]int{"a": 1}, config.TypeCustom},
		{struct{}{}, config.TypeCustom},
	}

	for _, tt := range tests {
		got := config.NewValue(tt.input).Type()
		if got != tt.want {
			t.Errorf("NewValue(%T).Type() = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestNewValueFromBytesError(t *testing.T) {
	ctx := context.Background()
	// Unknown codec name returns a raw pass-through value.
	val, err := config.NewValueFromBytes(ctx, []byte(`"hello"`), "nonexistent")
	if err != nil {
		t.Fatalf("NewValueFromBytes with unknown codec should return raw value, got: %v", err)
	}
	if val == nil {
		t.Error("Expected non-nil value")
	}
	if val.Codec() != "nonexistent" {
		t.Errorf("Codec() = %q, want %q", val.Codec(), "nonexistent")
	}

	// Empty codec name falls back to default.
	val2, err := config.NewValueFromBytes(ctx, []byte(`"hello"`), "")
	if err != nil {
		t.Fatalf("NewValueFromBytes with empty codec: %v", err)
	}
	if val2.Codec() != "json" {
		t.Errorf("Codec() = %q, want %q", val2.Codec(), "json")
	}

	// Invalid bytes with a known codec should error.
	_, err = config.NewValueFromBytes(ctx, []byte("not-json"), "json")
	if err == nil {
		t.Error("Expected error for invalid JSON bytes")
	}
}

func TestNewRawValue(t *testing.T) {
	raw := []byte{0xDE, 0xAD, 0xBE, 0xEF} // arbitrary binary data

	v := config.NewRawValue(raw, "client:encrypted:json")

	// Codec name is preserved.
	if v.Codec() != "client:encrypted:json" {
		t.Errorf("Codec() = %q, want %q", v.Codec(), "client:encrypted:json")
	}

	// Type is TypeCustom.
	if v.Type() != config.TypeCustom {
		t.Errorf("Type() = %v, want TypeCustom", v.Type())
	}

	ctx := context.Background()
	// Marshal returns the exact bytes.
	data, err := v.Marshal(ctx)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if string(data) != string(raw) {
		t.Errorf("Marshal() = %x, want %x", data, raw)
	}

	// Unmarshal should fail because rawCodec.Decode is unsupported.
	var s string
	if err := v.Unmarshal(ctx, &s); err == nil {
		t.Error("expected error from Unmarshal on raw value")
	}

	// Metadata defaults are accessible.
	if v.Metadata() == nil {
		t.Error("expected non-nil Metadata")
	}

	// ValueOptions are applied.
	v2 := config.NewRawValue(raw, "opaque", config.WithValueWriteMode(config.WriteModeCreate))
	if wm, ok := v2.(config.WriteModer); !ok || wm.WriteMode() != config.WriteModeCreate {
		t.Error("WithValueWriteMode option not applied to NewRawValue")
	}
}

// TestValueInt64EdgeCases tests Int64 conversion for edge case types.
func TestValueInt64EdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		input   any
		want    int64
		wantErr bool
	}{
		{"int8", int8(42), 42, false},
		{"int16", int16(42), 42, false},
		{"int32", int32(42), 42, false},
		{"int64", int64(42), 42, false},
		{"float32 exact", float32(42.0), 42, false},
		{"float32 truncation", float32(3.14), 0, true},
		{"float64 non-integer", 3.14, 0, true},
		{"float64 Inf", math.Inf(1), 0, true},
		{"float64 NaN", math.NaN(), 0, true},
		{"string parseable", "123", 123, false},
		{"string not parseable", "abc", 0, true},
		{"json.Number int", json.Number("42"), 42, false},
		{"json.Number not int", json.Number("3.14"), 0, true},
		{"unsupported type", []int{1}, 0, true},
		{"nil", nil, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val := config.NewValue(tt.input)
			got, err := val.Int64()
			if (err != nil) != tt.wantErr {
				t.Errorf("Int64() error = %v, wantErr = %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("Int64() = %d, want %d", got, tt.want)
			}
		})
	}
}

// TestValueFloat64EdgeCases tests Float64 conversion for edge case types.
func TestValueFloat64EdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		input   any
		want    float64
		wantErr bool
	}{
		{"float32", float32(3.14), float64(float32(3.14)), false},
		{"int", 42, 42.0, false},
		{"int8", int8(10), 10.0, false},
		{"int16", int16(10), 10.0, false},
		{"int32", int32(10), 10.0, false},
		{"int64", int64(10), 10.0, false},
		{"string parseable", "3.14", 3.14, false},
		{"string not parseable", "abc", 0, true},
		{"json.Number", json.Number("3.14"), 3.14, false},
		{"json.Number invalid", json.Number("abc"), 0, true},
		{"unsupported type", []int{1}, 0, true},
		{"nil", nil, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val := config.NewValue(tt.input)
			got, err := val.Float64()
			if (err != nil) != tt.wantErr {
				t.Errorf("Float64() error = %v, wantErr = %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("Float64() = %f, want %f", got, tt.want)
			}
		})
	}
}

// TestValueBoolEdgeCases tests Bool conversion for edge case types.
func TestValueBoolEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		input   any
		want    bool
		wantErr bool
	}{
		{"true", true, true, false},
		{"false", false, false, false},
		{"int nonzero", 1, true, false},
		{"int zero", 0, false, false},
		{"int64 nonzero", int64(1), true, false},
		{"int64 zero", int64(0), false, false},
		{"float64 nonzero", 1.0, true, false},
		{"float64 zero", 0.0, false, false},
		{"string true", "true", true, false},
		{"string false", "false", false, false},
		{"string invalid", "maybe", false, true},
		{"unsupported type", []int{}, false, true},
		{"nil", nil, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val := config.NewValue(tt.input)
			got, err := val.Bool()
			if (err != nil) != tt.wantErr {
				t.Errorf("Bool() error = %v, wantErr = %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("Bool() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestValueStringEdgeCases tests String conversion for edge case types.
func TestValueStringEdgeCases(t *testing.T) {
	// String from bytes
	val := config.NewValue([]byte("hello"))
	s, err := val.String()
	if err != nil {
		t.Fatalf("String() from []byte failed: %v", err)
	}
	if s != "hello" {
		t.Errorf("String() = %q, want %q", s, "hello")
	}

	// String from nil
	val = config.NewValue(nil)
	_, err = val.String()
	if err == nil {
		t.Error("String() from nil should return error")
	}
}

// TestValueMarshalNil tests Marshal with nil raw value.
func TestValueMarshalNil(t *testing.T) {
	val := config.NewValue(nil)
	_, err := val.Marshal(context.Background())
	if err == nil {
		t.Error("Marshal() with nil should return error")
	}
	if !errors.Is(err, config.ErrInvalidValue) {
		t.Errorf("Expected ErrInvalidValue, got: %v", err)
	}
}

// TestValueUnmarshalNil tests Unmarshal with nil raw value.
func TestValueUnmarshalNil(t *testing.T) {
	val := config.NewValue(nil)
	var target string
	err := val.Unmarshal(context.Background(), &target)
	if err == nil {
		t.Error("Unmarshal() with nil should return error")
	}
	if !errors.Is(err, config.ErrInvalidValue) {
		t.Errorf("Expected ErrInvalidValue, got: %v", err)
	}
}

// TestValueMarshalWithPreEncodedData tests Marshal when data is already encoded.
func TestValueMarshalWithPreEncodedData(t *testing.T) {
	ctx := context.Background()
	val, err := config.NewValueFromBytes(ctx, []byte(`"hello"`), "json")
	if err != nil {
		t.Fatalf("NewValueFromBytes failed: %v", err)
	}

	data, err := val.Marshal(ctx)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	if string(data) != `"hello"` {
		t.Errorf("Marshal = %q, want %q", string(data), `"hello"`)
	}
}

// TestValueUnmarshalWithCodecData tests Unmarshal using codec path.
func TestValueUnmarshalWithCodecData(t *testing.T) {
	ctx := context.Background()
	val, err := config.NewValueFromBytes(ctx, []byte(`{"name":"test"}`), "json")
	if err != nil {
		t.Fatalf("NewValueFromBytes failed: %v", err)
	}

	var result map[string]any
	if err := val.Unmarshal(ctx, &result); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if result["name"] != "test" {
		t.Errorf("name = %v, want %q", result["name"], "test")
	}
}

// TestGetWriteModeNonWriteModer tests GetWriteMode with a Value that does not implement WriteModer.
func TestGetWriteModeNonWriteModer(t *testing.T) {
	custom := &customValue{raw: "test", meta: &customMetadata{}}
	mode := config.GetWriteMode(custom)
	if mode != config.WriteModeUpsert {
		t.Errorf("GetWriteMode for non-WriteModer = %v, want Upsert", mode)
	}
}

// TestNewValueFromBytesValid tests NewValueFromBytes with valid JSON.
func TestNewValueFromBytesValid(t *testing.T) {
	val, err := config.NewValueFromBytes(context.Background(), []byte(`42`), "json",
		config.WithValueType(config.TypeInt),
		config.WithValueMetadata(1, time.Now(), time.Now()),
	)
	if err != nil {
		t.Fatalf("NewValueFromBytes failed: %v", err)
	}
	if val.Type() != config.TypeInt {
		t.Errorf("Type = %v, want TypeInt", val.Type())
	}
}

// TestValueCodecName tests the Codec() method.
func TestValueCodecName(t *testing.T) {
	val := config.NewValue("hello")
	codecName := val.Codec()
	if codecName != "json" {
		t.Errorf("Codec() = %q, want %q", codecName, "json")
	}
}

// TestDetectTypeJsonNumber tests detectType with json.Number.
func TestDetectTypeJsonNumber(t *testing.T) {
	// json.Number that parses as int
	val := config.NewValue(json.Number("42"))
	if val.Type() != config.TypeInt {
		t.Errorf("json.Number('42').Type() = %v, want TypeInt", val.Type())
	}

	// json.Number that doesn't parse as int (float)
	val = config.NewValue(json.Number("3.14"))
	if val.Type() != config.TypeFloat {
		t.Errorf("json.Number('3.14').Type() = %v, want TypeFloat", val.Type())
	}
}

// TestDetectTypeUnsigned tests detectType with unsigned int types.
func TestDetectTypeUnsigned(t *testing.T) {
	tests := []struct {
		input any
		want  config.Type
	}{
		{uint8(42), config.TypeInt},
		{uint16(42), config.TypeInt},
		{uint32(42), config.TypeInt},
		{uint64(42), config.TypeInt},
	}

	for _, tt := range tests {
		val := config.NewValue(tt.input)
		if val.Type() != tt.want {
			t.Errorf("NewValue(%T).Type() = %v, want %v", tt.input, val.Type(), tt.want)
		}
	}
}

// TestChangeTypeStringUnknown tests ChangeType.String() for an unknown type.
func TestChangeTypeStringUnknown(t *testing.T) {
	ct := config.ChangeType(99)
	if ct.String() != "unknown" {
		t.Errorf("ChangeType(99).String() = %q, want %q", ct.String(), "unknown")
	}
}

// TestParseTypeUnknown tests ParseType with an unrecognized string.
func TestParseTypeUnknown(t *testing.T) {
	got := config.ParseType("foobar")
	if got != config.TypeUnknown {
		t.Errorf("ParseType('foobar') = %v, want TypeUnknown", got)
	}
}

// TestWriteModeStringValues tests all WriteMode.String() values.
func TestWriteModeStringValues(t *testing.T) {
	if config.WriteModeUpsert.String() != "upsert" {
		t.Errorf("WriteModeUpsert.String() = %q", config.WriteModeUpsert.String())
	}
	if config.WriteModeCreate.String() != "create" {
		t.Errorf("WriteModeCreate.String() = %q", config.WriteModeCreate.String())
	}
	if config.WriteModeUpdate.String() != "update" {
		t.Errorf("WriteModeUpdate.String() = %q", config.WriteModeUpdate.String())
	}
}

// TestValueWriteMode tests Value's WriteMode when set via option.
func TestValueWriteMode(t *testing.T) {
	val := config.NewValue("test", config.WithValueWriteMode(config.WriteModeCreate))
	mode := config.GetWriteMode(val)
	if mode != config.WriteModeCreate {
		t.Errorf("WriteMode = %v, want Create", mode)
	}
}

// TestDetectTypeFloat64Special tests detectType with special float64 values.
func TestDetectTypeFloat64Special(t *testing.T) {
	// Inf should be detected as float
	val := config.NewValue(math.Inf(1))
	if val.Type() != config.TypeFloat {
		t.Errorf("Inf type = %v, want TypeFloat", val.Type())
	}

	// NaN should be detected as float
	val = config.NewValue(math.NaN())
	if val.Type() != config.TypeFloat {
		t.Errorf("NaN type = %v, want TypeFloat", val.Type())
	}
}

func TestMaxKeysPerNamespace(t *testing.T) {
	ctx := context.Background()

	mgr, err := config.New(
		config.WithStore(memory.NewStore()),
		config.WithMaxKeysPerNamespace(3),
	)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("test")

	// Create 3 keys (at limit)
	for i := 1; i <= 3; i++ {
		if err := cfg.Set(ctx, fmt.Sprintf("key%d", i), i); err != nil {
			t.Fatalf("Set key%d failed: %v", i, err)
		}
	}

	// 4th key should fail
	err = cfg.Set(ctx, "key4", 4)
	if !config.IsNamespaceFull(err) {
		t.Fatalf("expected ErrNamespaceFull, got: %v", err)
	}

	// Updating existing key should still work
	if err := cfg.Set(ctx, "key1", 100); err != nil {
		t.Fatalf("Update existing key failed: %v", err)
	}

	// WithIfExists (update mode) should bypass the check
	if err := cfg.Set(ctx, "key2", 200, config.WithIfExists()); err != nil {
		t.Fatalf("Update with WithIfExists failed: %v", err)
	}

	// Different namespace has its own limit
	cfg2 := mgr.Namespace("other")
	if err := cfg2.Set(ctx, "key1", 1); err != nil {
		t.Fatalf("Set in other namespace failed: %v", err)
	}
}

func TestMaxKeysPerNamespace_Unlimited(t *testing.T) {
	ctx := context.Background()

	// Default (0) means unlimited
	mgr, err := config.New(config.WithStore(memory.NewStore()))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer mgr.Close(ctx)

	cfg := mgr.Namespace("test")

	// Should be able to create many keys
	for i := 1; i <= 100; i++ {
		if err := cfg.Set(ctx, fmt.Sprintf("key%d", i), i); err != nil {
			t.Fatalf("Set key%d failed: %v", i, err)
		}
	}
}
