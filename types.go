package config

import "fmt"

// Type represents the type of a configuration value.
type Type int

const (
	// TypeUnknown indicates an unknown or unsupported type.
	TypeUnknown Type = iota

	// TypeInt represents an integer value.
	TypeInt

	// TypeFloat represents a floating-point value.
	TypeFloat

	// TypeString represents a string value.
	TypeString

	// TypeBool represents a boolean value.
	TypeBool

	// TypeMapStringInt represents a map[string]int value.
	TypeMapStringInt

	// TypeMapStringFloat represents a map[string]float64 value.
	TypeMapStringFloat

	// TypeMapStringString represents a map[string]string value.
	TypeMapStringString

	// TypeListInt represents a []int value.
	TypeListInt

	// TypeListFloat represents a []float64 value.
	TypeListFloat

	// TypeListString represents a []string value.
	TypeListString

	// TypeCustom represents a custom type that requires Unmarshal.
	TypeCustom
)

// String returns the string representation of the type.
func (t Type) String() string {
	switch t {
	case TypeInt:
		return "int"
	case TypeFloat:
		return "float"
	case TypeString:
		return "string"
	case TypeBool:
		return "bool"
	case TypeMapStringInt:
		return "map[string]int"
	case TypeMapStringFloat:
		return "map[string]float64"
	case TypeMapStringString:
		return "map[string]string"
	case TypeListInt:
		return "[]int"
	case TypeListFloat:
		return "[]float64"
	case TypeListString:
		return "[]string"
	case TypeCustom:
		return "custom"
	default:
		return fmt.Sprintf("unknown(%d)", t)
	}
}

// Value returns the underlying integer value of the type.
func (t Type) Value() int {
	return int(t)
}

// ParseType parses a string into a Type.
// Returns TypeUnknown for unrecognized strings.
func ParseType(s string) Type {
	switch s {
	case "int":
		return TypeInt
	case "float":
		return TypeFloat
	case "string":
		return TypeString
	case "bool":
		return TypeBool
	case "map[string]int":
		return TypeMapStringInt
	case "map[string]float64":
		return TypeMapStringFloat
	case "map[string]string":
		return TypeMapStringString
	case "[]int":
		return TypeListInt
	case "[]float64":
		return TypeListFloat
	case "[]string":
		return TypeListString
	case "custom":
		return TypeCustom
	default:
		return TypeUnknown
	}
}

// IsPrimitive returns true if the type is a primitive (int, float, string, bool).
func (t Type) IsPrimitive() bool {
	return t == TypeInt || t == TypeFloat || t == TypeString || t == TypeBool
}

// IsMap returns true if the type is a map type.
func (t Type) IsMap() bool {
	return t == TypeMapStringInt || t == TypeMapStringFloat || t == TypeMapStringString
}

// IsList returns true if the type is a list type.
func (t Type) IsList() bool {
	return t == TypeListInt || t == TypeListFloat || t == TypeListString
}
