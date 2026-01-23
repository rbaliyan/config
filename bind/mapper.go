// Package bind provides struct binding with custom tag support.
package bind

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

// FieldMapper handles struct-to-map and map-to-struct conversions
// with support for custom struct tags and field ignoring.
type FieldMapper struct {
	tagName string // struct tag name (default: "json")
}

// NewFieldMapper creates a new mapper with the specified tag name.
// If tagName is empty, defaults to "json".
func NewFieldMapper(tagName string) *FieldMapper {
	if tagName == "" {
		tagName = "json"
	}
	return &FieldMapper{tagName: tagName}
}

// StructToMap converts a struct to a map using the configured tag name.
// Fields tagged with `tagname:"-"` are ignored.
func (m *FieldMapper) StructToMap(v any) (map[string]any, error) {
	val := reflect.ValueOf(v)
	if val.Kind() == reflect.Pointer {
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected struct, got %s", val.Kind())
	}

	return m.structToMapRecursive(val)
}

func (m *FieldMapper) structToMapRecursive(val reflect.Value) (map[string]any, error) {
	result := make(map[string]any)
	typ := val.Type()

	for i := 0; i < val.NumField(); i++ {
		field := typ.Field(i)
		fieldVal := val.Field(i)

		// Skip unexported fields
		if !field.IsExported() {
			continue
		}

		// Get tag value
		tagValue := field.Tag.Get(m.tagName)

		// Skip fields with "-" tag
		if tagValue == "-" {
			continue
		}

		// Parse tag options (name,omitempty,etc)
		name, opts := parseTag(tagValue)
		if name == "" {
			name = field.Name
		}

		// Handle omitempty
		if hasOption(opts, "omitempty") && isEmptyValue(fieldVal) {
			continue
		}

		// Check for nonrecursive option
		nonrecursive := hasOption(opts, "nonrecursive")

		// Convert value
		var mapVal any
		switch fieldVal.Kind() {
		case reflect.Struct:
			if nonrecursive {
				mapVal = fieldVal.Interface()
			} else {
				nested, err := m.structToMapRecursive(fieldVal)
				if err != nil {
					return nil, err
				}
				mapVal = nested
			}
		case reflect.Ptr:
			if fieldVal.IsNil() {
				if hasOption(opts, "omitempty") {
					continue
				}
				mapVal = nil
			} else if fieldVal.Elem().Kind() == reflect.Struct {
				if nonrecursive {
					mapVal = fieldVal.Elem().Interface()
				} else {
					nested, err := m.structToMapRecursive(fieldVal.Elem())
					if err != nil {
						return nil, err
					}
					mapVal = nested
				}
			} else {
				mapVal = fieldVal.Elem().Interface()
			}
		case reflect.Slice, reflect.Array:
			if fieldVal.IsNil() {
				mapVal = nil
			} else {
				slice := make([]any, fieldVal.Len())
				for j := 0; j < fieldVal.Len(); j++ {
					elem := fieldVal.Index(j)
					if elem.Kind() == reflect.Struct {
						nested, err := m.structToMapRecursive(elem)
						if err != nil {
							return nil, err
						}
						slice[j] = nested
					} else {
						slice[j] = elem.Interface()
					}
				}
				mapVal = slice
			}
		case reflect.Map:
			if fieldVal.IsNil() {
				mapVal = nil
			} else {
				mapVal = fieldVal.Interface()
			}
		default:
			mapVal = fieldVal.Interface()
		}

		result[name] = mapVal
	}

	return result, nil
}

// StructToFlatMap converts a struct to a flat map with path-based keys.
// Nested structs are flattened using "/" as separator.
// Example: {Database: {Host: "localhost"}} -> {"database/host": "localhost"}
func (m *FieldMapper) StructToFlatMap(v any, prefix string) (map[string]any, error) {
	val := reflect.ValueOf(v)
	if val.Kind() == reflect.Pointer {
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected struct, got %s", val.Kind())
	}

	result := make(map[string]any)
	m.flattenStruct(val, prefix, result)
	return result, nil
}

func (m *FieldMapper) flattenStruct(val reflect.Value, prefix string, result map[string]any) {
	typ := val.Type()

	for i := 0; i < val.NumField(); i++ {
		field := typ.Field(i)
		fieldVal := val.Field(i)

		// Skip unexported fields
		if !field.IsExported() {
			continue
		}

		// Get tag value
		tagValue := field.Tag.Get(m.tagName)

		// Skip fields with "-" tag
		if tagValue == "-" {
			continue
		}

		// Parse tag options
		name, opts := parseTag(tagValue)
		if name == "" {
			name = field.Name
		}

		// Handle omitempty
		if hasOption(opts, "omitempty") && isEmptyValue(fieldVal) {
			continue
		}

		// Build the key path
		key := name
		if prefix != "" {
			key = prefix + "/" + name
		}

		// Check for nonrecursive option - store struct as single value
		nonrecursive := hasOption(opts, "nonrecursive")

		// Handle different types
		switch fieldVal.Kind() {
		case reflect.Struct:
			if nonrecursive {
				// Store as single value (will be JSON encoded by codec)
				result[key] = fieldVal.Interface()
			} else {
				// Recursively flatten nested struct
				m.flattenStruct(fieldVal, key, result)
			}
		case reflect.Ptr:
			if fieldVal.IsNil() {
				if !hasOption(opts, "omitempty") {
					result[key] = nil
				}
			} else if fieldVal.Elem().Kind() == reflect.Struct {
				if nonrecursive {
					result[key] = fieldVal.Elem().Interface()
				} else {
					m.flattenStruct(fieldVal.Elem(), key, result)
				}
			} else {
				result[key] = fieldVal.Elem().Interface()
			}
		default:
			result[key] = fieldVal.Interface()
		}
	}
}

// FlatMapToStruct converts a flat map with path-based keys to a struct.
// Keys are expected to use "/" as separator for nested fields.
func (m *FieldMapper) FlatMapToStruct(data map[string]any, prefix string, target any) error {
	val := reflect.ValueOf(target)
	if val.Kind() != reflect.Ptr || val.IsNil() {
		return fmt.Errorf("target must be a non-nil pointer")
	}
	val = val.Elem()
	if val.Kind() != reflect.Struct {
		return fmt.Errorf("target must be a pointer to struct")
	}

	// Build nested map from flat data
	nestedData := buildNestedMap(data, prefix)

	return m.mapToStructRecursive(nestedData, val)
}

// buildNestedMap converts flat path-based keys to nested map structure.
func buildNestedMap(data map[string]any, prefix string) map[string]any {
	result := make(map[string]any)

	for fullKey, value := range data {
		// Remove prefix from key
		key := fullKey
		if prefix != "" {
			if !strings.HasPrefix(fullKey, prefix) {
				continue
			}
			key = strings.TrimPrefix(fullKey, prefix)
			key = strings.TrimPrefix(key, "/")
		}

		if key == "" {
			continue
		}

		// Build nested structure
		parts := strings.Split(key, "/")
		current := result

		for i, part := range parts {
			if i == len(parts)-1 {
				// Leaf node
				current[part] = value
			} else {
				// Intermediate node
				if _, ok := current[part]; !ok {
					current[part] = make(map[string]any)
				}
				if next, ok := current[part].(map[string]any); ok {
					current = next
				} else {
					// Path conflict - overwrite
					newMap := make(map[string]any)
					current[part] = newMap
					current = newMap
				}
			}
		}
	}

	return result
}

// MapToStruct converts a map to a struct using the configured tag name.
// Fields tagged with `tagname:"-"` are ignored.
func (m *FieldMapper) MapToStruct(data map[string]any, target any) error {
	val := reflect.ValueOf(target)
	if val.Kind() != reflect.Ptr || val.IsNil() {
		return fmt.Errorf("target must be a non-nil pointer")
	}
	val = val.Elem()
	if val.Kind() != reflect.Struct {
		return fmt.Errorf("target must be a pointer to struct")
	}

	return m.mapToStructRecursive(data, val)
}

func (m *FieldMapper) mapToStructRecursive(data map[string]any, val reflect.Value) error {
	typ := val.Type()

	// Build a map of tag name -> field index
	fieldMap := make(map[string]int)
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if !field.IsExported() {
			continue
		}

		tagValue := field.Tag.Get(m.tagName)
		if tagValue == "-" {
			continue // Skip ignored fields
		}

		name, _ := parseTag(tagValue)
		if name == "" {
			name = field.Name
		}
		fieldMap[name] = i
		// Also map lowercase version for flexibility
		fieldMap[strings.ToLower(name)] = i
	}

	for key, value := range data {
		idx, ok := fieldMap[key]
		if !ok {
			idx, ok = fieldMap[strings.ToLower(key)]
		}
		if !ok {
			continue // No matching field
		}

		field := typ.Field(idx)
		fieldVal := val.Field(idx)

		if !fieldVal.CanSet() {
			continue
		}

		if err := m.setValue(fieldVal, value, field); err != nil {
			return fmt.Errorf("field %s: %w", field.Name, err)
		}
	}

	return nil
}

func (m *FieldMapper) setValue(fieldVal reflect.Value, value any, field reflect.StructField) error {
	if value == nil {
		return nil
	}

	valueVal := reflect.ValueOf(value)

	switch fieldVal.Kind() {
	case reflect.Struct:
		// Nested struct
		if mapVal, ok := value.(map[string]any); ok {
			return m.mapToStructRecursive(mapVal, fieldVal)
		}
		// Try JSON conversion as fallback
		return m.jsonConvert(value, fieldVal.Addr().Interface())

	case reflect.Ptr:
		if valueVal.Kind() == reflect.Invalid || (valueVal.Kind() == reflect.Ptr && valueVal.IsNil()) {
			return nil
		}
		// Create new instance
		newVal := reflect.New(fieldVal.Type().Elem())
		if fieldVal.Type().Elem().Kind() == reflect.Struct {
			if mapVal, ok := value.(map[string]any); ok {
				if err := m.mapToStructRecursive(mapVal, newVal.Elem()); err != nil {
					return err
				}
			}
		} else {
			newVal.Elem().Set(reflect.ValueOf(value))
		}
		fieldVal.Set(newVal)
		return nil

	case reflect.Slice:
		if sliceVal, ok := value.([]any); ok {
			slice := reflect.MakeSlice(fieldVal.Type(), len(sliceVal), len(sliceVal))
			elemType := fieldVal.Type().Elem()
			for i, elem := range sliceVal {
				elemVal := slice.Index(i)
				if elemType.Kind() == reflect.Struct {
					if mapElem, ok := elem.(map[string]any); ok {
						if err := m.mapToStructRecursive(mapElem, elemVal); err != nil {
							return err
						}
					}
				} else {
					if err := m.setBasicValue(elemVal, elem); err != nil {
						return err
					}
				}
			}
			fieldVal.Set(slice)
			return nil
		}
		return m.jsonConvert(value, fieldVal.Addr().Interface())

	case reflect.Map:
		return m.jsonConvert(value, fieldVal.Addr().Interface())

	default:
		return m.setBasicValue(fieldVal, value)
	}
}

func (m *FieldMapper) setBasicValue(fieldVal reflect.Value, value any) error {
	valueVal := reflect.ValueOf(value)

	// Handle type conversions
	if valueVal.Type().ConvertibleTo(fieldVal.Type()) {
		fieldVal.Set(valueVal.Convert(fieldVal.Type()))
		return nil
	}

	// Handle numeric conversions (JSON numbers are float64)
	if isNumeric(fieldVal.Kind()) && isNumeric(valueVal.Kind()) {
		return setNumericValue(fieldVal, valueVal)
	}

	// Fallback to JSON conversion
	return m.jsonConvert(value, fieldVal.Addr().Interface())
}

func (m *FieldMapper) jsonConvert(value any, target any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}

// parseTag splits a struct tag into name and options
func parseTag(tag string) (string, []string) {
	parts := strings.Split(tag, ",")
	if len(parts) == 0 {
		return "", nil
	}
	return parts[0], parts[1:]
}

func hasOption(opts []string, opt string) bool {
	for _, o := range opts {
		if o == opt {
			return true
		}
	}
	return false
}

func isEmptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	}
	return false
}

func isNumeric(k reflect.Kind) bool {
	switch k {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	}
	return false
}

func setNumericValue(fieldVal reflect.Value, valueVal reflect.Value) error {
	switch fieldVal.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		switch valueVal.Kind() {
		case reflect.Float32, reflect.Float64:
			fieldVal.SetInt(int64(valueVal.Float()))
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			fieldVal.SetInt(valueVal.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			fieldVal.SetInt(int64(valueVal.Uint()))
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		switch valueVal.Kind() {
		case reflect.Float32, reflect.Float64:
			fieldVal.SetUint(uint64(valueVal.Float()))
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			fieldVal.SetUint(uint64(valueVal.Int()))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			fieldVal.SetUint(valueVal.Uint())
		}
	case reflect.Float32, reflect.Float64:
		switch valueVal.Kind() {
		case reflect.Float32, reflect.Float64:
			fieldVal.SetFloat(valueVal.Float())
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			fieldVal.SetFloat(float64(valueVal.Int()))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			fieldVal.SetFloat(float64(valueVal.Uint()))
		}
	}
	return nil
}
