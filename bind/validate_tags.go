package bind

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

// TagValidator validates structs using struct tags.
type TagValidator struct {
	tagName string
}

// NewTagValidator creates a new tag-based validator.
// tagName is the struct tag to look for (e.g., "validate").
func NewTagValidator(tagName string) *TagValidator {
	return &TagValidator{tagName: tagName}
}

// Validate validates a struct using its tags.
func (v *TagValidator) Validate(value any) error {
	val := reflect.ValueOf(value)

	// Handle pointers
	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil
		}
		val = val.Elem()
	}

	// Only validate structs
	if val.Kind() != reflect.Struct {
		return nil
	}

	return v.validateStruct(val, "")
}

func (v *TagValidator) validateStruct(val reflect.Value, prefix string) error {
	typ := val.Type()

	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		fieldType := typ.Field(i)

		// Skip unexported fields
		if !fieldType.IsExported() {
			continue
		}

		// Get field name for error messages
		fieldName := fieldType.Name
		if prefix != "" {
			fieldName = prefix + "." + fieldName
		}

		// Get tag
		tag := fieldType.Tag.Get(v.tagName)

		// Handle embedded structs
		if field.Kind() == reflect.Struct && tag == "" {
			if err := v.validateStruct(field, fieldName); err != nil {
				return err
			}
			continue
		}

		// Handle pointer to struct
		if field.Kind() == reflect.Ptr && !field.IsNil() {
			elem := field.Elem()
			if elem.Kind() == reflect.Struct && tag == "" {
				if err := v.validateStruct(elem, fieldName); err != nil {
					return err
				}
				continue
			}
		}

		if tag == "" || tag == "-" {
			continue
		}

		// Parse and apply rules
		rules := parseTagRules(tag)
		for _, rule := range rules {
			if err := v.applyRule(field, fieldName, rule); err != nil {
				return err
			}
		}
	}

	return nil
}

// tagRule represents a parsed validation rule
type tagRule struct {
	name  string
	value string
}

// parseTagRules parses a tag string into rules
// Example: "required,min=1,max=100" -> [{required,""}, {min,"1"}, {max,"100"}]
func parseTagRules(tag string) []tagRule {
	var rules []tagRule
	parts := strings.Split(tag, ",")

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		if idx := strings.Index(part, "="); idx >= 0 {
			rules = append(rules, tagRule{
				name:  part[:idx],
				value: part[idx+1:],
			})
		} else {
			rules = append(rules, tagRule{name: part})
		}
	}

	return rules
}

func (v *TagValidator) applyRule(field reflect.Value, fieldName string, rule tagRule) error {
	switch rule.name {
	case "required":
		return v.validateRequired(field, fieldName)
	case "min":
		return v.validateMin(field, fieldName, rule.value)
	case "max":
		return v.validateMax(field, fieldName, rule.value)
	case "enum":
		return v.validateEnum(field, fieldName, rule.value)
	case "pattern":
		return v.validatePattern(field, fieldName, rule.value)
	default:
		// Unknown rule - ignore
		return nil
	}
}

func (v *TagValidator) validateRequired(field reflect.Value, fieldName string) error {
	if field.IsZero() {
		return &ValidationError{
			Field:  fieldName,
			Reason: "field is required",
		}
	}
	return nil
}

func (v *TagValidator) validateMin(field reflect.Value, fieldName, minStr string) error {
	minVal, err := strconv.ParseFloat(minStr, 64)
	if err != nil {
		return nil // Invalid rule, skip
	}

	switch field.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if float64(field.Int()) < minVal {
			return &ValidationError{
				Field:  fieldName,
				Value:  field.Int(),
				Reason: fmt.Sprintf("value must be at least %s", minStr),
			}
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if float64(field.Uint()) < minVal {
			return &ValidationError{
				Field:  fieldName,
				Value:  field.Uint(),
				Reason: fmt.Sprintf("value must be at least %s", minStr),
			}
		}
	case reflect.Float32, reflect.Float64:
		if field.Float() < minVal {
			return &ValidationError{
				Field:  fieldName,
				Value:  field.Float(),
				Reason: fmt.Sprintf("value must be at least %s", minStr),
			}
		}
	case reflect.String:
		if float64(len(field.String())) < minVal {
			return &ValidationError{
				Field:  fieldName,
				Value:  field.String(),
				Reason: fmt.Sprintf("length must be at least %s", minStr),
			}
		}
	case reflect.Slice, reflect.Array, reflect.Map:
		if float64(field.Len()) < minVal {
			return &ValidationError{
				Field:  fieldName,
				Value:  field.Len(),
				Reason: fmt.Sprintf("length must be at least %s", minStr),
			}
		}
	}

	return nil
}

func (v *TagValidator) validateMax(field reflect.Value, fieldName, maxStr string) error {
	maxVal, err := strconv.ParseFloat(maxStr, 64)
	if err != nil {
		return nil // Invalid rule, skip
	}

	switch field.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if float64(field.Int()) > maxVal {
			return &ValidationError{
				Field:  fieldName,
				Value:  field.Int(),
				Reason: fmt.Sprintf("value must be at most %s", maxStr),
			}
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if float64(field.Uint()) > maxVal {
			return &ValidationError{
				Field:  fieldName,
				Value:  field.Uint(),
				Reason: fmt.Sprintf("value must be at most %s", maxStr),
			}
		}
	case reflect.Float32, reflect.Float64:
		if field.Float() > maxVal {
			return &ValidationError{
				Field:  fieldName,
				Value:  field.Float(),
				Reason: fmt.Sprintf("value must be at most %s", maxStr),
			}
		}
	case reflect.String:
		if float64(len(field.String())) > maxVal {
			return &ValidationError{
				Field:  fieldName,
				Value:  field.String(),
				Reason: fmt.Sprintf("length must be at most %s", maxStr),
			}
		}
	case reflect.Slice, reflect.Array, reflect.Map:
		if float64(field.Len()) > maxVal {
			return &ValidationError{
				Field:  fieldName,
				Value:  field.Len(),
				Reason: fmt.Sprintf("length must be at most %s", maxStr),
			}
		}
	}

	return nil
}

func (v *TagValidator) validateEnum(field reflect.Value, fieldName, enumStr string) error {
	// Parse enum values (separated by |)
	allowed := strings.Split(enumStr, "|")

	var value string
	switch field.Kind() {
	case reflect.String:
		value = field.String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		value = strconv.FormatInt(field.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		value = strconv.FormatUint(field.Uint(), 10)
	default:
		return nil // Skip non-comparable types
	}

	for _, a := range allowed {
		if value == strings.TrimSpace(a) {
			return nil
		}
	}

	return &ValidationError{
		Field:  fieldName,
		Value:  value,
		Reason: fmt.Sprintf("value must be one of: %s", enumStr),
	}
}

func (v *TagValidator) validatePattern(field reflect.Value, fieldName, pattern string) error {
	if field.Kind() != reflect.String {
		return nil // Pattern only applies to strings
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil // Invalid pattern, skip
	}

	value := field.String()
	if !re.MatchString(value) {
		return &ValidationError{
			Field:  fieldName,
			Value:  value,
			Reason: fmt.Sprintf("value must match pattern: %s", pattern),
		}
	}

	return nil
}
