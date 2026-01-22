package config

import (
	"fmt"
	"slices"
	"strings"
)

// Reserved characters that cannot be used in tag keys or values.
// These characters are used as delimiters in the serialized format.
const tagReservedChars = ",="

// Tag represents a key-value tag for categorizing config entries.
// Tags are used to create multiple entries with the same key but different
// tag combinations. The combination of (namespace, key, sorted_tags) uniquely
// identifies an entry.
type Tag interface {
	Key() string
	Value() string
	Format() string // Returns "key=value"
}

// tag is the default implementation of Tag.
type tag struct {
	key   string
	value string
}

func (t *tag) Key() string    { return t.key }
func (t *tag) Value() string  { return t.value }
func (t *tag) Format() string { return t.key + "=" + t.value }

// ValidateTagKey validates a tag key doesn't contain reserved characters.
func ValidateTagKey(key string) error {
	if key == "" {
		return fmt.Errorf("tag key cannot be empty")
	}
	if strings.ContainsAny(key, tagReservedChars) {
		return fmt.Errorf("tag key %q contains reserved characters (comma or equals)", key)
	}
	return nil
}

// ValidateTagValue validates a tag value doesn't contain reserved characters.
func ValidateTagValue(value string) error {
	if strings.ContainsAny(value, tagReservedChars) {
		return fmt.Errorf("tag value %q contains reserved characters (comma or equals)", value)
	}
	return nil
}

// NewTag creates a new tag with the given key and value.
// Returns an error if key or value contain reserved characters (comma or equals).
func NewTag(key, value string) (Tag, error) {
	if err := ValidateTagKey(key); err != nil {
		return nil, err
	}
	if err := ValidateTagValue(value); err != nil {
		return nil, err
	}
	return &tag{key: key, value: value}, nil
}

// MustTag creates a new tag with the given key and value.
// Panics if key or value contain reserved characters (comma or equals).
// Use NewTag for error handling instead of panics.
func MustTag(key, value string) Tag {
	t, err := NewTag(key, value)
	if err != nil {
		panic(err)
	}
	return t
}

// NewTagValidated is an alias for NewTag for backwards compatibility.
// Deprecated: Use NewTag instead.
func NewTagValidated(key, value string) (Tag, error) {
	if err := ValidateTagKey(key); err != nil {
		return nil, err
	}
	if err := ValidateTagValue(value); err != nil {
		return nil, err
	}
	return &tag{key: key, value: value}, nil
}

// ParseTag parses a "key=value" string into a Tag.
func ParseTag(s string) (Tag, error) {
	parts := strings.SplitN(s, "=", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid tag format: %q (expected key=value)", s)
	}
	if parts[0] == "" {
		return nil, fmt.Errorf("invalid tag: empty key")
	}
	// Note: We don't validate for reserved chars in ParseTag since we're parsing
	// already serialized data. The comma delimiter is already consumed by ParseTags.
	// The equals sign is used as the key/value delimiter and handled by SplitN.
	return &tag{key: parts[0], value: parts[1]}, nil
}

// SortTags returns a new slice with tags sorted by key.
func SortTags(tags []Tag) []Tag {
	if len(tags) == 0 {
		return nil
	}
	sorted := make([]Tag, len(tags))
	copy(sorted, tags)
	slices.SortFunc(sorted, func(a, b Tag) int {
		return strings.Compare(a.Key(), b.Key())
	})
	return sorted
}

// FormatTags converts tags to sorted "key1=value1,key2=value2" string.
func FormatTags(tags []Tag) string {
	if len(tags) == 0 {
		return ""
	}
	sorted := SortTags(tags)
	parts := make([]string, len(sorted))
	for i, t := range sorted {
		parts[i] = t.Format()
	}
	return strings.Join(parts, ",")
}

// ParseTags parses "key1=value1,key2=value2" string into sorted []Tag.
func ParseTags(s string) ([]Tag, error) {
	if s == "" {
		return nil, nil
	}
	parts := strings.Split(s, ",")
	tags := make([]Tag, 0, len(parts))
	for _, p := range parts {
		t, err := ParseTag(p)
		if err != nil {
			return nil, err
		}
		tags = append(tags, t)
	}
	return SortTags(tags), nil
}

// MatchTags returns true if entryTags contain all filterTags (AND logic, exact match).
// If filterTags is empty, it always returns true.
func MatchTags(entryTags, filterTags []Tag) bool {
	if len(filterTags) == 0 {
		return true
	}
	entryMap := make(map[string]string, len(entryTags))
	for _, t := range entryTags {
		entryMap[t.Key()] = t.Value()
	}
	for _, ft := range filterTags {
		if entryMap[ft.Key()] != ft.Value() {
			return false
		}
	}
	return true
}

// TagsEqual returns true if two tag slices are identical (same keys and values).
func TagsEqual(a, b []Tag) bool {
	if len(a) != len(b) {
		return false
	}
	if len(a) == 0 {
		return true
	}
	sortedA := SortTags(a)
	sortedB := SortTags(b)
	for i := range sortedA {
		if sortedA[i].Key() != sortedB[i].Key() || sortedA[i].Value() != sortedB[i].Value() {
			return false
		}
	}
	return true
}
