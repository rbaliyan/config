package config

import (
	"testing"
)

func TestNewTag(t *testing.T) {
	tests := []struct {
		name      string
		key       string
		value     string
		wantErr   bool
		errSubstr string
	}{
		{"valid tag", "env", "production", false, ""},
		{"valid with hyphen", "my-key", "my-value", false, ""},
		{"valid with underscore", "my_key", "my_value", false, ""},
		{"empty key", "", "value", true, "empty"},
		{"key with comma", "key,name", "value", true, "reserved"},
		{"key with equals", "key=name", "value", true, "reserved"},
		{"value with comma", "key", "val,ue", true, "reserved"},
		{"value with equals", "key", "val=ue", true, "reserved"},
		{"empty value allowed", "key", "", false, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tag, err := NewTag(tt.key, tt.value)
			if tt.wantErr {
				if err == nil {
					t.Errorf("NewTag() expected error containing %q, got nil", tt.errSubstr)
				}
			} else {
				if err != nil {
					t.Errorf("NewTag() unexpected error: %v", err)
				}
				if tag == nil {
					t.Fatal("NewTag() returned nil tag")
				}
				if tag.Key() != tt.key {
					t.Errorf("Key() = %q, want %q", tag.Key(), tt.key)
				}
				if tag.Value() != tt.value {
					t.Errorf("Value() = %q, want %q", tag.Value(), tt.value)
				}
			}
		})
	}
}

func TestMustTag(t *testing.T) {
	// Valid tag should not panic
	tag := MustTag("env", "prod")
	if tag.Key() != "env" || tag.Value() != "prod" {
		t.Error("MustTag returned wrong values")
	}

	// Invalid tag should panic
	defer func() {
		if r := recover(); r == nil {
			t.Error("MustTag should panic on invalid key")
		}
	}()
	MustTag("key,invalid", "value")
}

func TestTagFormat(t *testing.T) {
	tag := MustTag("env", "production")
	if tag.Format() != "env=production" {
		t.Errorf("Format() = %q, want %q", tag.Format(), "env=production")
	}
}

func TestParseTag(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantKey string
		wantVal string
		wantErr bool
	}{
		{"valid", "env=production", "env", "production", false},
		{"empty value", "key=", "key", "", false},
		{"value with special chars", "url=https://example.com", "url", "https://example.com", false},
		{"no equals", "invalid", "", "", true},
		{"empty key", "=value", "", "", true},
		{"multiple equals", "key=val=ue", "key", "val=ue", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tag, err := ParseTag(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Error("ParseTag() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("ParseTag() unexpected error: %v", err)
				return
			}
			if tag.Key() != tt.wantKey {
				t.Errorf("Key() = %q, want %q", tag.Key(), tt.wantKey)
			}
			if tag.Value() != tt.wantVal {
				t.Errorf("Value() = %q, want %q", tag.Value(), tt.wantVal)
			}
		})
	}
}

func TestSortTags(t *testing.T) {
	tags := []Tag{
		MustTag("z", "last"),
		MustTag("a", "first"),
		MustTag("m", "middle"),
	}

	sorted := SortTags(tags)

	if len(sorted) != 3 {
		t.Fatalf("SortTags returned %d tags, want 3", len(sorted))
	}
	if sorted[0].Key() != "a" {
		t.Errorf("First tag key = %q, want %q", sorted[0].Key(), "a")
	}
	if sorted[1].Key() != "m" {
		t.Errorf("Second tag key = %q, want %q", sorted[1].Key(), "m")
	}
	if sorted[2].Key() != "z" {
		t.Errorf("Third tag key = %q, want %q", sorted[2].Key(), "z")
	}

	// Original should be unchanged
	if tags[0].Key() != "z" {
		t.Error("SortTags modified original slice")
	}
}

func TestSortTags_Empty(t *testing.T) {
	sorted := SortTags(nil)
	if sorted != nil {
		t.Error("SortTags(nil) should return nil")
	}

	sorted = SortTags([]Tag{})
	if sorted != nil {
		t.Error("SortTags([]) should return nil")
	}
}

func TestFormatTags(t *testing.T) {
	tests := []struct {
		name string
		tags []Tag
		want string
	}{
		{"empty", nil, ""},
		{"single", []Tag{MustTag("env", "prod")}, "env=prod"},
		{"multiple sorted", []Tag{MustTag("env", "prod"), MustTag("region", "us")}, "env=prod,region=us"},
		{"multiple unsorted", []Tag{MustTag("region", "us"), MustTag("env", "prod")}, "env=prod,region=us"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FormatTags(tt.tags)
			if got != tt.want {
				t.Errorf("FormatTags() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParseTags(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantLen int
		wantErr bool
	}{
		{"empty", "", 0, false},
		{"single", "env=prod", 1, false},
		{"multiple", "env=prod,region=us", 2, false},
		{"invalid format", "invalid", 0, true},
		{"empty key in list", "env=prod,=value", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tags, err := ParseTags(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Error("ParseTags() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("ParseTags() unexpected error: %v", err)
				return
			}
			if len(tags) != tt.wantLen {
				t.Errorf("ParseTags() returned %d tags, want %d", len(tags), tt.wantLen)
			}
		})
	}
}

func TestMatchTags(t *testing.T) {
	tests := []struct {
		name       string
		entryTags  []Tag
		filterTags []Tag
		want       bool
	}{
		{"empty filter matches all", []Tag{MustTag("env", "prod")}, nil, true},
		{"exact match", []Tag{MustTag("env", "prod")}, []Tag{MustTag("env", "prod")}, true},
		{"subset match", []Tag{MustTag("env", "prod"), MustTag("region", "us")}, []Tag{MustTag("env", "prod")}, true},
		{"no match wrong value", []Tag{MustTag("env", "dev")}, []Tag{MustTag("env", "prod")}, false},
		{"no match missing key", []Tag{MustTag("region", "us")}, []Tag{MustTag("env", "prod")}, false},
		{"filter more than entry", []Tag{MustTag("env", "prod")}, []Tag{MustTag("env", "prod"), MustTag("region", "us")}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MatchTags(tt.entryTags, tt.filterTags)
			if got != tt.want {
				t.Errorf("MatchTags() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTagsEqual(t *testing.T) {
	tests := []struct {
		name string
		a    []Tag
		b    []Tag
		want bool
	}{
		{"both nil", nil, nil, true},
		{"both empty", []Tag{}, []Tag{}, true},
		{"nil and empty", nil, []Tag{}, true},
		{"same single", []Tag{MustTag("a", "1")}, []Tag{MustTag("a", "1")}, true},
		{"same multiple", []Tag{MustTag("a", "1"), MustTag("b", "2")}, []Tag{MustTag("a", "1"), MustTag("b", "2")}, true},
		{"same different order", []Tag{MustTag("b", "2"), MustTag("a", "1")}, []Tag{MustTag("a", "1"), MustTag("b", "2")}, true},
		{"different length", []Tag{MustTag("a", "1")}, []Tag{MustTag("a", "1"), MustTag("b", "2")}, false},
		{"different key", []Tag{MustTag("a", "1")}, []Tag{MustTag("b", "1")}, false},
		{"different value", []Tag{MustTag("a", "1")}, []Tag{MustTag("a", "2")}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := TagsEqual(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("TagsEqual() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestValidateTagKey(t *testing.T) {
	tests := []struct {
		key     string
		wantErr bool
	}{
		{"valid", false},
		{"valid-key", false},
		{"valid_key", false},
		{"valid123", false},
		{"", true},
		{"with,comma", true},
		{"with=equals", true},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			err := ValidateTagKey(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateTagKey(%q) error = %v, wantErr %v", tt.key, err, tt.wantErr)
			}
		})
	}
}

func TestValidateTagValue(t *testing.T) {
	tests := []struct {
		value   string
		wantErr bool
	}{
		{"valid", false},
		{"", false}, // empty is allowed
		{"with spaces", false},
		{"with-hyphen", false},
		{"with,comma", true},
		{"with=equals", true},
	}

	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			err := ValidateTagValue(tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateTagValue(%q) error = %v, wantErr %v", tt.value, err, tt.wantErr)
			}
		})
	}
}
