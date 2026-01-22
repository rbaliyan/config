package codec

import (
	"testing"
)

func TestRegisterAndGet(t *testing.T) {
	// JSON, YAML, TOML are registered at init

	// Test Get returns registered codecs
	jsonCodec := Get("json")
	if jsonCodec == nil {
		t.Error("expected json codec to be registered")
	}
	if jsonCodec.Name() != "json" {
		t.Errorf("expected name 'json', got %q", jsonCodec.Name())
	}

	yamlCodec := Get("yaml")
	if yamlCodec == nil {
		t.Error("expected yaml codec to be registered")
	}

	tomlCodec := Get("toml")
	if tomlCodec == nil {
		t.Error("expected toml codec to be registered")
	}

	// Test Get returns nil for unknown codec
	if Get("unknown") != nil {
		t.Error("expected nil for unknown codec")
	}
}

func TestDefault(t *testing.T) {
	def := Default()
	if def == nil {
		t.Fatal("expected default codec to not be nil")
	}
	if def.Name() != "json" {
		t.Errorf("expected default codec to be 'json', got %q", def.Name())
	}
}

func TestNames(t *testing.T) {
	names := Names()
	if len(names) < 3 {
		t.Errorf("expected at least 3 codecs, got %d", len(names))
	}

	// Check that json, yaml, toml are present
	nameMap := make(map[string]bool)
	for _, n := range names {
		nameMap[n] = true
	}

	for _, expected := range []string{"json", "yaml", "toml"} {
		if !nameMap[expected] {
			t.Errorf("expected codec %q to be in names", expected)
		}
	}
}

func TestRegisterPanics(t *testing.T) {
	// Test nil codec panic
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for nil codec")
		}
	}()
	Register(nil)
}

type emptyNameCodec struct{}

func (e emptyNameCodec) Name() string                      { return "" }
func (e emptyNameCodec) Encode(v any) ([]byte, error)      { return nil, nil }
func (e emptyNameCodec) Decode(data []byte, v any) error   { return nil }

func TestRegisterEmptyNamePanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for empty name codec")
		}
	}()
	Register(emptyNameCodec{})
}

func TestJSONCodec(t *testing.T) {
	c := Get("json")
	if c == nil {
		t.Fatal("json codec not found")
	}

	// Test encoding
	data, err := c.Encode(map[string]int{"a": 1, "b": 2})
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	// Test decoding
	var result map[string]int
	if err := c.Decode(data, &result); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if result["a"] != 1 || result["b"] != 2 {
		t.Errorf("unexpected result: %v", result)
	}
}

func TestYAMLCodec(t *testing.T) {
	c := Get("yaml")
	if c == nil {
		t.Fatal("yaml codec not found")
	}

	// Test encoding
	data, err := c.Encode(map[string]string{"name": "test"})
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	// Test decoding
	var result map[string]string
	if err := c.Decode(data, &result); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if result["name"] != "test" {
		t.Errorf("unexpected result: %v", result)
	}
}

func TestTOMLCodec(t *testing.T) {
	c := Get("toml")
	if c == nil {
		t.Fatal("toml codec not found")
	}

	// Test encoding
	data, err := c.Encode(map[string]any{"enabled": true, "count": 42})
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	// Test decoding
	var result map[string]any
	if err := c.Decode(data, &result); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if result["enabled"] != true {
		t.Errorf("unexpected enabled: %v", result["enabled"])
	}
}

func TestCodecRoundTrip(t *testing.T) {
	type Config struct {
		Name    string   `json:"name" yaml:"name" toml:"name"`
		Port    int      `json:"port" yaml:"port" toml:"port"`
		Enabled bool     `json:"enabled" yaml:"enabled" toml:"enabled"`
		Tags    []string `json:"tags" yaml:"tags" toml:"tags"`
	}

	original := Config{
		Name:    "test-service",
		Port:    8080,
		Enabled: true,
		Tags:    []string{"api", "v2"},
	}

	for _, name := range []string{"json", "yaml", "toml"} {
		t.Run(name, func(t *testing.T) {
			c := Get(name)
			if c == nil {
				t.Skipf("%s codec not available", name)
			}

			// Encode
			data, err := c.Encode(original)
			if err != nil {
				t.Fatalf("encode error: %v", err)
			}

			// Decode
			var result Config
			if err := c.Decode(data, &result); err != nil {
				t.Fatalf("decode error: %v", err)
			}

			// Compare
			if result.Name != original.Name {
				t.Errorf("name mismatch: got %q, want %q", result.Name, original.Name)
			}
			if result.Port != original.Port {
				t.Errorf("port mismatch: got %d, want %d", result.Port, original.Port)
			}
			if result.Enabled != original.Enabled {
				t.Errorf("enabled mismatch: got %v, want %v", result.Enabled, original.Enabled)
			}
			if len(result.Tags) != len(original.Tags) {
				t.Errorf("tags length mismatch: got %d, want %d", len(result.Tags), len(original.Tags))
			}
		})
	}
}
