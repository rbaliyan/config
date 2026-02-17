package mongodb_test

import (
	"testing"

	"github.com/rbaliyan/config/mongodb"
)

func TestBSONCodec_Name(t *testing.T) {
	c := mongodb.BSON()
	if c.Name() != "bson" {
		t.Errorf("expected name 'bson', got %q", c.Name())
	}
}

func TestBSONCodec_RoundTrip(t *testing.T) {
	c := mongodb.BSON()

	tests := []struct {
		name  string
		input any
		want  any
	}{
		{"int32", int32(42), int32(0)},
		{"int64", int64(9999999999), int64(0)},
		{"float64", 3.14, float64(0)},
		{"string", "hello world", ""},
		{"bool_true", true, false},
		{"bool_false", false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := c.Encode(tt.input)
			if err != nil {
				t.Fatalf("Encode(%v): %v", tt.input, err)
			}
			if len(data) == 0 {
				t.Fatal("Encode returned empty data")
			}

			// Decode into a pointer of the same type
			switch v := tt.input.(type) {
			case int32:
				var out int32
				if err := c.Decode(data, &out); err != nil {
					t.Fatalf("Decode: %v", err)
				}
				if out != v {
					t.Errorf("got %v, want %v", out, v)
				}
			case int64:
				var out int64
				if err := c.Decode(data, &out); err != nil {
					t.Fatalf("Decode: %v", err)
				}
				if out != v {
					t.Errorf("got %v, want %v", out, v)
				}
			case float64:
				var out float64
				if err := c.Decode(data, &out); err != nil {
					t.Fatalf("Decode: %v", err)
				}
				if out != v {
					t.Errorf("got %v, want %v", out, v)
				}
			case string:
				var out string
				if err := c.Decode(data, &out); err != nil {
					t.Fatalf("Decode: %v", err)
				}
				if out != v {
					t.Errorf("got %q, want %q", out, v)
				}
			case bool:
				var out bool
				if err := c.Decode(data, &out); err != nil {
					t.Fatalf("Decode: %v", err)
				}
				if out != v {
					t.Errorf("got %v, want %v", out, v)
				}
			}
		})
	}
}

func TestBSONCodec_Document(t *testing.T) {
	c := mongodb.BSON()

	type Config struct {
		Host string `bson:"host"`
		Port int    `bson:"port"`
	}

	input := Config{Host: "localhost", Port: 5432}
	data, err := c.Encode(input)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	var out Config
	if err := c.Decode(data, &out); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if out != input {
		t.Errorf("got %+v, want %+v", out, input)
	}
}

func TestBSONCodec_DecodeEmptyData(t *testing.T) {
	c := mongodb.BSON()
	var out string
	err := c.Decode(nil, &out)
	if err == nil {
		t.Error("expected error for empty data")
	}
}

func TestBSONCodec_TypeByte(t *testing.T) {
	c := mongodb.BSON()

	// Encode a string and verify the type byte is 0x02 (BSON string type)
	data, err := c.Encode("test")
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if data[0] != 0x02 {
		t.Errorf("expected type byte 0x02 for string, got 0x%02x", data[0])
	}

	// Encode an int32 and verify the type byte is 0x10 (BSON int32 type)
	data, err = c.Encode(int32(1))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if data[0] != 0x10 {
		t.Errorf("expected type byte 0x10 for int32, got 0x%02x", data[0])
	}
}
