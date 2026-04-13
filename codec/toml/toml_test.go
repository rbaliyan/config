package toml

import (
	"context"
	"testing"

	"github.com/rbaliyan/config/codec"
)

func TestNew(t *testing.T) {
	c := New()
	if c == nil {
		t.Fatal("New() returned nil")
	}
	if c.Name() != "toml" {
		t.Errorf("Name() = %q, want %q", c.Name(), "toml")
	}
}

func TestCodecInterface(t *testing.T) {
	var _ codec.Codec = New()
}

func TestRoundTrip(t *testing.T) {
	c := New()

	type Config struct {
		Host string `toml:"host"`
		Port int    `toml:"port"`
	}

	original := Config{Host: "localhost", Port: 8080}
	ctx := context.Background()
	data, err := c.Encode(ctx, original)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	var got Config
	if err := c.Decode(ctx, data, &got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got != original {
		t.Errorf("got %+v, want %+v", got, original)
	}
}

func TestRegisteredViaInit(t *testing.T) {
	c := codec.Get("toml")
	if c == nil {
		t.Fatal("toml codec not registered via init()")
	}
}
