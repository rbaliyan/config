package config_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/codec"
	_ "github.com/rbaliyan/config/codec/json"
	_ "github.com/rbaliyan/config/codec/yaml"
)

func TestSecret_Masking(t *testing.T) {
	s := config.NewSecret("p@ssw0rd")

	if got := s.String(); got != "******" {
		t.Errorf("String() = %q, want ******", got)
	}
	if got := fmt.Sprintf("%s", s); got != "******" {
		t.Errorf("%%s = %q, want ******", got)
	}
	if got := fmt.Sprintf("%v", s); got != "******" {
		t.Errorf("%%v = %q, want ******", got)
	}
	if got := fmt.Sprintf("%#v", s); got != `config.Secret("******")` {
		t.Errorf("%%#v = %q, want config.Secret(\"******\")", got)
	}
}

func TestSecret_Value(t *testing.T) {
	s := config.NewSecret("p@ssw0rd")
	if got := s.Value(); got != "p@ssw0rd" {
		t.Errorf("Value() = %q, want p@ssw0rd", got)
	}
}

func TestSecret_Zero(t *testing.T) {
	var s config.Secret
	if got := s.Value(); got != "" {
		t.Errorf("zero Value() = %q, want empty", got)
	}
	if got := s.String(); got != "******" {
		t.Errorf("zero String() = %q, want ******", got)
	}
	if !s.IsZero() {
		t.Error("zero IsZero() = false, want true")
	}
	// Zero secret must marshal as empty string, not "******".
	b, err := s.MarshalText()
	if err != nil {
		t.Fatalf("MarshalText: %v", err)
	}
	if string(b) != "" {
		t.Errorf("zero MarshalText() = %q, want empty string", string(b))
	}
}

func TestSecret_IsZero(t *testing.T) {
	if config.NewSecret("x").IsZero() {
		t.Error("non-zero secret reported IsZero() = true")
	}
	if !config.NewSecret("").IsZero() {
		t.Error("empty-string secret reported IsZero() = false")
	}
}

func TestSecret_UnmarshalMask(t *testing.T) {
	// Feeding the mask back in must not silently preserve "******" as the value.
	var s config.Secret
	if err := s.UnmarshalText([]byte("******")); err != nil {
		t.Fatalf("UnmarshalText: %v", err)
	}
	if got := s.Value(); got != "" {
		t.Errorf("Value() after UnmarshalText(mask) = %q, want empty", got)
	}
	if !s.IsZero() {
		t.Error("Secret after UnmarshalText(mask) should be zero")
	}
}

func TestSecret_JSONEncoding(t *testing.T) {
	type cfg struct {
		Password config.Secret `json:"password"`
	}

	// Unmarshal: JSON string → Secret populates Value().
	raw := []byte(`{"password":"p@ssw0rd"}`)
	var c cfg
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got := c.Password.Value(); got != "p@ssw0rd" {
		t.Errorf("Value() after Unmarshal = %q, want p@ssw0rd", got)
	}

	// Marshal: Secret → JSON string containing "******", not real value.
	out, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if strings.Contains(string(out), "p@ssw0rd") {
		t.Errorf("Marshal leaked secret: %s", out)
	}
	if !strings.Contains(string(out), "******") {
		t.Errorf("Marshal does not contain mask: %s", out)
	}
}

func TestSecret_CodecJSON(t *testing.T) {
	ctx := context.Background()
	c := codec.Get("json")
	if c == nil {
		t.Fatal("json codec not registered")
	}

	type cfg struct {
		Token config.Secret `json:"token"`
	}

	// Decode from a plain JSON payload — Secret.Value() holds the real string.
	var got cfg
	if err := c.Decode(ctx, []byte(`{"token":"bearer-abc"}`), &got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Token.Value() != "bearer-abc" {
		t.Errorf("Value() = %q, want bearer-abc", got.Token.Value())
	}

	// Encode — output must not contain the real token.
	encoded, err := c.Encode(ctx, got)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if strings.Contains(string(encoded), "bearer-abc") {
		t.Errorf("encoded JSON leaked secret: %s", encoded)
	}
}

func TestSecret_CodecYAML(t *testing.T) {
	ctx := context.Background()
	c := codec.Get("yaml")
	if c == nil {
		t.Fatal("yaml codec not registered")
	}

	type cfg struct {
		Token config.Secret `yaml:"token"`
	}

	// Decode from YAML.
	yamlData := []byte("token: bearer-xyz\n")
	var got cfg
	if err := c.Decode(ctx, yamlData, &got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Token.Value() != "bearer-xyz" {
		t.Errorf("Value() = %q, want bearer-xyz", got.Token.Value())
	}

	// Encode — output must not contain the real token.
	encoded, err := c.Encode(ctx, got)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if strings.Contains(string(encoded), "bearer-xyz") {
		t.Errorf("encoded YAML leaked secret: %s", encoded)
	}
	if !strings.Contains(string(encoded), "******") {
		t.Errorf("encoded YAML does not contain mask: %s", encoded)
	}
}
