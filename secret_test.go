package config_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
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

func TestSecret_Bytes(t *testing.T) {
	s := config.NewSecret("p@ssw0rd")
	if got := string(s.Bytes()); got != "p@ssw0rd" {
		t.Errorf("Bytes() = %q, want p@ssw0rd", got)
	}
}

func TestSecret_NewSecretBytes(t *testing.T) {
	input := []byte("hello")
	s := config.NewSecretBytes(input)
	if string(s.Bytes()) != "hello" {
		t.Errorf("NewSecretBytes: Bytes() = %q, want hello", s.Bytes())
	}
	// Mutating the input must not affect the Secret.
	input[0] = 'X'
	if string(s.Bytes()) != "hello" {
		t.Error("NewSecretBytes does not copy input")
	}
}

func TestSecret_Clone(t *testing.T) {
	s := config.NewSecret("original")
	c := s.Clone()
	if !s.Equal(c) {
		t.Error("Clone() not equal to original")
	}
	s.Wipe()
	if c.IsZero() {
		t.Error("Wipe of source should not affect clone")
	}
}

func TestSecret_Equal(t *testing.T) {
	a := config.NewSecret("same")
	b := config.NewSecret("same")
	c := config.NewSecret("diff")
	if !a.Equal(b) {
		t.Error("Equal secrets report not equal")
	}
	if a.Equal(c) {
		t.Error("Different secrets report equal")
	}
	var zero *config.Secret
	if !zero.Equal(nil) {
		t.Error("Two nil secrets should be equal")
	}
}

func TestSecret_Zero(t *testing.T) {
	var s config.Secret
	if got := s.Bytes(); len(got) != 0 {
		t.Errorf("zero Bytes() = %q, want empty", got)
	}
	if got := s.String(); got != "******" {
		t.Errorf("zero String() = %q, want ******", got)
	}
	if !s.IsZero() {
		t.Error("zero IsZero() = false, want true")
	}
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
	var s config.Secret
	if err := s.UnmarshalText([]byte("******")); err != nil {
		t.Fatalf("UnmarshalText: %v", err)
	}
	if got := s.Bytes(); len(got) != 0 {
		t.Errorf("Bytes() after UnmarshalText(mask) = %q, want empty", got)
	}
	if !s.IsZero() {
		t.Error("Secret after UnmarshalText(mask) should be zero")
	}
}

func TestSecret_JSONEncoding(t *testing.T) {
	type cfg struct {
		Password *config.Secret `json:"password"`
	}

	raw := []byte(`{"password":"p@ssw0rd"}`)
	var c cfg
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got := string(c.Password.Bytes()); got != "p@ssw0rd" {
		t.Errorf("Bytes() after Unmarshal = %q, want p@ssw0rd", got)
	}

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

func TestSecret_MarshalJSON(t *testing.T) {
	s := config.NewSecret("tok")
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `"******"` {
		t.Errorf("MarshalJSON = %s, want %q", b, `"******"`)
	}
}

func TestSecret_LogValue(t *testing.T) {
	s := config.NewSecret("secret-token")
	val := s.LogValue()
	if val.Kind() != slog.KindString {
		t.Errorf("LogValue kind = %v, want String", val.Kind())
	}
	if val.String() != "******" {
		t.Errorf("LogValue = %q, want ******", val.String())
	}
}

func TestSecret_SQLDriverValuer(t *testing.T) {
	s := config.NewSecret("db-pass")
	v, err := s.Value()
	if err != nil {
		t.Fatalf("Value: %v", err)
	}
	b, ok := v.([]byte)
	if !ok {
		t.Fatalf("Value type = %T, want []byte", v)
	}
	if string(b) != "db-pass" {
		t.Errorf("Value bytes = %q, want db-pass", b)
	}

	var zero config.Secret
	zv, _ := zero.Value()
	if zv != nil {
		t.Errorf("zero Value() = %v, want nil", zv)
	}
}

func TestSecret_SQLScanner(t *testing.T) {
	var s config.Secret

	if err := s.Scan([]byte("scanned")); err != nil {
		t.Fatalf("Scan([]byte): %v", err)
	}
	if string(s.Bytes()) != "scanned" {
		t.Errorf("after Scan([]byte): Bytes() = %q, want scanned", s.Bytes())
	}

	if err := s.Scan("from-string"); err != nil {
		t.Fatalf("Scan(string): %v", err)
	}
	if string(s.Bytes()) != "from-string" {
		t.Errorf("after Scan(string): Bytes() = %q, want from-string", s.Bytes())
	}

	if err := s.Scan(nil); err != nil {
		t.Fatalf("Scan(nil): %v", err)
	}
	if !s.IsZero() {
		t.Error("after Scan(nil): IsZero() = false")
	}

	if err := s.Scan(42); err == nil {
		t.Error("Scan(int) should return error")
	}
}

func TestSecret_DriverRoundTrip(t *testing.T) {
	original := config.NewSecret("round-trip")
	v, err := original.Value()
	if err != nil {
		t.Fatalf("Value: %v", err)
	}

	var restored config.Secret
	if err := restored.Scan(v); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !original.Equal(&restored) {
		t.Errorf("round-trip mismatch: got %q", restored.Bytes())
	}
}

func TestSecret_Wipe(t *testing.T) {
	s := config.NewSecret("sensitive-data")
	if s.IsZero() {
		t.Fatal("IsZero() = true before Wipe, want false")
	}
	if string(s.Bytes()) != "sensitive-data" {
		t.Fatalf("Bytes() = %q before Wipe, want sensitive-data", s.Bytes())
	}

	s.Wipe()

	if !s.IsZero() {
		t.Error("IsZero() = false after Wipe, want true")
	}
	if got := s.Bytes(); len(got) != 0 {
		t.Errorf("Bytes() = %q after Wipe, want empty", got)
	}

	// Wipe on an already-zero secret must not panic.
	s.Wipe()
}

func TestSecret_CodecJSON(t *testing.T) {
	ctx := context.Background()
	c := codec.Get("json")
	if c == nil {
		t.Fatal("json codec not registered")
	}

	type cfg struct {
		Token *config.Secret `json:"token"`
	}

	var got cfg
	if err := c.Decode(ctx, []byte(`{"token":"bearer-abc"}`), &got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if string(got.Token.Bytes()) != "bearer-abc" {
		t.Errorf("Bytes() = %q, want bearer-abc", got.Token.Bytes())
	}

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
		Token *config.Secret `yaml:"token"`
	}

	yamlData := []byte("token: bearer-xyz\n")
	var got cfg
	if err := c.Decode(ctx, yamlData, &got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if string(got.Token.Bytes()) != "bearer-xyz" {
		t.Errorf("Bytes() = %q, want bearer-xyz", got.Token.Bytes())
	}

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
