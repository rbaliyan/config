package config_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

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

// --- TypeSecret / NewSecretValue / SecretFrom ---

func TestTypeSecret_Enum(t *testing.T) {
	if got := config.TypeSecret.String(); got != "secret" {
		t.Errorf("TypeSecret.String() = %q, want \"secret\"", got)
	}
	if got := config.ParseType("secret"); got != config.TypeSecret {
		t.Errorf("ParseType(\"secret\") = %v, want TypeSecret", got)
	}
}

func TestNewSecretValue_Attributes(t *testing.T) {
	s := config.NewSecret("my-token")
	v := config.NewSecretValue(s)

	if got := v.Type(); got != config.TypeSecret {
		t.Errorf("Type() = %v, want TypeSecret", got)
	}
	if got := v.Codec(); got != "secret" {
		t.Errorf("Codec() = %q, want \"secret\"", got)
	}
	str, err := v.String()
	if err != nil {
		t.Fatalf("String(): %v", err)
	}
	if str != "******" {
		t.Errorf("String() = %q, want \"******\"", str)
	}
}

func TestNewSecretValue_NilSafe(t *testing.T) {
	v := config.NewSecretValue(nil)
	if got := v.Type(); got != config.TypeSecret {
		t.Errorf("Type() = %v, want TypeSecret", got)
	}
	str, err := v.String()
	if err != nil {
		t.Fatalf("String(): %v", err)
	}
	if str != "******" {
		t.Errorf("String() = %q, want \"******\"", str)
	}
}

// TestNewSecretValue_Marshal verifies Marshal returns the real bytes, not the mask.
func TestNewSecretValue_Marshal(t *testing.T) {
	ctx := context.Background()
	v := config.NewSecretValue(config.NewSecret("real-password"))

	data, err := v.Marshal(ctx)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if string(data) == "******" {
		t.Error("Marshal returned the mask instead of actual bytes")
	}
	if string(data) != "real-password" {
		t.Errorf("Marshal = %q, want \"real-password\"", data)
	}
}

func TestSecretFrom_RoundTrip(t *testing.T) {
	ctx := context.Background()
	original := config.NewSecret("round-trip-secret")
	v := config.NewSecretValue(original)

	got, err := config.SecretFrom(ctx, v)
	if err != nil {
		t.Fatalf("SecretFrom: %v", err)
	}
	defer got.Wipe()

	if !original.Equal(got) {
		t.Errorf("SecretFrom returned wrong bytes: got %q", got.Bytes())
	}
}

func TestSecretFrom_WrongType(t *testing.T) {
	ctx := context.Background()
	v := config.NewValue("not-a-secret")
	if _, err := config.SecretFrom(ctx, v); !errors.Is(err, config.ErrTypeMismatch) {
		t.Errorf("SecretFrom(TypeString) err = %v, want ErrTypeMismatch", err)
	}
}

func TestSecretFrom_Nil(t *testing.T) {
	ctx := context.Background()
	if _, err := config.SecretFrom(ctx, nil); !errors.Is(err, config.ErrNotFound) {
		t.Errorf("SecretFrom(nil) err = %v, want ErrNotFound", err)
	}
}

func TestSecretValue_Unmarshal(t *testing.T) {
	ctx := context.Background()
	original := config.NewSecret("my-api-key")
	v := config.NewSecretValue(original)

	var got *config.Secret
	if err := v.Unmarshal(ctx, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got == nil {
		t.Fatal("Unmarshal produced nil *Secret")
	}
	defer got.Wipe()
	if !original.Equal(got) {
		t.Errorf("Unmarshal bytes mismatch: got %q", got.Bytes())
	}
}

func TestSecretValue_UnmarshalWrongTarget(t *testing.T) {
	ctx := context.Background()
	v := config.NewSecretValue(config.NewSecret("token"))

	var s string
	if err := v.Unmarshal(ctx, &s); !errors.Is(err, config.ErrTypeMismatch) {
		t.Errorf("Unmarshal into *string err = %v, want ErrTypeMismatch", err)
	}
	var m map[string]any
	if err := v.Unmarshal(ctx, &m); !errors.Is(err, config.ErrTypeMismatch) {
		t.Errorf("Unmarshal into map err = %v, want ErrTypeMismatch", err)
	}
}

// TestDetectType_Secret verifies NewValue(*Secret) detects TypeSecret and masks String().
func TestDetectType_Secret(t *testing.T) {
	s := config.NewSecret("detect-me")
	v := config.NewValue(s)

	if got := v.Type(); got != config.TypeSecret {
		t.Errorf("NewValue(*Secret).Type() = %v, want TypeSecret", got)
	}
	str, err := v.String()
	if err != nil {
		t.Fatalf("String(): %v", err)
	}
	if str != "******" {
		t.Errorf("NewValue(*Secret).String() = %q, want \"******\"", str)
	}
}

func TestSecretValue_TypeConversionsUnsupported(t *testing.T) {
	v := config.NewSecretValue(config.NewSecret("secret-bytes"))
	if _, err := v.Int64(); err == nil {
		t.Error("Int64() on TypeSecret should return error")
	}
	if _, err := v.Float64(); err == nil {
		t.Error("Float64() on TypeSecret should return error")
	}
	if _, err := v.Bool(); err == nil {
		t.Error("Bool() on TypeSecret should return error")
	}
}

// TestSecretValue_DBReadback simulates how store backends reconstruct a TypeSecret value
// from persisted bytes (the path taken by PostgreSQL, MongoDB, Redis, and SQLite).
// When codec="raw" is not registered, NewValueFromBytes falls through to NewRawValue,
// which preserves the bytes and respects WithValueType(TypeSecret).
func TestSecretValue_DBReadback(t *testing.T) {
	ctx := context.Background()
	plaintext := "db-stored-secret"

	v, err := config.NewValueFromBytes(ctx, []byte(plaintext), "secret",
		config.WithValueType(config.TypeSecret))
	if err != nil {
		t.Fatalf("NewValueFromBytes: %v", err)
	}

	if got := v.Type(); got != config.TypeSecret {
		t.Errorf("Type() = %v, want TypeSecret", got)
	}
	str, err := v.String()
	if err != nil {
		t.Fatalf("String(): %v", err)
	}
	if str != "******" {
		t.Errorf("DBReadback String() = %q, plaintext leaked", str)
	}
	if str == plaintext {
		t.Error("String() returned plaintext — secret leaked")
	}

	got, err := config.SecretFrom(ctx, v)
	if err != nil {
		t.Fatalf("SecretFrom after DBReadback: %v", err)
	}
	defer got.Wipe()
	if string(got.Bytes()) != plaintext {
		t.Errorf("SecretFrom bytes = %q, want %q", got.Bytes(), plaintext)
	}
}

// TestSecretValue_WipeIndependence verifies that wiping an extracted *Secret
// does not corrupt the Value's backing bytes for subsequent extractions.
func TestSecretValue_WipeIndependence(t *testing.T) {
	ctx := context.Background()
	v := config.NewSecretValue(config.NewSecret("sensitive"))

	first, err := config.SecretFrom(ctx, v)
	if err != nil {
		t.Fatalf("first SecretFrom: %v", err)
	}
	first.Wipe()

	second, err := config.SecretFrom(ctx, v)
	if err != nil {
		t.Fatalf("second SecretFrom after wipe: %v", err)
	}
	defer second.Wipe()

	if second.IsZero() {
		t.Error("value lost its bytes after caller wiped the first extraction")
	}
	if string(second.Bytes()) != "sensitive" {
		t.Errorf("second extraction = %q, want \"sensitive\"", second.Bytes())
	}
}

func TestNewSecretValue_WithOptions(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	v := config.NewSecretValue(config.NewSecret("tok"),
		config.WithValueMetadata(7, now, now))

	if got := v.Metadata().Version(); got != 7 {
		t.Errorf("Version() = %d, want 7", got)
	}
	if got := v.Type(); got != config.TypeSecret {
		t.Errorf("Type() = %v after WithValueMetadata, want TypeSecret", got)
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
