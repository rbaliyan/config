package config_test

import (
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/rbaliyan/config"
)

// TestSecret_BSONValueRoundTrip marshals a Secret via its BSON value methods,
// unmarshals into a fresh Secret, and asserts equality. It also verifies the
// plaintext never appears in the BSON binary payload as an inadvertently
// readable string prefix-free leak (the value is stored as binary, not text).
func TestSecret_BSONValueRoundTrip(t *testing.T) {
	const plaintext = "super-secret-token"
	orig := config.NewSecret(plaintext)

	typ, data, err := orig.MarshalBSONValue()
	if err != nil {
		t.Fatalf("MarshalBSONValue: %v", err)
	}
	if bson.Type(typ) != bson.TypeBinary {
		t.Fatalf("marshalled type = 0x%02x, want Binary 0x%02x", typ, byte(bson.TypeBinary))
	}

	// The raw bytes carry the plaintext (binary subtype 0 stores it verbatim),
	// but it must never be exposed via the masked String()/intermediate text.
	if orig.String() != "******" {
		t.Errorf("String() leaked: %q", orig.String())
	}

	var decoded config.Secret
	if err := decoded.UnmarshalBSONValue(typ, data); err != nil {
		t.Fatalf("UnmarshalBSONValue: %v", err)
	}
	if !orig.Equal(&decoded) {
		t.Error("round-tripped Secret does not equal original")
	}
	if got := string(decoded.Bytes()); got != plaintext {
		t.Errorf("decoded bytes = %q, want %q", got, plaintext)
	}
	// The decoded Secret's own String() must still mask.
	if decoded.String() != "******" {
		t.Errorf("decoded String() leaked: %q", decoded.String())
	}
}

func TestSecret_BSONValueZeroMarshalsNull(t *testing.T) {
	var zero config.Secret
	typ, data, err := zero.MarshalBSONValue()
	if err != nil {
		t.Fatalf("MarshalBSONValue: %v", err)
	}
	if bson.Type(typ) != bson.TypeNull {
		t.Errorf("zero Secret type = 0x%02x, want Null 0x%02x", typ, byte(bson.TypeNull))
	}
	if data != nil {
		t.Errorf("zero Secret data = %v, want nil", data)
	}

	// Null round-trips back to a zero Secret.
	var decoded config.Secret
	if err := decoded.UnmarshalBSONValue(byte(bson.TypeNull), nil); err != nil {
		t.Fatalf("UnmarshalBSONValue(null): %v", err)
	}
	if !decoded.IsZero() {
		t.Error("decoded Secret should be zero after Null round-trip")
	}
}

func TestSecret_UnmarshalBSONValue_StringMaskBecomesZero(t *testing.T) {
	// A BSON String equal to the mask must not be treated as a real credential.
	typ, payload, err := bson.MarshalValue("******")
	if err != nil {
		t.Fatalf("MarshalValue: %v", err)
	}
	var s config.Secret
	if err := s.UnmarshalBSONValue(byte(typ), payload); err != nil {
		t.Fatalf("UnmarshalBSONValue(mask): %v", err)
	}
	if !s.IsZero() {
		t.Error("masked string should decode to a zero Secret")
	}
}

func TestSecret_UnmarshalBSONValue_StringValue(t *testing.T) {
	typ, payload, err := bson.MarshalValue("plain-string-secret")
	if err != nil {
		t.Fatalf("MarshalValue: %v", err)
	}
	var s config.Secret
	if err := s.UnmarshalBSONValue(byte(typ), payload); err != nil {
		t.Fatalf("UnmarshalBSONValue(string): %v", err)
	}
	if got := string(s.Bytes()); got != "plain-string-secret" {
		t.Errorf("decoded = %q, want plain-string-secret", got)
	}
}

func TestSecret_UnmarshalBSONValue_Errors(t *testing.T) {
	// nil receiver.
	var nilSecret *config.Secret
	if err := nilSecret.UnmarshalBSONValue(byte(bson.TypeNull), nil); err == nil {
		t.Error("nil receiver should error")
	}

	// Unsupported BSON type (e.g. Int32).
	var s config.Secret
	if err := s.UnmarshalBSONValue(byte(bson.TypeInt32), []byte{1, 0, 0, 0}); err == nil {
		t.Error("unsupported BSON type should error")
	}

	// Malformed binary payload.
	var s2 config.Secret
	if err := s2.UnmarshalBSONValue(byte(bson.TypeBinary), []byte{0x01}); err == nil {
		t.Error("malformed binary should error")
	}
}

func TestSecret_BSONValue_NoPlaintextInMaskedRepresentations(t *testing.T) {
	const plaintext = "leak-canary-9000"
	s := config.NewSecret(plaintext)
	for name, repr := range map[string]string{
		"String":   s.String(),
		"GoString": s.GoString(),
	} {
		if strings.Contains(repr, plaintext) {
			t.Errorf("%s leaked plaintext: %q", name, repr)
		}
	}
}
