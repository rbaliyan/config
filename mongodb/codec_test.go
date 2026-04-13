package mongodb

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/rbaliyan/config/codec"
)

// --- stringBSONAdapter tests ---

func TestStringBSONAdapter_ToBSON(t *testing.T) {
	a := stringBSONAdapter{}
	rv, err := a.ToBSON([]byte(`{"key":"value"}`))
	if err != nil {
		t.Fatalf("ToBSON: %v", err)
	}
	if rv.Type != bson.TypeString {
		t.Errorf("expected TypeString, got %v", rv.Type)
	}
	var s string
	if err := rv.Unmarshal(&s); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if s != `{"key":"value"}` {
		t.Errorf("got %q, want %q", s, `{"key":"value"}`)
	}
}

func TestStringBSONAdapter_FromBSON_String(t *testing.T) {
	a := stringBSONAdapter{}
	// Create a BSON string value
	bsonType, bsonVal, err := bson.MarshalValue("hello")
	if err != nil {
		t.Fatal(err)
	}
	rv := bson.RawValue{Type: bsonType, Value: bsonVal}

	data, err := a.FromBSON(rv)
	if err != nil {
		t.Fatalf("FromBSON: %v", err)
	}
	if string(data) != "hello" {
		t.Errorf("got %q, want %q", data, "hello")
	}
}

func TestStringBSONAdapter_FromBSON_LegacyBinData(t *testing.T) {
	a := stringBSONAdapter{}
	// Simulate legacy BinData storage
	legacyData := []byte(`{"key":"value"}`)
	bsonType, bsonVal, err := bson.MarshalValue(legacyData)
	if err != nil {
		t.Fatal(err)
	}
	rv := bson.RawValue{Type: bsonType, Value: bsonVal}

	got, err := a.FromBSON(rv)
	if err != nil {
		t.Fatalf("FromBSON legacy: %v", err)
	}
	if string(got) != string(legacyData) {
		t.Errorf("legacy fallback: got %q, want %q", got, legacyData)
	}
}

func TestStringBSONAdapter_RoundTrip(t *testing.T) {
	a := stringBSONAdapter{}
	original := []byte("name: test\nport: 8080\n")

	rv, err := a.ToBSON(original)
	if err != nil {
		t.Fatalf("ToBSON: %v", err)
	}

	got, err := a.FromBSON(rv)
	if err != nil {
		t.Fatalf("FromBSON: %v", err)
	}
	if string(got) != string(original) {
		t.Errorf("round-trip: got %q, want %q", got, original)
	}
}

// --- bsonCodec BSONValueCodec tests ---

func TestBsonCodec_ToBSON(t *testing.T) {
	ctx := context.Background()
	c := &bsonCodec{}
	encoded, err := c.Encode(ctx, "test")
	if err != nil {
		t.Fatal(err)
	}

	rv, err := c.ToBSON(encoded)
	if err != nil {
		t.Fatalf("ToBSON: %v", err)
	}
	// Type byte should match original encoding
	if rv.Type != bson.Type(encoded[0]) {
		t.Errorf("type mismatch: got %v, want %v", rv.Type, bson.Type(encoded[0]))
	}
}

func TestBsonCodec_ToBSON_Empty(t *testing.T) {
	c := &bsonCodec{}
	_, err := c.ToBSON(nil)
	if err == nil {
		t.Error("expected error for empty data")
	}
}

func TestBsonCodec_FromBSON(t *testing.T) {
	ctx := context.Background()
	c := &bsonCodec{}
	// Encode a value, convert to BSON, convert back, decode
	encoded, err := c.Encode(ctx, int32(42))
	if err != nil {
		t.Fatal(err)
	}

	rv, err := c.ToBSON(encoded)
	if err != nil {
		t.Fatal(err)
	}

	got, err := c.FromBSON(rv)
	if err != nil {
		t.Fatalf("FromBSON: %v", err)
	}

	var result int32
	if err := c.Decode(ctx, got, &result); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if result != 42 {
		t.Errorf("got %d, want 42", result)
	}
}

// --- toBSONValue / fromBSONValue dispatch tests ---

func TestToBSONValue_WithBSONValueCodec(t *testing.T) {
	ctx := context.Background()
	// jsonBSONCodec implements BSONValueCodec
	c := codec.Get("json")
	data, err := c.Encode(ctx, map[string]string{"key": "value"})
	if err != nil {
		t.Fatal(err)
	}

	rv, err := toBSONValue(c, data)
	if err != nil {
		t.Fatalf("toBSONValue: %v", err)
	}
	// JSON adapter stores as BSON string
	if rv.Type != bson.TypeString {
		t.Errorf("expected TypeString, got %v", rv.Type)
	}
}

func TestToBSONValue_FallbackBinData(t *testing.T) {
	// Use a plain codec that does NOT implement BSONValueCodec
	c := &plainCodecForTest{}
	data := []byte("opaque-bytes")

	rv, err := toBSONValue(c, data)
	if err != nil {
		t.Fatalf("toBSONValue fallback: %v", err)
	}
	// Should be stored as BinData
	if rv.Type != bson.TypeBinary {
		t.Errorf("expected TypeBinary for fallback, got %v", rv.Type)
	}
}

func TestFromBSONValue_WithBSONValueCodec(t *testing.T) {
	c := codec.Get("json")
	original := []byte(`{"a":1}`)

	rv, err := toBSONValue(c, original)
	if err != nil {
		t.Fatal(err)
	}

	got, err := fromBSONValue(c, rv)
	if err != nil {
		t.Fatalf("fromBSONValue: %v", err)
	}
	if string(got) != string(original) {
		t.Errorf("got %q, want %q", got, original)
	}
}

func TestFromBSONValue_FallbackBinData(t *testing.T) {
	c := &plainCodecForTest{}
	original := []byte("opaque-bytes")

	rv, err := toBSONValue(c, original)
	if err != nil {
		t.Fatal(err)
	}

	got, err := fromBSONValue(c, rv)
	if err != nil {
		t.Fatalf("fromBSONValue fallback: %v", err)
	}
	if string(got) != string(original) {
		t.Errorf("got %q, want %q", got, original)
	}
}

func TestFromBSONValue_LegacyBinDataWithBSONValueCodec(t *testing.T) {
	// Simulate reading a legacy BinData document with a BSON-aware codec
	c := codec.Get("json")
	legacyData := []byte(`42`)
	bsonType, bsonVal, err := bson.MarshalValue(legacyData)
	if err != nil {
		t.Fatal(err)
	}
	rv := bson.RawValue{Type: bsonType, Value: bsonVal}

	got, err := fromBSONValue(c, rv)
	if err != nil {
		t.Fatalf("fromBSONValue legacy: %v", err)
	}
	if string(got) != string(legacyData) {
		t.Errorf("got %q, want %q", got, legacyData)
	}
}

// --- SupportsCodec tests ---

func TestSupportsCodec_RegisteredCodec(t *testing.T) {
	s := &Store{}
	for _, name := range []string{"json", "yaml", "toml", "bson"} {
		if !s.SupportsCodec(name) {
			t.Errorf("SupportsCodec(%q) = false, want true", name)
		}
	}
}

func TestSupportsCodec_UnregisteredCodec(t *testing.T) {
	s := &Store{}
	if s.SupportsCodec("nonexistent") {
		t.Error("SupportsCodec(nonexistent) = true, want false")
	}
}

// plainCodecForTest is a codec that does NOT implement BSONValueCodec,
// used to test the BinData fallback path.
type plainCodecForTest struct{}

func (c *plainCodecForTest) Name() string                                            { return "test-plain" }
func (c *plainCodecForTest) Encode(_ context.Context, _ any) ([]byte, error)         { return nil, nil }
func (c *plainCodecForTest) Decode(_ context.Context, _ []byte, _ any) error         { return nil }
