package mongodb

import (
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/rbaliyan/config/codec"
)

func init() {
	_ = codec.Register(&bsonCodec{})
}

// bsonCodec encodes values using BSON MarshalValue.
// The byte format is [type_byte][value_bytes...] where the type byte
// is the BSON type from bson.MarshalValue, enabling native BSON storage
// in MongoDB while maintaining a []byte interface for the codec system.
type bsonCodec struct{}

var (
	_ codec.Codec    = (*bsonCodec)(nil)
	_ BSONValueCodec = (*bsonCodec)(nil)
)

func (c *bsonCodec) Name() string { return "bson" }

func (c *bsonCodec) Encode(v any) ([]byte, error) {
	t, val, err := bson.MarshalValue(v)
	if err != nil {
		return nil, fmt.Errorf("bson encode: %w", err)
	}
	// Prepend type byte to value bytes
	result := make([]byte, 1+len(val))
	result[0] = byte(t)
	copy(result[1:], val)
	return result, nil
}

func (c *bsonCodec) Decode(data []byte, v any) error {
	if len(data) == 0 {
		return fmt.Errorf("bson decode: empty data")
	}
	raw := bson.RawValue{Type: bson.Type(data[0]), Value: data[1:]}
	if err := raw.Unmarshal(v); err != nil {
		return fmt.Errorf("bson decode: %w", err)
	}
	return nil
}

func (c *bsonCodec) ToBSON(encoded []byte) (bson.RawValue, error) {
	if len(encoded) == 0 {
		return bson.RawValue{}, fmt.Errorf("bson ToBSON: empty data")
	}
	return bson.RawValue{Type: bson.Type(encoded[0]), Value: encoded[1:]}, nil
}

func (c *bsonCodec) FromBSON(rv bson.RawValue) ([]byte, error) {
	data := make([]byte, 1+len(rv.Value))
	data[0] = byte(rv.Type)
	copy(data[1:], rv.Value)
	return data, nil
}

// BSON returns the BSON codec instance.
func BSON() codec.Codec {
	return &bsonCodec{}
}
