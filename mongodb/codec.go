package mongodb

import "go.mongodb.org/mongo-driver/v2/bson"

// BSONValueCodec is a store-specific codec interface for MongoDB.
// Codecs that implement this interface control how their encoded bytes
// are stored as native BSON values. Codecs that don't implement this
// are stored as BinData (binary fallback).
type BSONValueCodec interface {
	ToBSON(encoded []byte) (bson.RawValue, error)
	FromBSON(rv bson.RawValue) ([]byte, error)
}

// stringBSONAdapter stores encoded bytes as a BSON string.
// On read, it handles both BSON string (new format) and BinData (legacy).
// Embed this in codec adapters whose output is human-readable text.
type stringBSONAdapter struct{}

func (stringBSONAdapter) ToBSON(encoded []byte) (bson.RawValue, error) {
	t, val, err := bson.MarshalValue(string(encoded))
	if err != nil {
		return bson.RawValue{}, err
	}
	return bson.RawValue{Type: t, Value: val}, nil
}

func (stringBSONAdapter) FromBSON(rv bson.RawValue) ([]byte, error) {
	if rv.Type == bson.TypeString {
		var s string
		if err := rv.Unmarshal(&s); err != nil {
			return nil, err
		}
		return []byte(s), nil
	}
	// Legacy BinData fallback
	var data []byte
	if err := rv.Unmarshal(&data); err != nil {
		return nil, err
	}
	return data, nil
}
