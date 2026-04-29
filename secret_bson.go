package config

import (
	"fmt"
	"runtime"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

// MarshalBSONValue implements bson.ValueMarshaler.
// A zero Secret marshals as BSON Null. A non-zero Secret marshals as BSON
// Binary (subtype 0x00) containing the raw bytes. Unlike text/JSON marshaling,
// this returns the actual content — use only in trusted MongoDB contexts.
func (s *Secret) MarshalBSONValue() (byte, []byte, error) {
	if s.IsZero() {
		return byte(bson.TypeNull), nil, nil
	}
	return byte(bson.TypeBinary), bsoncore.AppendBinary(nil, bson.TypeBinaryGeneric, s.v), nil
}

// UnmarshalBSONValue implements bson.ValueUnmarshaler.
// Accepts BSON Null (zero Secret), BSON Binary (raw bytes), or BSON String
// (text representation). A string equal to the mask "******" results in a
// zero Secret to prevent masked tokens from being treated as real credentials.
// Any prior backing bytes are zeroed before being replaced so re-used Secrets
// do not leave stale plaintext on the heap.
func (s *Secret) UnmarshalBSONValue(typ byte, data []byte) error {
	if s == nil {
		return fmt.Errorf("config: UnmarshalBSONValue on nil *Secret")
	}
	hadValue := s.v != nil
	clear(s.v)
	switch bson.Type(typ) {
	case bson.TypeNull:
		s.v = nil
		return nil
	case bson.TypeBinary:
		_, b, _, ok := bsoncore.ReadBinary(data)
		if !ok {
			s.v = nil
			return fmt.Errorf("config: malformed BSON binary for Secret")
		}
		s.v = make([]byte, len(b))
		copy(s.v, b)
	case bson.TypeString:
		str, _, ok := bsoncore.ReadString(data)
		if !ok {
			s.v = nil
			return fmt.Errorf("config: malformed BSON string for Secret")
		}
		if str == secretMask {
			s.v = nil
			return nil
		}
		s.v = []byte(str)
	default:
		s.v = nil
		return fmt.Errorf("config: cannot unmarshal BSON type 0x%02x into Secret", typ)
	}
	if !hadValue {
		runtime.SetFinalizer(s, (*Secret).Wipe)
	}
	return nil
}
