package mongodb

import (
	"github.com/rbaliyan/config/codec"
	jsoncodec "github.com/rbaliyan/config/codec/json"
)

func init() { _ = codec.Register(&jsonBSONCodec{inner: jsoncodec.New()}) }

// jsonBSONCodec wraps the base JSON codec with BSON-native storage.
// JSON bytes are stored as BSON strings for readability in MongoDB.
type jsonBSONCodec struct {
	inner codec.Codec
	stringBSONAdapter
}

var (
	_ codec.Codec    = (*jsonBSONCodec)(nil)
	_ BSONValueCodec = (*jsonBSONCodec)(nil)
)

func (c *jsonBSONCodec) Name() string                    { return c.inner.Name() }
func (c *jsonBSONCodec) Encode(v any) ([]byte, error)    { return c.inner.Encode(v) }
func (c *jsonBSONCodec) Decode(data []byte, v any) error { return c.inner.Decode(data, v) }
