package mongodb

import (
	"github.com/rbaliyan/config/codec"
	yamlcodec "github.com/rbaliyan/config/codec/yaml"
)

func init() { _ = codec.Register(&yamlBSONCodec{inner: yamlcodec.New()}) }

// yamlBSONCodec wraps the base YAML codec with BSON-native storage.
// YAML bytes are stored as BSON strings for readability in MongoDB.
type yamlBSONCodec struct {
	inner codec.Codec
	stringBSONAdapter
}

var (
	_ codec.Codec    = (*yamlBSONCodec)(nil)
	_ BSONValueCodec = (*yamlBSONCodec)(nil)
)

func (c *yamlBSONCodec) Name() string                    { return c.inner.Name() }
func (c *yamlBSONCodec) Encode(v any) ([]byte, error)    { return c.inner.Encode(v) }
func (c *yamlBSONCodec) Decode(data []byte, v any) error { return c.inner.Decode(data, v) }
