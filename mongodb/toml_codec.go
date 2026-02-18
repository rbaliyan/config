package mongodb

import (
	"github.com/rbaliyan/config/codec"
	tomlcodec "github.com/rbaliyan/config/codec/toml"
)

func init() { _ = codec.Register(&tomlBSONCodec{inner: tomlcodec.New()}) }

// tomlBSONCodec wraps the base TOML codec with BSON-native storage.
// TOML bytes are stored as BSON strings for readability in MongoDB.
type tomlBSONCodec struct {
	inner codec.Codec
	stringBSONAdapter
}

var (
	_ codec.Codec    = (*tomlBSONCodec)(nil)
	_ BSONValueCodec = (*tomlBSONCodec)(nil)
)

func (c *tomlBSONCodec) Name() string                    { return c.inner.Name() }
func (c *tomlBSONCodec) Encode(v any) ([]byte, error)    { return c.inner.Encode(v) }
func (c *tomlBSONCodec) Decode(data []byte, v any) error { return c.inner.Decode(data, v) }
