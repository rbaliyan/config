// Package json provides a JSON codec for the config library.
package json

import (
	"encoding/json"

	"github.com/rbaliyan/config/codec"
)

func init() {
	_ = codec.Register(&jsonCodec{})
}

// jsonCodec implements Codec using JSON encoding.
type jsonCodec struct{}

// Compile-time interface check
var _ codec.Codec = (*jsonCodec)(nil)

func (c *jsonCodec) Name() string {
	return "json"
}

func (c *jsonCodec) Encode(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (c *jsonCodec) Decode(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

// New returns a new JSON codec instance.
func New() codec.Codec {
	return &jsonCodec{}
}
