package codec

import (
	"encoding/json"
)

func init() {
	_ = Register(&jsonCodec{})
}

// jsonCodec implements Codec using JSON encoding.
type jsonCodec struct{}

// Compile-time interface check
var _ Codec = (*jsonCodec)(nil)

func (c *jsonCodec) Name() string {
	return "json"
}

func (c *jsonCodec) Encode(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (c *jsonCodec) Decode(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

// JSON returns the JSON codec instance.
func JSON() Codec {
	return &jsonCodec{}
}
