package codec

import (
	"gopkg.in/yaml.v3"
)

func init() {
	_ = Register(&yamlCodec{})
}

// yamlCodec implements the Codec interface using YAML encoding.
type yamlCodec struct{}

// Compile-time interface check
var _ Codec = (*yamlCodec)(nil)

// Name returns the codec name.
func (c *yamlCodec) Name() string {
	return "yaml"
}

// Encode encodes a value to YAML bytes.
func (c *yamlCodec) Encode(v any) ([]byte, error) {
	return yaml.Marshal(v)
}

// Decode decodes YAML bytes into a value.
func (c *yamlCodec) Decode(data []byte, v any) error {
	return yaml.Unmarshal(data, v)
}

// YAML returns a new YAML codec instance.
func YAML() Codec {
	return &yamlCodec{}
}
