// Package yaml provides a YAML codec for the config library.
package yaml

import (
	"context"

	"github.com/rbaliyan/config/codec"
	"gopkg.in/yaml.v3"
)

func init() {
	_ = codec.Register(&yamlCodec{})
}

// yamlCodec implements the Codec interface using YAML encoding.
type yamlCodec struct{}

// Compile-time interface check
var _ codec.Codec = (*yamlCodec)(nil)

// Name returns the codec name.
func (c *yamlCodec) Name() string {
	return "yaml"
}

// Encode encodes a value to YAML bytes.
func (c *yamlCodec) Encode(_ context.Context, v any) ([]byte, error) {
	return yaml.Marshal(v)
}

// Decode decodes YAML bytes into a value.
func (c *yamlCodec) Decode(_ context.Context, data []byte, v any) error {
	return yaml.Unmarshal(data, v)
}

// New returns a new YAML codec instance.
func New() codec.Codec {
	return &yamlCodec{}
}
