// Package toml provides a TOML codec for the config library.
package toml

import (
	"bytes"

	"github.com/BurntSushi/toml"
	"github.com/rbaliyan/config/codec"
)

func init() {
	_ = codec.Register(&tomlCodec{})
}

// tomlCodec implements the Codec interface using TOML encoding.
type tomlCodec struct{}

// Compile-time interface check
var _ codec.Codec = (*tomlCodec)(nil)

// Name returns the codec name.
func (c *tomlCodec) Name() string {
	return "toml"
}

// Encode encodes a value to TOML bytes.
func (c *tomlCodec) Encode(v any) ([]byte, error) {
	var buf bytes.Buffer
	err := toml.NewEncoder(&buf).Encode(v)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes TOML bytes into a value.
func (c *tomlCodec) Decode(data []byte, v any) error {
	_, err := toml.NewDecoder(bytes.NewReader(data)).Decode(v)
	return err
}

// New returns a new TOML codec instance.
func New() codec.Codec {
	return &tomlCodec{}
}
