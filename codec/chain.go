package codec

import (
	"context"
	"fmt"
)

// chain is a Codec that applies a base serializer followed by an ordered
// sequence of byte-level Transformers. On Encode the transformers run
// forward (index 0 → n-1); on Decode they run in reverse (n-1 → 0).
type chain struct {
	base         Codec
	transformers []Transformer
	name         string
}

// NewChain returns a Codec that composes base with the given transformers.
// Encode calls base.Encode then each transformer's Transform in order.
// Decode calls each transformer's Reverse in reverse order then base.Decode.
//
// The codec name is built by prepending each transformer's Name from index 0
// to n-1, producing outermost-first format. For example:
//
//	NewChain(json, serverEnc, clientEnc) → "client:encrypted:json"
//
// If no transformers are provided the base codec is returned directly.
// Panics if base is nil (initialization-time, per codebase convention).
func NewChain(base Codec, transformers ...Transformer) Codec {
	if base == nil {
		panic("codec: NewChain base codec is nil")
	}
	if len(transformers) == 0 {
		return base
	}

	// Defensive copy and nil check.
	ts := make([]Transformer, len(transformers))
	copy(ts, transformers)
	for i, t := range ts {
		if t == nil {
			panic(fmt.Sprintf("codec: NewChain transformer[%d] is nil", i))
		}
	}

	// Build name: prepend each transformer name from 0..n-1.
	name := base.Name()
	for i := 0; i < len(ts); i++ {
		name = ts[i].Name() + ":" + name
	}

	return &chain{
		base:         base,
		transformers: ts,
		name:         name,
	}
}

func (c *chain) Name() string { return c.name }

func (c *chain) Encode(ctx context.Context, v any) ([]byte, error) {
	data, err := c.base.Encode(ctx, v)
	if err != nil {
		return nil, fmt.Errorf("codec: chain encode: base: %w", err)
	}

	for i, t := range c.transformers {
		data, err = t.Transform(ctx, data)
		if err != nil {
			return nil, fmt.Errorf("codec: chain encode: transformer[%d] %q: %w", i, t.Name(), err)
		}
	}
	return data, nil
}

func (c *chain) Decode(ctx context.Context, data []byte, v any) error {
	var err error
	for i := len(c.transformers) - 1; i >= 0; i-- {
		data, err = c.transformers[i].Reverse(ctx, data)
		if err != nil {
			return fmt.Errorf("codec: chain decode: transformer[%d] %q: %w", i, c.transformers[i].Name(), err)
		}
	}

	if err := c.base.Decode(ctx, data, v); err != nil {
		return fmt.Errorf("codec: chain decode: base: %w", err)
	}
	return nil
}
