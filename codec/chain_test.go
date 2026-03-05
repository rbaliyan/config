package codec_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/rbaliyan/config/codec"
)

// rot1 is a trivial Transformer that adds 1 to each byte on Transform
// and subtracts 1 on Reverse. Used for testing only.
type rot1 struct {
	name string
}

func (r *rot1) Name() string { return r.name }

func (r *rot1) Transform(data []byte) ([]byte, error) {
	out := make([]byte, len(data))
	for i, b := range data {
		out[i] = b + 1
	}
	return out, nil
}

func (r *rot1) Reverse(data []byte) ([]byte, error) {
	out := make([]byte, len(data))
	for i, b := range data {
		out[i] = b - 1
	}
	return out, nil
}

// fakeCodec is a minimal Codec for testing.
type fakeCodec struct {
	name      string
	encodeErr error
	decodeErr error
}

func (f *fakeCodec) Name() string { return f.name }

func (f *fakeCodec) Encode(v any) ([]byte, error) {
	if f.encodeErr != nil {
		return nil, f.encodeErr
	}
	return []byte(v.(string)), nil
}

func (f *fakeCodec) Decode(data []byte, v any) error {
	if f.decodeErr != nil {
		return f.decodeErr
	}
	*v.(*string) = string(data)
	return nil
}

// failTransformer fails on Transform or Reverse as configured.
type failTransformer struct {
	name         string
	transformErr error
	reverseErr   error
}

func (f *failTransformer) Name() string { return f.name }

func (f *failTransformer) Transform(data []byte) ([]byte, error) {
	if f.transformErr != nil {
		return nil, f.transformErr
	}
	return data, nil
}

func (f *failTransformer) Reverse(data []byte) ([]byte, error) {
	if f.reverseErr != nil {
		return nil, f.reverseErr
	}
	return data, nil
}

func TestNewChainZeroTransformersReturnsBase(t *testing.T) {
	base := &fakeCodec{name: "test"}
	got := codec.NewChain(base)
	if got != base {
		t.Error("expected NewChain with no transformers to return base directly")
	}
}

func TestNewChainNilBasePanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for nil base")
		}
	}()
	codec.NewChain(nil)
}

func TestNewChainSingleTransformer(t *testing.T) {
	base := &fakeCodec{name: "json"}
	c := codec.NewChain(base, &rot1{name: "rot"})

	if c.Name() != "rot:json" {
		t.Errorf("Name() = %q, want %q", c.Name(), "rot:json")
	}

	data, err := c.Encode("abc")
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	var got string
	if err := c.Decode(data, &got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got != "abc" {
		t.Errorf("round-trip: got %q, want %q", got, "abc")
	}
}

func TestNewChainMultipleTransformers(t *testing.T) {
	base := &fakeCodec{name: "json"}
	c := codec.NewChain(base, &rot1{name: "encrypted"}, &rot1{name: "client"})

	if c.Name() != "client:encrypted:json" {
		t.Errorf("Name() = %q, want %q", c.Name(), "client:encrypted:json")
	}

	data, err := c.Encode("xyz")
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	// Each rot1 adds 1, so two of them should add 2 to each byte.
	for i, b := range []byte("xyz") {
		if data[i] != b+2 {
			t.Errorf("byte[%d] = %d, want %d", i, data[i], b+2)
		}
	}

	var got string
	if err := c.Decode(data, &got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got != "xyz" {
		t.Errorf("round-trip: got %q, want %q", got, "xyz")
	}
}

func TestNewChainBaseEncodeError(t *testing.T) {
	base := &fakeCodec{name: "json", encodeErr: errors.New("encode boom")}
	c := codec.NewChain(base, &rot1{name: "rot"})

	_, err := c.Encode("x")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "base") {
		t.Errorf("error should mention base: %v", err)
	}
}

func TestNewChainTransformError(t *testing.T) {
	base := &fakeCodec{name: "json"}
	c := codec.NewChain(base,
		&failTransformer{name: "ok"},
		&failTransformer{name: "bad", transformErr: errors.New("transform boom")},
	)

	_, err := c.Encode("x")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "transformer[1]") {
		t.Errorf("error should mention transformer index: %v", err)
	}
}

func TestNewChainReverseError(t *testing.T) {
	base := &fakeCodec{name: "json"}
	c := codec.NewChain(base,
		&failTransformer{name: "bad", reverseErr: errors.New("reverse boom")},
	)

	_, err := c.Encode("x")
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	var got string
	err = c.Decode([]byte("x"), &got)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "transformer[0]") {
		t.Errorf("error should mention transformer index: %v", err)
	}
}

func TestNewChainBaseDecodeError(t *testing.T) {
	base := &fakeCodec{name: "json", decodeErr: errors.New("decode boom")}
	c := codec.NewChain(base, &rot1{name: "rot"})

	data, err := c.Encode("x")
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	var got string
	err = c.Decode(data, &got)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "base") {
		t.Errorf("error should mention base: %v", err)
	}
}

func TestNewChainSliceImmutability(t *testing.T) {
	base := &fakeCodec{name: "json"}
	ts := []codec.Transformer{&rot1{name: "a"}, &rot1{name: "b"}}
	c := codec.NewChain(base, ts...)

	// Mutate the original slice after construction.
	ts[0] = &rot1{name: "mutated"}

	// The chain should still use the original transformer.
	if !strings.HasPrefix(c.Name(), "b:a:") {
		t.Errorf("Name() = %q, chain was affected by slice mutation", c.Name())
	}
}
