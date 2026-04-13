package codec

import (
	"context"
	"fmt"
	"sync"
)

// Codec defines the interface for encoding and decoding configuration values.
type Codec interface {
	// Name returns the codec identifier (e.g., "json", "yaml").
	Name() string

	// Encode serializes a value to bytes.
	Encode(ctx context.Context, v any) ([]byte, error)

	// Decode deserializes bytes into the target.
	Decode(ctx context.Context, data []byte, v any) error
}

// Transformer defines a reversible byte-level transformation such as
// encryption or compression. Transformers compose via NewChain.
type Transformer interface {
	// Name returns a short identifier for the transformation (e.g., "encrypted", "gzip").
	Name() string

	// Transform applies the forward transformation to data.
	Transform(ctx context.Context, data []byte) ([]byte, error)

	// Reverse undoes the transformation, recovering the original data.
	Reverse(ctx context.Context, data []byte) ([]byte, error)
}

var (
	registryMu sync.RWMutex
	registry   = make(map[string]Codec)
)

// Register adds a codec to the global registry.
// Re-registering a name replaces the previous codec (e.g., the mongodb
// package upgrades json/yaml/toml with BSON-aware versions).
// Returns an error if codec is nil or has an empty name.
func Register(codec Codec) error {
	if codec == nil {
		return fmt.Errorf("codec: Register codec is nil")
	}
	name := codec.Name()
	if name == "" {
		return fmt.Errorf("codec: Register codec name is empty")
	}

	registryMu.Lock()
	defer registryMu.Unlock()

	registry[name] = codec
	return nil
}

// Get retrieves a codec by name from the registry.
// Returns nil if not found.
func Get(name string) Codec {
	registryMu.RLock()
	defer registryMu.RUnlock()

	return registry[name]
}

// Names returns the names of all registered codecs.
func Names() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()

	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	return names
}

// Default returns the default codec (JSON).
// Returns nil if the "json" codec has not been registered.
// The config package automatically registers JSON via blank import.
func Default() Codec {
	return Get("json")
}
