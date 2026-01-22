package codec

import (
	"sync"
)

// Codec defines the interface for encoding and decoding configuration values.
type Codec interface {
	// Name returns the codec identifier (e.g., "json", "yaml").
	Name() string

	// Encode serializes a value to bytes.
	Encode(v any) ([]byte, error)

	// Decode deserializes bytes into the target.
	Decode(data []byte, v any) error
}

var (
	registryMu sync.RWMutex
	registry   = make(map[string]Codec)
)

// Register adds a codec to the global registry.
// Panics if name is empty or codec is nil.
func Register(codec Codec) {
	if codec == nil {
		panic("codec: Register codec is nil")
	}
	name := codec.Name()
	if name == "" {
		panic("codec: Register codec name is empty")
	}

	registryMu.Lock()
	defer registryMu.Unlock()

	registry[name] = codec
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
func Default() Codec {
	return Get("json")
}
