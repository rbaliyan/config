package config

import (
	"time"
)

// entry is the internal storage representation for configuration values.
// This is internal to store implementations and not exposed in the public API.
type entry struct {
	// ID is a unique identifier for pagination (auto-increment or ObjectID).
	ID string `json:"id" bson:"_id,omitempty"`

	// Key is the configuration key (without namespace).
	Key string `json:"key" bson:"key"`

	// Namespace is the configuration namespace.
	Namespace string `json:"namespace" bson:"namespace"`

	// Value is the raw configuration value as bytes.
	Value []byte `json:"value" bson:"value"`

	// Codec specifies how Value is encoded (e.g., "json", "yaml").
	Codec string `json:"codec" bson:"codec"`

	// Type is the value type for type-safe access.
	Type Type `json:"type" bson:"type"`

	// Version is auto-incremented on each update.
	Version int64 `json:"version" bson:"version"`

	// CreatedAt is the initial creation timestamp.
	CreatedAt time.Time `json:"created_at" bson:"created_at"`

	// UpdatedAt is the last modification timestamp.
	UpdatedAt time.Time `json:"updated_at" bson:"updated_at"`
}

// clone creates a deep copy of the entry.
func (e *entry) clone() *entry {
	if e == nil {
		return nil
	}

	clone := *e

	// Deep copy Value
	if e.Value != nil {
		clone.Value = make([]byte, len(e.Value))
		copy(clone.Value, e.Value)
	}

	return &clone
}

// toValue converts an entry to a Value.
func (e *entry) toValue() (Value, error) {
	if e == nil {
		return nil, ErrNotFound
	}

	return NewValueFromBytes(
		e.Value,
		e.Codec,
		WithValueType(e.Type),
		WithValueMetadata(e.Version, e.CreatedAt, e.UpdatedAt),
		WithValueEntryID(e.ID),
	)
}

// fullKey returns the fully qualified key including namespace.
// Format: "namespace/key"
func (e *entry) fullKey() string {
	if e.Namespace == "" {
		return e.Key
	}
	return e.Namespace + "/" + e.Key
}
