package config

import (
	"crypto/subtle"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"log/slog"
	"runtime"
)

const secretMask = "******"

// Secret holds sensitive bytes that can be explicitly zeroed.
//
// String() and all text/JSON serialization methods return the mask "******"
// so secrets are never accidentally leaked into logs, error messages, or HTTP
// responses. Use Bytes() to obtain the actual content.
//
// The internal representation is []byte so the secret can be zeroed in place
// via Wipe(). A runtime finalizer zeroes the backing bytes when the Secret
// becomes unreachable, providing a best-effort safety net. Call Wipe()
// explicitly for deterministic zeroing.
//
// All methods use pointer receivers. Use *Secret in struct fields and function
// signatures. Equal() performs constant-time comparison.
//
//	var cfg struct {
//	    DBPassword *config.Secret `yaml:"db_password"`
//	}
//	fmt.Println(cfg.DBPassword)             // ******
//	pwd := cfg.DBPassword.Bytes()           // []byte("actual-password"); zero after use
//	cfg.DBPassword.Wipe()                   // zeroes backing bytes
type Secret struct {
	v []byte
}

// NewSecret wraps s in a Secret and registers a finalizer that zeroes the
// backing bytes when the Secret becomes unreachable.
func NewSecret(s string) *Secret {
	sec := &Secret{v: []byte(s)}
	runtime.SetFinalizer(sec, (*Secret).Wipe)
	return sec
}

// NewSecretBytes creates a Secret from a byte slice.
// It copies the input; callers may zero their slice after this call.
// A finalizer is registered to zero the backing bytes on GC.
func NewSecretBytes(b []byte) *Secret {
	v := make([]byte, len(b))
	copy(v, b)
	sec := &Secret{v: v}
	runtime.SetFinalizer(sec, (*Secret).Wipe)
	return sec
}

// Bytes returns a copy of the secret bytes.
// Each call allocates a new slice; callers should zero it after use.
func (s *Secret) Bytes() []byte {
	if s == nil {
		return nil
	}
	b := make([]byte, len(s.v))
	copy(b, s.v)
	return b
}

// Clone returns a deep copy of the secret with its own finalizer.
func (s *Secret) Clone() *Secret { return NewSecretBytes(s.v) }

// Equal reports whether s and other hold the same secret value.
// It uses constant-time comparison to resist timing attacks.
func (s *Secret) Equal(other *Secret) bool {
	if s == nil && other == nil {
		return true
	}
	if s == nil || other == nil {
		return false
	}
	return subtle.ConstantTimeCompare(s.v, other.v) == 1
}

// IsZero reports whether the secret holds no value.
func (s *Secret) IsZero() bool { return s == nil || len(s.v) == 0 }

// Wipe zeroes the in-memory backing bytes and resets the secret to zero.
// After Wipe, Bytes() returns nil and IsZero() returns true.
// Safe to call multiple times and on a nil receiver.
func (s *Secret) Wipe() {
	if s == nil {
		return
	}
	clear(s.v)
	s.v = nil
}

// String implements fmt.Stringer and always returns "******".
func (s *Secret) String() string { return secretMask }

// GoString implements fmt.GoStringer so %#v also masks the value.
func (s *Secret) GoString() string { return `config.Secret("` + secretMask + `")` }

// LogValue implements slog.LogValuer so structured logging never leaks secrets.
func (s *Secret) LogValue() slog.Value { return slog.StringValue(secretMask) }

// MarshalJSON implements json.Marshaler. Always marshals as "******".
func (s *Secret) MarshalJSON() ([]byte, error) { return json.Marshal(secretMask) }

// MarshalText implements encoding.TextMarshaler. A zero Secret marshals as an
// empty string so config files are not polluted with mask tokens on round-trip.
// A non-zero Secret always marshals as "******".
func (s *Secret) MarshalText() ([]byte, error) {
	if s.IsZero() {
		return []byte(""), nil
	}
	return []byte(secretMask), nil
}

// UnmarshalText implements encoding.TextUnmarshaler, allowing YAML, JSON,
// TOML, and other text-based formats to populate the Secret.
// If the input is the mask "******", the secret is set to the empty value
// to prevent masked tokens from being treated as real credentials on round-trip.
func (s *Secret) UnmarshalText(b []byte) error {
	if string(b) == secretMask {
		clear(s.v)
		s.v = nil
		return nil
	}
	s.v = make([]byte, len(b))
	copy(s.v, b)
	return nil
}

// Value implements driver.Valuer so a Secret can be written to a SQL database.
// A zero Secret writes NULL. A non-zero Secret writes the raw bytes.
// Unlike text/JSON marshaling, this returns the actual content — use only with
// trusted database drivers over encrypted connections.
func (s *Secret) Value() (driver.Value, error) {
	if s.IsZero() {
		return nil, nil
	}
	b := make([]byte, len(s.v))
	copy(b, s.v)
	return b, nil
}

// Scan implements sql.Scanner so a Secret can be read from a SQL database.
// A NULL column value results in a zero Secret.
func (s *Secret) Scan(value any) error {
	if value == nil {
		clear(s.v)
		s.v = nil
		return nil
	}
	switch v := value.(type) {
	case []byte:
		s.v = make([]byte, len(v))
		copy(s.v, v)
	case string:
		s.v = []byte(v)
	default:
		return fmt.Errorf("cannot scan %T into Secret", value)
	}
	return nil
}
