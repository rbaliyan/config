package config

const secretMask = "******"

// Secret holds a sensitive string.
//
// String() and all serialization methods (JSON, text, fmt verbs) return the
// mask "******" so secrets are never accidentally leaked into logs, error
// messages, or HTTP responses. Use Value() to obtain the actual content.
//
//	var cfg struct {
//	    DBPassword config.Secret `yaml:"db_password"`
//	}
//	fmt.Println(cfg.DBPassword)          // ******
//	pwd := cfg.DBPassword.Value()        // "actual-password"
type Secret struct {
	v string
}

// NewSecret wraps s in a Secret.
func NewSecret(s string) Secret { return Secret{v: s} }

// Value returns the actual secret string.
func (s Secret) Value() string { return s.v }

// String implements fmt.Stringer and always returns "******".
func (s Secret) String() string { return secretMask }

// GoString implements fmt.GoStringer so %#v also masks the value.
func (s Secret) GoString() string { return `config.Secret("` + secretMask + `")` }

// IsZero reports whether the secret holds no value.
func (s Secret) IsZero() bool { return s.v == "" }

// MarshalText implements encoding.TextMarshaler. A zero Secret marshals as an
// empty string so config files are not polluted with mask tokens on round-trip.
// A non-zero Secret always marshals as "******".
func (s Secret) MarshalText() ([]byte, error) {
	if s.v == "" {
		return []byte(""), nil
	}
	return []byte(secretMask), nil
}

// UnmarshalText implements encoding.TextUnmarshaler, allowing YAML, JSON,
// TOML, and other text-based formats to populate the Secret.
// If the input is the mask "******", the secret is set to the empty string
// to prevent masked tokens from being treated as real credentials.
func (s *Secret) UnmarshalText(b []byte) error {
	if string(b) == secretMask {
		s.v = ""
		return nil
	}
	s.v = string(b)
	return nil
}
