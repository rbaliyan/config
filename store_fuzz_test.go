package config

import (
	"strings"
	"testing"
)

func FuzzValidateKey(f *testing.F) {
	f.Add("app/database/host")
	f.Add("simple-key")
	f.Add("key_with.dots")
	f.Add("")
	f.Add("/leading-slash")
	f.Add("trailing-slash/")
	f.Add("path/../traversal")
	f.Add("valid/key/path")
	f.Add("special!@#$%chars")
	f.Add("a")

	f.Fuzz(func(t *testing.T, key string) {
		err := ValidateKey(key)

		// Idempotence: validation must be a pure function of its input.
		if (ValidateKey(key) == nil) != (err == nil) {
			t.Fatalf("ValidateKey not idempotent for %q", key)
		}

		// Any key that PASSES validation must honor the documented contract,
		// so a passing key can never carry a traversal or slash boundary that
		// would let it escape its namespace.
		if err == nil {
			if key == "" {
				t.Errorf("empty key passed validation")
			}
			if strings.Contains(key, "..") {
				t.Errorf("key with traversal passed validation: %q", key)
			}
			if strings.HasPrefix(key, "/") || strings.HasSuffix(key, "/") {
				t.Errorf("key with leading/trailing slash passed validation: %q", key)
			}
		}
	})
}

func FuzzValidateNamespace(f *testing.F) {
	f.Add("production")
	f.Add("dev-01")
	f.Add("test_env")
	f.Add("")
	f.Add("-starts-with-dash")
	f.Add("_starts-with-underscore")
	f.Add("UPPERCASE")
	f.Add("special!chars")
	f.Add("a")

	f.Fuzz(func(t *testing.T, ns string) {
		err := ValidateNamespace(ns)

		// Idempotence: deterministic for a given input.
		if (ValidateNamespace(ns) == nil) != (err == nil) {
			t.Fatalf("ValidateNamespace not idempotent for %q", ns)
		}

		// The empty namespace is documented as always valid (default namespace).
		if ns == "" && err != nil {
			t.Errorf("empty namespace must be valid, got %v", err)
		}

		// A passing namespace must not contain control characters or whitespace
		// that would corrupt downstream keys/wire framing.
		if err == nil {
			for _, r := range ns {
				if r < 0x20 || r == 0x7f {
					t.Errorf("namespace with control char %q passed validation: %q", r, ns)
					break
				}
			}
		}
	})
}
