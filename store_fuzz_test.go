package config

import "testing"

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
		_ = ValidateKey(key)
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
		_ = ValidateNamespace(ns)
	})
}
