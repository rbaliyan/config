package file

import (
	"context"
	"os"
	"testing"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/expand"
)

// writeFile writes content to path, failing the test on error.
func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("writeFile(%q): %v", path, err)
	}
}

// connectStore calls Connect and fails the test on error.
func connectStore(t *testing.T, s *Store) {
	t.Helper()
	if err := s.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
}

// storeGet calls Get and fails the test on error.
func storeGet(t *testing.T, s *Store, ns, key string) config.Value {
	t.Helper()
	v, err := s.Get(context.Background(), ns, key)
	if err != nil {
		t.Fatalf("Get(%q, %q): %v", ns, key, err)
	}
	return v
}

// --- expand.Dollar tests ---

func TestDollar(t *testing.T) {
	lookup := func(name string) (string, bool) {
		m := map[string]string{"HOST": "localhost", "PORT": "5432"}
		v, ok := m[name]
		return v, ok
	}

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"simple", "${HOST}", "localhost"},
		{"multiple", "${HOST}:${PORT}", "localhost:5432"},
		{"inline", "connect to ${HOST} on port ${PORT}", "connect to localhost on port 5432"},
		{"default used", "${MISSING:-fallback}", "fallback"},
		{"default not used", "${HOST:-other}", "localhost"},
		{"empty default", "${MISSING:-}", ""},
		{"not found no default", "${MISSING}", "${MISSING}"},
		{"backslash disables", `\${HOST}`, "${HOST}"},
		{"double backslash", `\\`, `\`},
		{"escape then expand", `\${HOST} and ${PORT}`, "${HOST} and 5432"},
		{"other backslash passthrough", `\n`, `\n`},
		{"no tokens", "plain string", "plain string"},
		{"empty string", "", ""},
		{"unclosed brace", "${HOST", "${HOST"},
		{"dollar without brace", "$HOST", "$HOST"},
		{"adjacent tokens", "${HOST}${PORT}", "localhost5432"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := expand.Dollar(tc.input, lookup)
			if got != tc.want {
				t.Errorf("Dollar(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestDollarNoOp(t *testing.T) {
	fn := func(string) (string, bool) { return "", false }
	input := "no placeholders here"
	if got := expand.Dollar(input, fn); got != input {
		t.Errorf("Dollar(%q) = %q, want unchanged", input, got)
	}
}

// --- expand.Angle tests ---

func TestAngle(t *testing.T) {
	lookup := func(name string) (string, bool) {
		m := map[string]string{
			"password": "s3cr3t",
			"host":     "db.internal",
			"my-var":   "hyphenated",
		}
		v, ok := m[name]
		return v, ok
	}

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"simple", "<password>", "s3cr3t"},
		{"multiple", "<host>:<password>", "db.internal:s3cr3t"},
		{"inline", "connect <host> with <password>", "connect db.internal with s3cr3t"},
		{"hyphenated name", "<my-var>", "hyphenated"},
		{"not found", "<MISSING>", "<MISSING>"},
		{"backslash disables", `\<password>`, "<password>"},
		{"double backslash", `\\`, `\`},
		{"escape then expand", `\<password> and <host>`, "<password> and db.internal"},
		{"other backslash passthrough", `\n`, `\n`},
		{"no brackets", "no brackets", "no brackets"},
		{"empty string", "", ""},
		{"html tag not matched", "<div>content</div>", "<div>content</div>"},
		{"unclosed angle", "<password", "<password"},
		{"digit start not matched", "<1invalid>", "<1invalid>"},
		{"empty angle", "<>", "<>"},
		{"dollar syntax untouched", "${password}", "${password}"},
		{"adjacent", "<host><password>", "db.internals3cr3t"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := expand.Angle(tc.input, lookup)
			if got != tc.want {
				t.Errorf("Angle(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

// --- EnvExpander / ChainExpanders ---

func TestEnvExpander(t *testing.T) {
	t.Setenv("EXPAND_TEST_VAR", "hello")
	fn := EnvExpander()
	v, ok := fn("EXPAND_TEST_VAR")
	if !ok || v != "hello" {
		t.Errorf("EnvExpander: got %q, %v; want %q, true", v, ok, "hello")
	}
	_, ok = fn("EXPAND_TEST_MISSING")
	if ok {
		t.Error("EnvExpander: expected not-found for missing variable")
	}
}

func TestChainExpanders(t *testing.T) {
	first := func(name string) (string, bool) {
		if name == "A" {
			return "from-first", true
		}
		return "", false
	}
	second := func(name string) (string, bool) {
		if name == "B" {
			return "from-second", true
		}
		if name == "A" {
			return "from-second", true
		}
		return "", false
	}

	chain := ChainExpanders(first, second)

	if v, ok := chain("A"); !ok || v != "from-first" {
		t.Errorf("chain(A) = %q, %v; want from-first, true", v, ok)
	}
	if v, ok := chain("B"); !ok || v != "from-second" {
		t.Errorf("chain(B) = %q, %v; want from-second, true", v, ok)
	}
	if _, ok := chain("C"); ok {
		t.Error("chain(C): expected not-found")
	}
}

// --- Store integration: WithExpansion (${VAR}) ---

func TestStoreWithExpansion(t *testing.T) {
	t.Setenv("FILE_TEST_HOST", "db.example.com")
	t.Setenv("FILE_TEST_PORT", "5432")

	dir := t.TempDir()
	yamlPath := dir + "/config.yaml"
	writeFile(t, yamlPath, `
database:
  host: ${FILE_TEST_HOST}
  port: "${FILE_TEST_PORT}"
  url: "postgres://${FILE_TEST_HOST}:${FILE_TEST_PORT}/mydb"
  name: mydb
  escaped: \${NOT_EXPANDED}
  default: "${MISSING_VAR:-5000}"
`)

	store := NewStore(yamlPath, WithExpansion(EnvExpander()))
	connectStore(t, store)
	defer store.Close(t.Context())

	cases := []struct{ key, want string }{
		{"host", "db.example.com"},
		{"port", "5432"},
		{"url", "postgres://db.example.com:5432/mydb"},
		{"name", "mydb"},
		{"escaped", "${NOT_EXPANDED}"},
		{"default", "5000"},
	}
	for _, tc := range cases {
		val := storeGet(t, store, "database", tc.key)
		s, err := val.String()
		if err != nil {
			t.Errorf("key %q String(): %v", tc.key, err)
			continue
		}
		if s != tc.want {
			t.Errorf("key %q: got %q, want %q", tc.key, s, tc.want)
		}
	}
}

func TestStoreWithCustomExpansion(t *testing.T) {
	secrets := map[string]string{"DB_PASSWORD": "s3cr3t"}
	expander := func(name string) (string, bool) {
		v, ok := secrets[name]
		return v, ok
	}

	dir := t.TempDir()
	yamlPath := dir + "/config.yaml"
	writeFile(t, yamlPath, `
database:
  password: "${DB_PASSWORD}"
  other: "${UNSET_VAR}"
`)

	store := NewStore(yamlPath, WithExpansion(expander))
	connectStore(t, store)
	defer store.Close(t.Context())

	pw := storeGet(t, store, "database", "password")
	s, err := pw.String()
	if err != nil {
		t.Fatalf("password String(): %v", err)
	}
	if s != "s3cr3t" {
		t.Errorf("password: got %q, want s3cr3t", s)
	}

	other := storeGet(t, store, "database", "other")
	s, err = other.String()
	if err != nil {
		t.Fatalf("other String(): %v", err)
	}
	if s != "${UNSET_VAR}" {
		t.Errorf("other: got %q, want ${UNSET_VAR}", s)
	}
}

func TestStoreWithChainedExpansion(t *testing.T) {
	t.Setenv("CHAIN_HOST", "env-host")

	overrides := map[string]string{"CHAIN_PORT": "9999"}
	customFn := func(name string) (string, bool) {
		v, ok := overrides[name]
		return v, ok
	}

	dir := t.TempDir()
	yamlPath := dir + "/config.yaml"
	writeFile(t, yamlPath, `
app:
  host: "${CHAIN_HOST}"
  port: "${CHAIN_PORT}"
`)

	store := NewStore(yamlPath,
		WithExpansion(customFn),
		WithExpansion(EnvExpander()),
	)
	connectStore(t, store)
	defer store.Close(t.Context())

	host := storeGet(t, store, "app", "host")
	if s, _ := host.String(); s != "env-host" {
		t.Errorf("host: got %q, want env-host", s)
	}

	port := storeGet(t, store, "app", "port")
	if s, _ := port.String(); s != "9999" {
		t.Errorf("port: got %q, want 9999", s)
	}
}

// --- Store integration: WithAngleBracketExpander (<VAR>) ---

func TestStoreWithAngleBracketExpander(t *testing.T) {
	t.Setenv("ANGLE_TEST_HOST", "redis.internal")
	t.Setenv("ANGLE_TEST_PORT", "6379")

	dir := t.TempDir()
	yamlPath := dir + "/config.yaml"
	writeFile(t, yamlPath, `
cache:
  host: "<ANGLE_TEST_HOST>"
  port: "<ANGLE_TEST_PORT>"
  url: "redis://<ANGLE_TEST_HOST>:<ANGLE_TEST_PORT>"
  escaped: \<NOT_EXPANDED>
  untouched_dollar: "${ANGLE_TEST_HOST}"
`)

	store := NewStore(yamlPath, WithAngleBracketExpander(EnvExpander()))
	connectStore(t, store)
	defer store.Close(t.Context())

	cases := []struct{ key, want string }{
		{"host", "redis.internal"},
		{"port", "6379"},
		{"url", "redis://redis.internal:6379"},
		{"escaped", "<NOT_EXPANDED>"},
		{"untouched_dollar", "${ANGLE_TEST_HOST}"},
	}
	for _, tc := range cases {
		val := storeGet(t, store, "cache", tc.key)
		s, err := val.String()
		if err != nil {
			t.Errorf("key %q String(): %v", tc.key, err)
			continue
		}
		if s != tc.want {
			t.Errorf("key %q: got %q, want %q", tc.key, s, tc.want)
		}
	}
}

func TestStoreWithBothExpansions(t *testing.T) {
	t.Setenv("BOTH_HOST", "pg.internal")

	secrets := map[string]string{"db_pass": "hunter2"}
	secretFn := func(name string) (string, bool) {
		v, ok := secrets[name]
		return v, ok
	}

	dir := t.TempDir()
	yamlPath := dir + "/config.yaml"
	writeFile(t, yamlPath, `
db:
  host: "${BOTH_HOST}"
  password: "<db_pass>"
`)

	store := NewStore(yamlPath,
		WithExpansion(EnvExpander()),
		WithAngleBracketExpander(secretFn),
	)
	connectStore(t, store)
	defer store.Close(t.Context())

	host := storeGet(t, store, "db", "host")
	if s, _ := host.String(); s != "pg.internal" {
		t.Errorf("host: got %q, want pg.internal", s)
	}

	pw := storeGet(t, store, "db", "password")
	if s, _ := pw.String(); s != "hunter2" {
		t.Errorf("password: got %q, want hunter2", s)
	}
}

func TestStoreExpansionDisabledByDefault(t *testing.T) {
	t.Setenv("NO_EXPAND_DOLLAR", "should-not-appear")
	t.Setenv("NO_EXPAND_ANGLE", "should-not-appear")

	dir := t.TempDir()
	yamlPath := dir + "/config.yaml"
	writeFile(t, yamlPath, `
app:
  dollar: "${NO_EXPAND_DOLLAR}"
  angle: "<NO_EXPAND_ANGLE>"
`)

	store := NewStore(yamlPath) // no expansion options
	connectStore(t, store)
	defer store.Close(t.Context())

	for _, tc := range []struct{ key, want string }{
		{"dollar", "${NO_EXPAND_DOLLAR}"},
		{"angle", "<NO_EXPAND_ANGLE>"},
	} {
		val := storeGet(t, store, "app", tc.key)
		s, err := val.String()
		if err != nil {
			t.Fatalf("String(): %v", err)
		}
		if s != tc.want {
			t.Errorf("key %q: got %q, want %q (expansion should be disabled)", tc.key, s, tc.want)
		}
	}
}
