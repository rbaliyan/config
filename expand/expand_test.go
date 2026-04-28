package expand_test

import (
	"testing"

	"github.com/rbaliyan/config/expand"
)

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
		{"inline", "connect to ${HOST} on ${PORT}", "connect to localhost on 5432"},
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

func TestAngleNoOp(t *testing.T) {
	fn := func(string) (string, bool) { return "", false }
	input := "no angle brackets here"
	if got := expand.Angle(input, fn); got != input {
		t.Errorf("Angle(%q) = %q, want unchanged", input, got)
	}
}

func TestEnvExpander(t *testing.T) {
	t.Setenv("EXPAND_TEST_VAR", "hello")
	fn := expand.EnvExpander()

	v, ok := fn("EXPAND_TEST_VAR")
	if !ok || v != "hello" {
		t.Errorf("EnvExpander: got %q, %v; want %q, true", v, ok, "hello")
	}
	_, ok = fn("EXPAND_TEST_MISSING_XYZ")
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
			return "shadowed", true
		}
		return "", false
	}

	chain := expand.ChainExpanders(first, second)

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

func TestChainExpandersEmpty(t *testing.T) {
	chain := expand.ChainExpanders()
	if _, ok := chain("X"); ok {
		t.Error("empty chain should never find a key")
	}
}

func TestDollarSinglePass(t *testing.T) {
	// Expanded values must not be re-scanned.
	lookup := func(name string) (string, bool) {
		if name == "A" {
			return "${B}", true
		}
		if name == "B" {
			return "expanded-b", true
		}
		return "", false
	}
	got := expand.Dollar("${A}", lookup)
	if got != "${B}" {
		t.Errorf("Dollar single-pass: got %q, want ${B} (no re-scan)", got)
	}
}

func TestAngleSinglePass(t *testing.T) {
	lookup := func(name string) (string, bool) {
		if name == "A" {
			return "<B>", true
		}
		if name == "B" {
			return "expanded-b", true
		}
		return "", false
	}
	got := expand.Angle("<A>", lookup)
	if got != "<B>" {
		t.Errorf("Angle single-pass: got %q, want <B> (no re-scan)", got)
	}
}
