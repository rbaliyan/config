package expand

import (
	"strings"
	"testing"
)

// FuzzExpandDollar drives the hand-written ${VAR} / ${VAR:-default} parser in
// Dollar with arbitrary input strings and several expander functions.
//
// Oracles (only invariants the parser actually guarantees):
//  1. No panic on any input. The parser walks bytes with manual index math, so
//     an off-by-one or slice-bound bug would surface as a panic here.
//  2. Bounded output under the empty expander: when nothing resolves and no
//     default applies, every placeholder is written back literally and every
//     escape collapses, so the output length can never exceed the input length.
//     (No substitution path can grow the string.)
//  3. Termination: Dollar always returns. Dollar is single-pass — it does not
//     re-scan its own substitutions — so even pathological nested inputs like
//     "${:-${:-}}" complete in one walk. The fuzzer exercising this without
//     hanging is itself the assertion (libFuzzer would flag a timeout).
//
// Note: idempotence (Dollar(Dollar(s)) == Dollar(s)) is deliberately NOT
// asserted. It does not hold and is not promised: a single pass can emit a
// string that is itself a valid placeholder (e.g. "${:-${:-}}" -> "${:-}"),
// and a second pass would expand that. Single-pass means "no internal rescan",
// not "fixpoint".
func FuzzExpandDollar(f *testing.F) {
	seeds := []string{
		"",
		"plain text",
		"${HOST}",
		"${HOST:-localhost}",
		"${A}${B}${C}",
		`\${HOST}`,
		`\\`,
		"${unclosed",
		"${}",
		"${:-only-default}",
		"${:-${:-}}", // nested-looking; first '}' wins (greedy)
		"prefix ${X:-d} suffix",
		"$",
		"${{nested}}",
		"a$b$c",
		"${é中文}",
		strings.Repeat("${X}", 64),
	}
	for _, s := range seeds {
		f.Add(s)
	}

	emptyExpander := func(string) (string, bool) { return "", false }
	identityExpander := func(name string) (string, bool) { return name, true }

	f.Fuzz(func(t *testing.T, s string) {
		// Oracle 1 + 2: empty expander rewrites everything literally / collapses
		// escapes, so output never grows.
		out := Dollar(s, emptyExpander)
		if len(out) > len(s) {
			t.Fatalf("empty-expander output grew: in=%q (%d) out=%q (%d)", s, len(s), out, len(out))
		}

		// Oracle 1 (other branches): the resolving and env branches must also
		// never panic. Oracle 3 (termination) is implicit: these calls return.
		_ = Dollar(s, identityExpander)
		_ = Dollar(s, EnvExpander())
	})
}

// FuzzExpandAngle drives the hand-written <VAR> parser in Angle. Oracles mirror
// FuzzExpandDollar: no panic, empty-expander output never grows, and the parser
// always terminates (single-pass, no rescan of its own output).
func FuzzExpandAngle(f *testing.F) {
	seeds := []string{
		"",
		"plain",
		"<VAR>",
		"<a><b><c>",
		`\<VAR>`,
		`\\`,
		"<unclosed",
		"<>",
		"<1bad>",      // digit-first: not a valid name, written literally
		"a < b",       // comparison operator, not a token
		"<has space>", // space: not a valid name
		"<div>",       // valid HTML-tag-like name
		"<with-hyphen>",
		"<_under>",
		"<é中文>",
		strings.Repeat("<X>", 64),
	}
	for _, s := range seeds {
		f.Add(s)
	}

	emptyExpander := func(string) (string, bool) { return "", false }
	identityExpander := func(name string) (string, bool) { return name, true }

	f.Fuzz(func(t *testing.T, s string) {
		out := Angle(s, emptyExpander)
		if len(out) > len(s) {
			t.Fatalf("empty-expander output grew: in=%q (%d) out=%q (%d)", s, len(s), out, len(out))
		}

		_ = Angle(s, identityExpander)
		_ = Angle(s, EnvExpander())
	})
}
