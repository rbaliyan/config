// Package expand provides variable substitution for configuration values.
//
// It supports two placeholder syntaxes, each independently enabled:
//
//   - ${VAR} and ${VAR:-default}  — shell-style, enabled with WithDollarExpander
//   - <VAR>                       — angle-bracket style, enabled with WithAngleExpander
//
// Expansion is single-pass: the substituted result is not re-scanned for
// further placeholders, preventing infinite loops.
//
// # Backslash escaping
//
// A backslash immediately before a placeholder opener disables substitution:
//
//	\${HOST}   →  ${HOST}   (literal, not expanded)
//	\<HOST>    →  <HOST>    (literal, not expanded)
//	\\         →  \         (escaped backslash)
//
// # Usage
//
// Parse-time expansion (applied when a file store loads the file):
//
//	store := file.NewStore("config.yaml",
//	    file.WithExpansion(expand.EnvExpander()),
//	    file.WithAngleBracketExpander(secretFn),
//	)
//
// Query-time expansion (applied on every Get/Find, works with any backend):
//
//	inner, _ := memory.NewStore()
//	inner.Connect(ctx)
//	s := expand.NewStore(inner,
//	    expand.WithDollarExpander(expand.EnvExpander()),
//	    expand.WithAngleExpander(vaultFn),
//	)
package expand

import (
	"os"
	"strings"
)

// ExpanderFunc resolves a placeholder name to its substitution value.
// Returning (_, false) signals "not found"; when no default is configured
// for that placeholder, the token is left unchanged in the output.
type ExpanderFunc func(name string) (value string, ok bool)

// EnvExpander returns an ExpanderFunc backed by os.LookupEnv.
func EnvExpander() ExpanderFunc {
	return func(name string) (string, bool) {
		return os.LookupEnv(name)
	}
}

// ChainExpanders returns an ExpanderFunc that tries each fn in order,
// returning the first (value, true) result. Useful for layering multiple
// resolution sources (e.g. Vault → env → static defaults).
func ChainExpanders(fns ...ExpanderFunc) ExpanderFunc {
	return func(name string) (string, bool) {
		for _, fn := range fns {
			if v, ok := fn(name); ok {
				return v, true
			}
		}
		return "", false
	}
}

// Dollar replaces ${VAR} and ${VAR:-default} tokens in s using fn.
//
// Rules:
//   - ${VAR}          → fn("VAR"); left unchanged if not found and no default.
//   - ${VAR:-default} → fn("VAR"); uses "default" when not found.
//   - \${VAR}         → literal "${VAR}".
//   - \\              → literal "\".
//   - Unclosed ${     → written literally.
func Dollar(s string, fn ExpanderFunc) string {
	if !strings.ContainsAny(s, "$\\") {
		return s
	}

	var b strings.Builder
	b.Grow(len(s))

	i := 0
	for i < len(s) {
		ch := s[i]

		if ch == '\\' && i+1 < len(s) {
			switch s[i+1] {
			case '$':
				b.WriteByte('$')
				i += 2
				continue
			case '\\':
				b.WriteByte('\\')
				i += 2
				continue
			}
		}

		if ch == '$' && i+1 < len(s) && s[i+1] == '{' {
			rest := s[i+2:]
			end := strings.IndexByte(rest, '}')
			if end == -1 {
				b.WriteString(s[i:])
				return b.String()
			}
			token := rest[:end]
			i += 2 + end + 1

			name, def, hasDefault := strings.Cut(token, ":-")
			if v, ok := fn(name); ok {
				b.WriteString(v)
			} else if hasDefault {
				b.WriteString(def)
			} else {
				b.WriteString("${")
				b.WriteString(token)
				b.WriteString("}")
			}
			continue
		}

		b.WriteByte(ch)
		i++
	}
	return b.String()
}

// Angle replaces <VAR> tokens in s using fn.
//
// Only identifiers are matched: a name must start with a letter or underscore
// and contain only letters, digits, underscores, or hyphens. Tokens with
// digit-first names (<1bad>), spaces, or special characters are written
// literally, which filters out comparison operators (a < b) and many HTML
// constructs. However, valid single-word HTML tag names (e.g. <div>, <span>)
// are syntactically matched and will be substituted if the expander has a key
// with that name. Use \<tag> to write any literal <…> sequence.
//
// Rules:
//   - <VAR>   → fn("VAR"); left unchanged if not found.
//   - \<VAR>  → literal "<VAR>".
//   - \\      → literal "\".
//   - Unclosed < or non-identifier content → written literally.
func Angle(s string, fn ExpanderFunc) string {
	if !strings.ContainsAny(s, "<\\") {
		return s
	}

	var b strings.Builder
	b.Grow(len(s))

	i := 0
	for i < len(s) {
		ch := s[i]

		if ch == '\\' && i+1 < len(s) {
			switch s[i+1] {
			case '<':
				b.WriteByte('<')
				i += 2
				continue
			case '\\':
				b.WriteByte('\\')
				i += 2
				continue
			}
		}

		if ch == '<' && i+1 < len(s) && isNameStart(s[i+1]) {
			rest := s[i+1:]
			end := strings.IndexByte(rest, '>')
			if end != -1 {
				name := rest[:end]
				if isValidName(name) {
					i += 1 + end + 1
					if v, ok := fn(name); ok {
						b.WriteString(v)
					} else {
						b.WriteByte('<')
						b.WriteString(name)
						b.WriteByte('>')
					}
					continue
				}
			}
		}

		b.WriteByte(ch)
		i++
	}
	return b.String()
}

func isNameStart(c byte) bool {
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_'
}

func isValidName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if (c < 'A' || c > 'Z') && (c < 'a' || c > 'z') &&
			(c < '0' || c > '9') && c != '_' && c != '-' {
			return false
		}
	}
	return true
}
