package file

// This file re-exports the subset of the expand package API needed by the
// file store, keeping file-specific option wiring in one place.
//
// Parse-time expansion (this package) vs query-time expansion (expand.Store):
//
//   - Parse-time: placeholders are resolved once when the config file is read.
//     Faster reads; env-var changes are not picked up until the file reloads.
//   - Query-time: use expand.NewStore to wrap any backend; placeholders are
//     resolved on every Get/Find so source changes are reflected immediately.

import "github.com/rbaliyan/config/expand"

// ExpanderFunc resolves a placeholder name to its substitution value.
// This is an alias for expand.ExpanderFunc so callers need not import
// the expand package just to pass a custom resolver.
type ExpanderFunc = expand.ExpanderFunc

// EnvExpander returns an ExpanderFunc backed by os.LookupEnv.
// Convenience re-export of expand.EnvExpander.
func EnvExpander() ExpanderFunc { return expand.EnvExpander() }

// ChainExpanders returns an ExpanderFunc that tries each fn in order.
// Convenience re-export of expand.ChainExpanders.
func ChainExpanders(fns ...ExpanderFunc) ExpanderFunc { return expand.ChainExpanders(fns...) }
