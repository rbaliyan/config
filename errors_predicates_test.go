package config_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/rbaliyan/config"
)

func TestErrorPredicates(t *testing.T) {
	cases := []struct {
		name string
		pred func(error) bool
		// match holds errors the predicate must accept (incl. wrapped form).
		match error
		// nonMatch holds a different sentinel the predicate must reject.
		nonMatch error
	}{
		{
			name:     "IsUnsupportedCodec",
			pred:     config.IsUnsupportedCodec,
			match:    config.ErrUnsupportedCodec,
			nonMatch: config.ErrVersionNotFound,
		},
		{
			name:     "IsVersionNotFound",
			pred:     config.IsVersionNotFound,
			match:    config.ErrVersionNotFound,
			nonMatch: config.ErrVersioningNotSupported,
		},
		{
			name:     "IsVersioningNotSupported",
			pred:     config.IsVersioningNotSupported,
			match:    config.ErrVersioningNotSupported,
			nonMatch: config.ErrVersionNotFound,
		},
		{
			name:     "IsInvalidCursor",
			pred:     config.IsInvalidCursor,
			match:    config.ErrInvalidCursor,
			nonMatch: config.ErrUnsupportedCodec,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Direct sentinel matches.
			if !tc.pred(tc.match) {
				t.Errorf("%s(%v) = false, want true (direct match)", tc.name, tc.match)
			}
			// Wrapped sentinel still matches via errors.Is.
			wrapped := fmt.Errorf("context: %w", tc.match)
			if !tc.pred(wrapped) {
				t.Errorf("%s(wrapped) = false, want true", tc.name)
			}
			// nil never matches.
			if tc.pred(nil) {
				t.Errorf("%s(nil) = true, want false", tc.name)
			}
			// A different sentinel must not match.
			if tc.pred(tc.nonMatch) {
				t.Errorf("%s(%v) = true, want false (non-match)", tc.name, tc.nonMatch)
			}
			// An arbitrary unrelated error must not match.
			if tc.pred(errors.New("unrelated")) {
				t.Errorf("%s(unrelated) = true, want false", tc.name)
			}
		})
	}
}
