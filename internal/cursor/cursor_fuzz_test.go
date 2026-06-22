package cursor

import (
	"bytes"
	"errors"
	"testing"

	"github.com/rbaliyan/config"
)

// FuzzCursorUnmarshal exercises the cursor envelope decoder with three strong
// oracles:
//
//  1. Round-trip identity — for any valid (backend, payload), Marshal then
//     Unmarshal with the same backend must return the original payload bytes.
//  2. Raw-bytes safety — Unmarshal of arbitrary fuzzed bytes must never panic,
//     and any error it returns must wrap config.ErrInvalidCursor.
//  3. Foreign-tag rejection — a cursor marshalled under one backend tag must be
//     rejected (with ErrInvalidCursor) when decoded expecting a different tag.
//
// The fuzzer drives all three from a single (backend, otherBackend, payload,
// rawCursor) tuple so the corpus minimiser can shrink each independently.
func FuzzCursorUnmarshal(f *testing.F) {
	// Valid envelope seeds: (backend, otherBackend, payload, rawCursor).
	// rawCursor is independently fuzzed for the raw-bytes-safety oracle.
	f.Add("memory", "postgres", []byte("ns-1"), "")
	f.Add("postgres", "mongodb", []byte(""), "AQdtZW1vcnk") // valid-ish memory cursor with empty payload
	f.Add("sqlite", "memory", []byte("prod/us/west1"), "not-base64!")
	f.Add("mongodb", "sqlite", []byte("\x00\x01\x02\xff"), "AA")   // truncated raw
	f.Add("b", "a", []byte("x"), "AQ")                             // version byte only, truncated
	f.Add("backend", "other", []byte("payload"), "AgdiYWNrZW5keA") // wrong version (0x02) prefix
	f.Add("memory", "postgres", make([]byte, 256), "")             // large payload
	f.Add("ns", "sn", []byte("é中文"), "////")                       // unicode payload, junk cursor

	f.Fuzz(func(t *testing.T, backend, other string, payload []byte, rawCursor string) {
		// --- Oracle 2: raw-bytes safety on the fuzzed cursor string ---
		// Must never panic; any error must wrap config.ErrInvalidCursor.
		// Use a fixed, non-empty backend so the empty-backend guard (which
		// returns a non-ErrInvalidCursor error by design) is not exercised here.
		if got, err := Unmarshal("memory", rawCursor); err != nil {
			if !errors.Is(err, config.ErrInvalidCursor) {
				t.Fatalf("Unmarshal(%q) returned error not wrapping ErrInvalidCursor: %v", rawCursor, err)
			}
		} else {
			_ = got // success is acceptable (the fuzzed string happened to be a valid memory cursor)
		}

		// The remaining oracles need a valid, non-empty backend tag within the
		// length bound. Skip tuples Marshal would reject; that path is covered
		// by unit tests, and feeding it here just produces uninteresting noise.
		if backend == "" || len(backend) > maxBackendLen {
			return
		}

		// --- Oracle 1: round-trip identity ---
		cur, err := Marshal(backend, payload)
		if err != nil {
			t.Fatalf("Marshal(%q, %d bytes) unexpectedly failed: %v", backend, len(payload), err)
		}
		got, err := Unmarshal(backend, cur)
		if err != nil {
			t.Fatalf("Unmarshal of own Marshal output failed: %v", err)
		}
		// Marshal of an empty payload yields an empty (but non-nil-meaningful)
		// payload; normalise both sides to compare bytes regardless of nil-ness.
		if !bytes.Equal(got, payload) {
			t.Fatalf("round-trip payload mismatch: got %q want %q", got, payload)
		}

		// --- Oracle 3: foreign-tag rejection ---
		// Only meaningful when the two tags actually differ and the other tag
		// is itself a valid tag for Unmarshal's backend-required guard.
		if other != "" && other != backend {
			_, err := Unmarshal(other, cur)
			if err == nil {
				t.Fatalf("cursor marshalled with tag %q was accepted under tag %q", backend, other)
			}
			if !errors.Is(err, config.ErrInvalidCursor) {
				t.Fatalf("foreign-tag rejection error does not wrap ErrInvalidCursor: %v", err)
			}
		}
	})
}
