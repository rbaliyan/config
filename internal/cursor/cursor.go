// Package cursor provides a standardized envelope for opaque pagination
// cursors used across config store backends.
//
// This package is for backend implementors only. Callers of
// [github.com/rbaliyan/config.NamespaceLister.ListNamespaces] never construct
// or inspect cursors directly — they pass the empty string for the first
// page and round-trip whatever the backend returned.
//
// Cursors emitted by one backend should never be accepted by another: if a
// caller swaps stores while holding a cursor, the new store has no way to
// honour the old cursor's internal format. The envelope solves this by
// tagging every cursor with the backend that produced it and a protocol
// version so future format changes can be detected.
//
// Wire format (before base64-url encoding):
//
//	byte 0:           version (currently 0x01)
//	byte 1:           backend name length (uint8, must be > 0)
//	bytes 2..2+n-1:   backend name (UTF-8)
//	bytes 2+n..end:   opaque payload bytes
//
// The whole envelope is base64-url encoded (no padding) so the result is
// safe to transport in URL query strings, gRPC strings, and JSON without
// further escaping.
//
// [Unmarshal] returns an error that wraps
// [github.com/rbaliyan/config.ErrInvalidCursor] for any malformed input,
// unsupported protocol version, or backend tag mismatch. Backends MUST NOT
// call [Unmarshal] with an empty cursor string: an empty cursor at the
// interface level signals "first page" and must be handled by the caller
// before reaching the decode path.
package cursor

import (
	"encoding/base64"
	"fmt"

	"github.com/rbaliyan/config"
)

// currentVersion is the protocol version emitted by [Marshal]. Bumping this
// is a breaking format change; the new Unmarshal must continue to recognise
// older versions for in-flight cursor compatibility.
const currentVersion byte = 0x01

// maxBackendLen is the upper bound on the backend tag length. The wire
// format uses a single byte for the length prefix, so 255 is the hard cap.
const maxBackendLen = 255

// Marshal builds an opaque cursor string from a backend tag and an
// arbitrary opaque payload. The backend tag must be non-empty and at most
// 255 bytes when UTF-8 encoded. The returned string is safe to round-trip
// through [github.com/rbaliyan/config.NamespaceLister.ListNamespaces] as
// the nextCursor return value; callers of that interface never inspect
// the string contents.
func Marshal(backend string, payload []byte) (string, error) {
	if backend == "" {
		return "", fmt.Errorf("cursor: backend tag is required")
	}
	if len(backend) > maxBackendLen {
		return "", fmt.Errorf("cursor: backend tag %q exceeds %d bytes", backend, maxBackendLen)
	}
	buf := make([]byte, 0, 2+len(backend)+len(payload))
	buf = append(buf, currentVersion)
	buf = append(buf, byte(len(backend))) // #nosec G115 -- bounded by the len(backend) > maxBackendLen check above (255)
	buf = append(buf, backend...)
	buf = append(buf, payload...)
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

// Unmarshal decodes an opaque cursor string back into its payload after
// verifying the envelope: protocol version must be supported, backend tag
// must match `backend` exactly, and the byte layout must be well-formed.
//
// Any failure returns an error that wraps [config.ErrInvalidCursor]. An
// empty cursor is rejected — callers must short-circuit "first page"
// requests before calling Unmarshal.
func Unmarshal(backend, cursor string) ([]byte, error) {
	if cursor == "" {
		return nil, wrapInvalid("cursor: empty cursor")
	}
	if backend == "" {
		return nil, fmt.Errorf("cursor: backend tag is required for unmarshal")
	}

	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, wrapInvalid(fmt.Sprintf("cursor: base64 decode: %v", err))
	}
	if len(raw) < 2 {
		return nil, wrapInvalid("cursor: truncated envelope")
	}
	// Version dispatch. The shape is a switch — not an equality check —
	// so the next bumper has to keep an explicit case for v1 alive. A
	// stray default that swallowed old envelopes would break in-flight
	// clients silently, which is exactly what the versioning is here to
	// prevent.
	switch raw[0] {
	case currentVersion:
		return decodeV1(raw, backend)
	default:
		return nil, wrapInvalid(fmt.Sprintf("cursor: unsupported version 0x%02x", raw[0]))
	}
}

// decodeV1 parses the v1 envelope layout. Kept as a separate function so
// future protocol versions live in their own decoder and the dispatch in
// [Unmarshal] is the only place that needs editing.
func decodeV1(raw []byte, backend string) ([]byte, error) {
	if len(raw) < 2 {
		// Defensive — the caller already checked, but the version-
		// dispatched decoder shouldn't assume.
		return nil, wrapInvalid("cursor: truncated v1 envelope")
	}
	nameLen := int(raw[1])
	if nameLen == 0 {
		return nil, wrapInvalid("cursor: empty backend tag")
	}
	if 2+nameLen > len(raw) {
		return nil, wrapInvalid("cursor: backend tag overruns envelope")
	}
	got := string(raw[2 : 2+nameLen])
	if got != backend {
		return nil, wrapInvalid(fmt.Sprintf("cursor: backend tag %q does not match expected %q", got, backend))
	}
	// Return a copy so callers can retain the payload independently of
	// the decode buffer.
	payload := raw[2+nameLen:]
	out := make([]byte, len(payload))
	copy(out, payload)
	return out, nil
}

// MarshalString is a convenience helper for the common case where the
// payload is a single string (e.g. the last-emitted namespace name in a
// keyset-paginated query). An empty payload round-trips faithfully through
// [UnmarshalString] — backends use the empty cursor string at the
// interface level to signal "first page," but the envelope itself never
// holds an empty cursor (the interface caller short-circuits before
// reaching Marshal).
func MarshalString(backend, payload string) (string, error) {
	return Marshal(backend, []byte(payload))
}

// UnmarshalString is the inverse of [MarshalString]. Returns the same
// errors as [Unmarshal]; an empty cursor input is rejected (use the
// interface-level empty string as the "first page" signal instead).
func UnmarshalString(backend, cursor string) (string, error) {
	b, err := Unmarshal(backend, cursor)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// invalidCursorError carries a descriptive message but unwraps to
// [config.ErrInvalidCursor] so callers can use errors.Is.
type invalidCursorError struct {
	msg string
}

func (e *invalidCursorError) Error() string { return e.msg }
func (e *invalidCursorError) Unwrap() error { return config.ErrInvalidCursor }

func wrapInvalid(msg string) error {
	return &invalidCursorError{msg: msg}
}
