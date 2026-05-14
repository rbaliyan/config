package cursor_test

import (
	"bytes"
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/internal/cursor"
)

func TestMarshalUnmarshal_RoundTripsPayload(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		backend string
		payload []byte
	}{
		{"empty payload", "memory", nil},
		{"ascii string payload", "postgres", []byte("prod/us/west")},
		{"binary payload", "mongodb", []byte{0x00, 0xff, 0x7f, 0x01}},
		{"unicode tag", "redis", []byte("naïve-namespace-π")},
		{"long payload", "sqlite", bytes.Repeat([]byte("x"), 1024)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, err := cursor.Marshal(tc.backend, tc.payload)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			if c == "" {
				t.Fatal("Marshal returned empty cursor")
			}
			got, err := cursor.Unmarshal(tc.backend, c)
			if err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			if !bytes.Equal(got, tc.payload) {
				t.Errorf("payload round-trip mismatch: got %x, want %x", got, tc.payload)
			}
		})
	}
}

func TestMarshalString_RoundTrip(t *testing.T) {
	t.Parallel()

	const backend = "memory"
	for _, payload := range []string{"", "prod", "a/b/c", "naïve-namespace-π"} {
		c, err := cursor.MarshalString(backend, payload)
		if err != nil {
			t.Fatalf("MarshalString(%q): %v", payload, err)
		}
		got, err := cursor.UnmarshalString(backend, c)
		if err != nil {
			t.Fatalf("UnmarshalString(%q): %v", payload, err)
		}
		if got != payload {
			t.Errorf("payload mismatch: got %q, want %q", got, payload)
		}
	}
}

func TestMarshal_RejectsEmptyBackend(t *testing.T) {
	t.Parallel()
	if _, err := cursor.Marshal("", []byte("data")); err == nil {
		t.Fatal("expected error for empty backend tag")
	}
}

func TestMarshal_RejectsTooLongBackend(t *testing.T) {
	t.Parallel()
	tag := strings.Repeat("x", 256)
	if _, err := cursor.Marshal(tag, []byte("data")); err == nil {
		t.Fatal("expected error for backend tag > 255 bytes")
	}
}

func TestUnmarshal_RejectsMismatchedBackend(t *testing.T) {
	t.Parallel()
	c, err := cursor.Marshal("postgres", []byte("foo"))
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	_, err = cursor.Unmarshal("memory", c)
	if !errors.Is(err, config.ErrInvalidCursor) {
		t.Errorf("expected ErrInvalidCursor for mismatched backend, got %v", err)
	}
}

func TestUnmarshal_RejectsMalformedBase64(t *testing.T) {
	t.Parallel()
	_, err := cursor.Unmarshal("memory", "!!!not-base64!!!")
	if !errors.Is(err, config.ErrInvalidCursor) {
		t.Errorf("expected ErrInvalidCursor for bad base64, got %v", err)
	}
}

func TestUnmarshal_RejectsEmptyCursor(t *testing.T) {
	t.Parallel()
	_, err := cursor.Unmarshal("memory", "")
	if !errors.Is(err, config.ErrInvalidCursor) {
		t.Errorf("expected ErrInvalidCursor for empty cursor, got %v", err)
	}
}

func TestUnmarshal_RejectsTruncatedEnvelope(t *testing.T) {
	t.Parallel()
	// Single byte envelope cannot contain version + length, must be rejected.
	c := base64.RawURLEncoding.EncodeToString([]byte{0x01})
	_, err := cursor.Unmarshal("memory", c)
	if !errors.Is(err, config.ErrInvalidCursor) {
		t.Errorf("expected ErrInvalidCursor for truncated envelope, got %v", err)
	}
}

func TestUnmarshal_RejectsUnsupportedVersion(t *testing.T) {
	t.Parallel()
	// Fabricate a v0xFF envelope.
	raw := []byte{0xFF, byte(len("memory"))}
	raw = append(raw, "memory"...)
	raw = append(raw, "payload"...)
	c := base64.RawURLEncoding.EncodeToString(raw)
	_, err := cursor.Unmarshal("memory", c)
	if !errors.Is(err, config.ErrInvalidCursor) {
		t.Errorf("expected ErrInvalidCursor for unknown version, got %v", err)
	}
}

func TestUnmarshal_RejectsBackendOverrun(t *testing.T) {
	t.Parallel()
	// Claim a backend length longer than the envelope contains.
	raw := []byte{0x01, 100}
	raw = append(raw, "memory"...)
	c := base64.RawURLEncoding.EncodeToString(raw)
	_, err := cursor.Unmarshal("memory", c)
	if !errors.Is(err, config.ErrInvalidCursor) {
		t.Errorf("expected ErrInvalidCursor for backend overrun, got %v", err)
	}
}

func TestUnmarshal_RejectsZeroLengthBackend(t *testing.T) {
	t.Parallel()
	// version byte + zero-length name; the rest is the payload.
	raw := []byte{0x01, 0x00, 'p', 'a', 'y'}
	c := base64.RawURLEncoding.EncodeToString(raw)
	_, err := cursor.Unmarshal("memory", c)
	if !errors.Is(err, config.ErrInvalidCursor) {
		t.Errorf("expected ErrInvalidCursor for zero-length backend, got %v", err)
	}
}

func TestUnmarshal_RequiresBackendArgument(t *testing.T) {
	t.Parallel()
	c, err := cursor.Marshal("memory", []byte("x"))
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	_, err = cursor.Unmarshal("", c)
	if err == nil {
		t.Fatal("expected error when unmarshalling with empty backend tag")
	}
	if errors.Is(err, config.ErrInvalidCursor) {
		t.Errorf("empty-backend programmer error should not wrap ErrInvalidCursor, got %v", err)
	}
}

func TestUnmarshal_ReturnsPayloadCopy(t *testing.T) {
	t.Parallel()
	c, err := cursor.Marshal("memory", []byte("original"))
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	out, err := cursor.Unmarshal("memory", c)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	// Mutating the returned slice must not poison a subsequent unmarshal.
	out[0] = 'X'
	out2, err := cursor.Unmarshal("memory", c)
	if err != nil {
		t.Fatalf("second Unmarshal: %v", err)
	}
	if string(out2) != "original" {
		t.Errorf("payload mutated across calls: got %q, want %q", out2, "original")
	}
}

func TestIsInvalidCursor_UnwrapsConfigSentinel(t *testing.T) {
	t.Parallel()
	_, err := cursor.Unmarshal("memory", "garbage!!!")
	if err == nil {
		t.Fatal("expected error")
	}
	if !config.IsInvalidCursor(err) {
		t.Errorf("config.IsInvalidCursor returned false for %v", err)
	}
}
