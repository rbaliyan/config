package config_test

import (
	"context"
	"testing"
	"time"

	"github.com/rbaliyan/config"
)

func TestCloneValue_CopiesMetadataAndIsIndependent(t *testing.T) {
	ctx := context.Background()
	created := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	updated := created.Add(time.Hour)

	src := config.NewValue(map[string]any{"a": 1, "nested": map[string]any{"b": 2}},
		config.WithValueMetadata(7, created, updated),
		config.WithValueEntryID("entry-99"),
		config.WithValueExpiresAt(updated.Add(24*time.Hour)),
		config.WithValueWriteMode(config.WriteModeCreate),
	)

	clone := config.CloneValue(src, "fresh-content")

	// Metadata must be copied verbatim.
	cm := clone.Metadata()
	if cm.Version() != 7 {
		t.Errorf("clone version = %d, want 7", cm.Version())
	}
	if !cm.CreatedAt().Equal(created) {
		t.Errorf("clone createdAt = %v, want %v", cm.CreatedAt(), created)
	}
	if !cm.UpdatedAt().Equal(updated) {
		t.Errorf("clone updatedAt = %v, want %v", cm.UpdatedAt(), updated)
	}
	if got := config.EntryID(clone); got != "entry-99" {
		t.Errorf("clone EntryID = %q, want entry-99", got)
	}
	if !cm.ExpiresAt().Equal(updated.Add(24 * time.Hour)) {
		t.Errorf("clone expiresAt = %v, want copied TTL", cm.ExpiresAt())
	}
	if got := config.GetWriteMode(clone); got != config.WriteModeCreate {
		t.Errorf("clone WriteMode = %v, want WriteModeCreate", got)
	}

	// The clone carries the new raw content, not the source content.
	got, err := clone.String()
	if err != nil {
		t.Fatalf("clone String: %v", err)
	}
	if got != "fresh-content" {
		t.Errorf("clone content = %q, want fresh-content", got)
	}

	// Mutating the clone's metadata struct must not affect the source: they
	// must be distinct *valueMetadata instances. We verify independence by
	// checking the source still reports its original entry ID after the clone
	// was created with a (potentially overriding) option below.
	clone2 := config.CloneValue(src, "other", config.WithValueEntryID("changed-id"))
	if config.EntryID(clone2) != "changed-id" {
		t.Errorf("clone2 EntryID override = %q, want changed-id", config.EntryID(clone2))
	}
	if config.EntryID(src) != "entry-99" {
		t.Errorf("source EntryID mutated to %q; clone must not alias source metadata", config.EntryID(src))
	}

	_ = ctx
}

func TestCloneValue_StaleFlagNotCopied(t *testing.T) {
	src := config.NewValue("v",
		config.WithValueMetadata(1, time.Now(), time.Now()),
		config.WithValueStale(true),
	)
	if !src.Metadata().IsStale() {
		t.Fatal("source should be stale")
	}
	clone := config.CloneValue(src, "v2")
	if clone.Metadata().IsStale() {
		t.Error("clone must not inherit the stale flag")
	}
}

func TestEntryID(t *testing.T) {
	// nil value → empty.
	if got := config.EntryID(nil); got != "" {
		t.Errorf("EntryID(nil) = %q, want empty", got)
	}

	// Value without entry ID → empty.
	noID := config.NewValue("x")
	if got := config.EntryID(noID); got != "" {
		t.Errorf("EntryID(no-id) = %q, want empty", got)
	}

	// Value with entry ID → that ID.
	withID := config.NewValue("x", config.WithValueEntryID("abc-123"))
	if got := config.EntryID(withID); got != "abc-123" {
		t.Errorf("EntryID = %q, want abc-123", got)
	}
}
