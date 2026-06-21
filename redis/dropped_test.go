package redis

import (
	"context"
	"testing"

	"github.com/rbaliyan/config"
)

// TestStore_DroppedEvents verifies that a full subscriber buffer increments the
// dropped-event counter and invokes the WithOnDropped callback. It drives
// notifyWatchers directly (white-box) so no live Redis is required.
func TestStore_DroppedEvents(t *testing.T) {
	var got []config.ChangeEvent
	s := NewStore(WithOnDropped(func(e config.ChangeEvent) {
		got = append(got, e)
	}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	we := &watchEntry{
		filter: config.WatchFilter{},
		ch:     make(chan config.ChangeEvent, 1),
		ctx:    ctx,
		cancel: cancel,
	}
	s.watchers[we] = struct{}{}

	ev := config.ChangeEvent{Type: config.ChangeTypeSet, Namespace: "ns", Key: "k"}
	s.notifyWatchers(ev) // fills the size-1 buffer
	s.notifyWatchers(ev) // buffer full -> dropped

	if got, want := s.DroppedEvents(), int64(1); got != want {
		t.Errorf("DroppedEvents() = %d, want %d", got, want)
	}
	if len(got) != 1 {
		t.Errorf("onDropped invoked %d times, want 1", len(got))
	}
}
