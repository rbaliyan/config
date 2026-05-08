package k8s

import (
	"context"
	"sync"

	"github.com/rbaliyan/config"
)

// watchEntry represents a single subscriber to Store.Watch.
type watchEntry struct {
	filter    config.WatchFilter
	ch        chan config.ChangeEvent
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.Mutex
	closed    bool
	closeOnce sync.Once
}

// notifyWatchers sends an event to all matching watchers.
func (s *Store) notifyWatchers(event config.ChangeEvent) {
	if s.closed.Load() {
		return
	}

	s.watchMu.RLock()
	watchers := make([]*watchEntry, 0, len(s.watchers))
	for we := range s.watchers {
		watchers = append(watchers, we)
	}
	s.watchMu.RUnlock()

	for _, we := range watchers {
		if config.MatchesWatchFilter(event, we.filter) {
			s.sendToWatcher(we, event)
		}
	}
}

// sendToWatcher safely sends an event to a watcher, dropping on a full channel.
func (s *Store) sendToWatcher(we *watchEntry, event config.ChangeEvent) {
	we.mu.Lock()
	defer we.mu.Unlock()

	if we.closed {
		return
	}

	select {
	case we.ch <- event:
	case <-we.ctx.Done():
	default:
		// Channel full; drop the event.
	}
}
