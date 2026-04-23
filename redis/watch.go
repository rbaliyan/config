package redis

import (
	"context"
	"sync"

	"github.com/rbaliyan/config"
)

type watchEntry struct {
	filter    config.WatchFilter
	ch        chan config.ChangeEvent
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.Mutex
	closed    bool
	closeOnce sync.Once
}

func (s *Store) notifyWatchers(event config.ChangeEvent) {
	s.watchMu.RLock()
	watchers := make([]*watchEntry, 0, len(s.watchers))
	for we := range s.watchers {
		watchers = append(watchers, we)
	}
	s.watchMu.RUnlock()

	for _, we := range watchers {
		s.sendToWatcher(we, event)
	}
}

func (s *Store) sendToWatcher(we *watchEntry, event config.ChangeEvent) {
	if !config.MatchesWatchFilter(event, we.filter) {
		return
	}
	we.mu.Lock()
	defer we.mu.Unlock()
	if we.closed {
		return
	}
	select {
	case we.ch <- event:
	default:
	}
}
