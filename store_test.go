package config_test

import (
	"testing"

	"github.com/rbaliyan/config"
)

func TestMatchesWatchFilter(t *testing.T) {
	tests := []struct {
		name   string
		event  config.ChangeEvent
		filter config.WatchFilter
		want   bool
	}{
		{
			name:   "empty filter matches everything",
			event:  config.ChangeEvent{Namespace: "ns", Key: "key"},
			filter: config.WatchFilter{},
			want:   true,
		},
		{
			name:  "namespace match",
			event: config.ChangeEvent{Namespace: "ns", Key: "key"},
			filter: config.WatchFilter{
				Namespaces: []string{"ns"},
			},
			want: true,
		},
		{
			name:  "namespace no match",
			event: config.ChangeEvent{Namespace: "other", Key: "key"},
			filter: config.WatchFilter{
				Namespaces: []string{"ns"},
			},
			want: false,
		},
		{
			name:  "prefix match",
			event: config.ChangeEvent{Namespace: "ns", Key: "app/debug"},
			filter: config.WatchFilter{
				Prefixes: []string{"app/"},
			},
			want: true,
		},
		{
			name:  "prefix no match",
			event: config.ChangeEvent{Namespace: "ns", Key: "db/host"},
			filter: config.WatchFilter{
				Prefixes: []string{"app/"},
			},
			want: false,
		},
		{
			name:  "namespace and prefix both match",
			event: config.ChangeEvent{Namespace: "ns", Key: "app/debug"},
			filter: config.WatchFilter{
				Namespaces: []string{"ns"},
				Prefixes:   []string{"app/"},
			},
			want: true,
		},
		{
			name:  "namespace matches but prefix does not",
			event: config.ChangeEvent{Namespace: "ns", Key: "db/host"},
			filter: config.WatchFilter{
				Namespaces: []string{"ns"},
				Prefixes:   []string{"app/"},
			},
			want: false,
		},
		{
			name:  "namespace does not match but prefix does",
			event: config.ChangeEvent{Namespace: "other", Key: "app/debug"},
			filter: config.WatchFilter{
				Namespaces: []string{"ns"},
				Prefixes:   []string{"app/"},
			},
			want: false,
		},
		{
			name:  "multiple namespaces one matches",
			event: config.ChangeEvent{Namespace: "b", Key: "key"},
			filter: config.WatchFilter{
				Namespaces: []string{"a", "b", "c"},
			},
			want: true,
		},
		{
			name:  "multiple prefixes one matches",
			event: config.ChangeEvent{Namespace: "ns", Key: "db/host"},
			filter: config.WatchFilter{
				Prefixes: []string{"app/", "db/", "cache/"},
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := config.MatchesWatchFilter(tt.event, tt.filter)
			if got != tt.want {
				t.Errorf("MatchesWatchFilter() = %v, want %v", got, tt.want)
			}
		})
	}
}
