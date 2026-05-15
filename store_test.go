package config_test

import (
	"encoding/json"
	"maps"
	"testing"

	"github.com/rbaliyan/config"
)

// TestStoreStats_NewStoreStatsNilMaps confirms that passing nil maps to
// the constructor yields a usable snapshot whose iterators yield no
// pairs and whose point lookups return zero. Nil-tolerant construction
// is what makes wrapper stores like otel and expand able to forward a
// "no inner provider" result without first allocating empty maps.
func TestStoreStats_NewStoreStatsNilMaps(t *testing.T) {
	s := config.NewStoreStats(0, nil, nil)
	if s.TotalEntries() != 0 {
		t.Errorf("TotalEntries() = %d, want 0", s.TotalEntries())
	}
	if got := s.CountForType(config.TypeString); got != 0 {
		t.Errorf("CountForType(missing) = %d, want 0", got)
	}
	if got := s.CountForNamespace("missing"); got != 0 {
		t.Errorf("CountForNamespace(missing) = %d, want 0", got)
	}
	var n int
	for range s.EntriesByType() {
		n++
	}
	if n != 0 {
		t.Errorf("EntriesByType yielded %d pairs, want 0", n)
	}
	for range s.EntriesByNamespace() {
		n++
	}
	if n != 0 {
		t.Errorf("EntriesByNamespace yielded %d pairs, want 0", n)
	}
}

// TestStoreStats_ConstructorClonesMaps asserts the snapshot is
// independent of the caller's maps post-construction. This is the
// load-bearing invariant for caching: a shared snapshot must not
// mutate when its constructor's inputs are later mutated.
func TestStoreStats_ConstructorClonesMaps(t *testing.T) {
	byType := map[config.Type]int64{config.TypeInt: 5}
	byNs := map[string]int64{"prod": 3}

	s := config.NewStoreStats(8, byType, byNs)

	// Poison the caller's maps after construction.
	byType[config.TypeInt] = 999
	byType[config.TypeString] = 42
	byNs["prod"] = 999
	byNs["dev"] = 7

	if got := s.CountForType(config.TypeInt); got != 5 {
		t.Errorf("snapshot was poisoned by caller mutation: CountForType(int) = %d, want 5", got)
	}
	if got := s.CountForType(config.TypeString); got != 0 {
		t.Errorf("snapshot leaked new caller key: CountForType(string) = %d, want 0", got)
	}
	if got := s.CountForNamespace("prod"); got != 3 {
		t.Errorf("snapshot was poisoned by caller mutation: CountForNamespace(prod) = %d, want 3", got)
	}
	if got := s.CountForNamespace("dev"); got != 0 {
		t.Errorf("snapshot leaked new caller key: CountForNamespace(dev) = %d, want 0", got)
	}
}

// TestStoreStats_JSONRoundTrip is the wire-contract pin: a snapshot
// produced by MarshalJSON must reconstruct equal counts via
// UnmarshalStoreStats. If anyone ever renames a wire key in
// storeStatsWire, this test breaks loudly instead of silently
// shipping a backward-incompatible JSON shape.
func TestStoreStats_JSONRoundTrip(t *testing.T) {
	original := config.NewStoreStats(7,
		map[config.Type]int64{config.TypeInt: 2, config.TypeString: 5},
		map[string]int64{"prod": 4, "dev": 3},
	)

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := config.UnmarshalStoreStats(data)
	if err != nil {
		t.Fatalf("UnmarshalStoreStats: %v", err)
	}

	if got.TotalEntries() != original.TotalEntries() {
		t.Errorf("TotalEntries round-trip: got %d, want %d",
			got.TotalEntries(), original.TotalEntries())
	}
	for tp, want := range original.EntriesByType() {
		if g := got.CountForType(tp); g != want {
			t.Errorf("byType[%v] = %d, want %d", tp, g, want)
		}
	}
	for ns, want := range original.EntriesByNamespace() {
		if g := got.CountForNamespace(ns); g != want {
			t.Errorf("byNamespace[%q] = %d, want %d", ns, g, want)
		}
	}
}

// TestStoreStats_UnmarshalKnownWireFormat pins the documented JSON
// schema (`total_entries`, `entries_by_type`, `entries_by_namespace`)
// to literal bytes. A wire-key rename or restructure that breaks
// HTTP/JSON dashboards and gateway clients will fail this test before
// it hits production.
func TestStoreStats_UnmarshalKnownWireFormat(t *testing.T) {
	const wire = `{"total_entries":3,"entries_by_type":{"4":2,"1":1},"entries_by_namespace":{"prod":3}}`

	s, err := config.UnmarshalStoreStats([]byte(wire))
	if err != nil {
		t.Fatalf("UnmarshalStoreStats: %v", err)
	}
	if s.TotalEntries() != 3 {
		t.Errorf("TotalEntries = %d, want 3", s.TotalEntries())
	}
	if got := s.CountForNamespace("prod"); got != 3 {
		t.Errorf("CountForNamespace(prod) = %d, want 3", got)
	}
}

// TestStoreStats_UnmarshalMalformed asserts that a non-JSON byte
// stream returns a non-nil error instead of a silent empty snapshot.
func TestStoreStats_UnmarshalMalformed(t *testing.T) {
	_, err := config.UnmarshalStoreStats([]byte("definitely not json {{{"))
	if err == nil {
		t.Fatal("expected error for malformed JSON, got nil")
	}
}

// TestStoreStats_IteratorIsReadOnly demonstrates that the iterator
// returned by EntriesByType / EntriesByNamespace exposes the snapshot
// only by yielded copies of (key, value) — the underlying map is not
// reachable for mutation through the public surface. Equivalent
// guarantees are covered for CountForType/CountForNamespace by
// ConstructorClonesMaps.
func TestStoreStats_IteratorIsReadOnly(t *testing.T) {
	s := config.NewStoreStats(2,
		map[config.Type]int64{config.TypeInt: 2},
		map[string]int64{"ns": 2},
	)
	// Collect to a private map; mutate it; assert the snapshot is
	// unaffected. This is the documented escape hatch.
	private := maps.Collect(s.EntriesByType())
	private[config.TypeInt] = 999
	private[config.TypeString] = 42
	if got := s.CountForType(config.TypeInt); got != 2 {
		t.Errorf("snapshot mutated through Collect copy: got %d, want 2", got)
	}
	if got := s.CountForType(config.TypeString); got != 0 {
		t.Errorf("snapshot leaked through Collect copy: got %d, want 0", got)
	}
}

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
