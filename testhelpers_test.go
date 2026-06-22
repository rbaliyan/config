package config_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"time"

	"github.com/rbaliyan/config"
)

// failingStore wraps a store and can simulate failures for resilience testing
type failingStore struct {
	config.Store
	failGet atomic.Bool
}

func (s *failingStore) Get(ctx context.Context, namespace, key string) (config.Value, error) {
	if s.failGet.Load() {
		return nil, errors.New("simulated store failure")
	}
	return s.Store.Get(ctx, namespace, key)
}

// customValue is a Value implementation that is NOT the internal *val type,
// used to test the staleValueWrapper path in MarkStale.
type customValue struct {
	raw  string
	meta *customMetadata
}

type customMetadata struct {
	version int64
}

func (m *customMetadata) Version() int64       { return m.version }
func (m *customMetadata) CreatedAt() time.Time { return time.Time{} }
func (m *customMetadata) UpdatedAt() time.Time { return time.Time{} }
func (m *customMetadata) ExpiresAt() time.Time { return time.Time{} }
func (m *customMetadata) IsStale() bool        { return false }

func (v *customValue) Marshal(_ context.Context) ([]byte, error) { return json.Marshal(v.raw) }
func (v *customValue) Unmarshal(_ context.Context, target any) error {
	data, _ := json.Marshal(v.raw)
	return json.Unmarshal(data, target)
}
func (v *customValue) Type() config.Type         { return config.TypeString }
func (v *customValue) Codec() string             { return "json" }
func (v *customValue) Metadata() config.Metadata { return v.meta }
func (v *customValue) Int64() (int64, error) {
	return 0, errors.New("not an int")
}
func (v *customValue) Float64() (float64, error) {
	return 0, errors.New("not a float")
}
func (v *customValue) String() (string, error) {
	return v.raw, nil
}
func (v *customValue) Bool() (bool, error) {
	return false, errors.New("not a bool")
}

// watchFailStore is a store that fails Watch on demand, to test watchChanges error paths.
type watchFailStore struct {
	config.Store
	watchFails atomic.Int32
	watchCalls atomic.Int32
}

func (s *watchFailStore) Watch(ctx context.Context, filter config.WatchFilter) (<-chan config.ChangeEvent, error) {
	call := s.watchCalls.Add(1)
	remaining := s.watchFails.Load()
	if remaining > 0 && call <= remaining {
		return nil, errors.New("simulated watch failure")
	}
	return s.Store.Watch(ctx, filter)
}

// watchNotSupportedStore is a store that returns ErrWatchNotSupported.
type watchNotSupportedStore struct {
	config.Store
}

func (s *watchNotSupportedStore) Watch(ctx context.Context, filter config.WatchFilter) (<-chan config.ChangeEvent, error) {
	return nil, config.ErrWatchNotSupported
}

// healthCheckStore is a store that implements HealthChecker.
type healthCheckStore struct {
	config.Store
	healthErr error
}

func (s *healthCheckStore) Health(ctx context.Context) error {
	return s.healthErr
}
