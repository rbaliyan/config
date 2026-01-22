package otel

import (
	"go.opentelemetry.io/otel/metric"
)

// Metrics holds all OTEL metrics for the store.
type Metrics struct {
	// Counters
	OperationCount metric.Int64Counter
	ErrorCount     metric.Int64Counter

	// Histograms
	OperationLatency metric.Float64Histogram

	// Cache metrics
	CacheHits   metric.Int64Counter
	CacheMisses metric.Int64Counter
}

// initMetrics initializes all metrics
func initMetrics(meter metric.Meter) (*Metrics, error) {
	m := &Metrics{}
	var err error

	m.OperationCount, err = meter.Int64Counter(
		"config.operations.total",
		metric.WithDescription("Total number of config operations"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	m.ErrorCount, err = meter.Int64Counter(
		"config.errors.total",
		metric.WithDescription("Total number of config operation errors"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	m.OperationLatency, err = meter.Float64Histogram(
		"config.operation.duration",
		metric.WithDescription("Duration of config operations in seconds"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	m.CacheHits, err = meter.Int64Counter(
		"config.cache.hits",
		metric.WithDescription("Number of cache hits"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	m.CacheMisses, err = meter.Int64Counter(
		"config.cache.misses",
		metric.WithDescription("Number of cache misses"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// RecordCacheHit records a cache hit
func (m *Metrics) RecordCacheHit(ctx interface{ Context() interface{} }, namespace string) {
	// This would be called from an instrumented cache wrapper
}

// RecordCacheMiss records a cache miss
func (m *Metrics) RecordCacheMiss(ctx interface{ Context() interface{} }, namespace string) {
	// This would be called from an instrumented cache wrapper
}
