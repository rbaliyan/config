package otel

import (
	"go.opentelemetry.io/otel/metric"
)

// metrics holds all OTEL metrics for the store.
type metrics struct {
	operationCount   metric.Int64Counter
	errorCount       metric.Int64Counter
	operationLatency metric.Float64Histogram
}

// initMetrics initializes all metrics.
func initMetrics(meter metric.Meter) (*metrics, error) {
	m := &metrics{}
	var err error

	m.operationCount, err = meter.Int64Counter(
		"config.operations.total",
		metric.WithDescription("Total number of config operations"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	m.errorCount, err = meter.Int64Counter(
		"config.errors.total",
		metric.WithDescription("Total number of config operation errors"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	m.operationLatency, err = meter.Float64Histogram(
		"config.operation.duration",
		metric.WithDescription("Duration of config operations in seconds"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	return m, nil
}
