package replica

import (
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// WithTracesEnabled enables or disables span tracing (default: disabled).
// When enabled, every store operation emits a span to the configured tracer.
func WithTracesEnabled(enabled bool) Option {
	return func(o *options) { o.enableTraces = enabled }
}

// WithMetricsEnabled enables or disables metrics (default: disabled).
// When enabled, operations are recorded via the configured meter. Metric
// initialization errors are returned from Connect.
func WithMetricsEnabled(enabled bool) Option {
	return func(o *options) { o.enableMetrics = enabled }
}

// WithTracerName sets the tracer name used when no custom tracer is injected
// (default: "github.com/rbaliyan/config/replica").
func WithTracerName(name string) Option {
	return func(o *options) { o.tracerName = name }
}

// WithMeterName sets the meter name used when no custom meter is injected
// (default: "github.com/rbaliyan/config/replica").
func WithMeterName(name string) Option {
	return func(o *options) { o.meterName = name }
}

// WithBackendName sets a descriptive label for the replica topology, used as
// the "config.replica.backend" span attribute. Example: "mongo->postgres".
func WithBackendName(name string) Option {
	return func(o *options) { o.backendName = name }
}

// WithTracer injects a custom tracer, bypassing the global provider.
func WithTracer(t trace.Tracer) Option {
	return func(o *options) { o.tracer = t }
}

// WithMeter injects a custom meter, bypassing the global provider.
func WithMeter(m metric.Meter) Option {
	return func(o *options) { o.meter = m }
}

// replicaMetrics holds all OTel metric instruments for the replica store.
type replicaMetrics struct {
	// Standard store operation metrics
	operationCount   metric.Int64Counter
	operationLatency metric.Float64Histogram
	errorCount       metric.Int64Counter

	// Async replication metrics (ModeAsync only)
	replicationEvents metric.Int64Counter    // events processed by the background goroutine
	replicationLag    metric.Float64Histogram // event.Timestamp → processing time
	replicationErrors metric.Int64Counter    // failures applying events to secondaries

	// Initial sync metric
	syncDuration metric.Float64Histogram
}

func initReplicaMetrics(meter metric.Meter) (*replicaMetrics, error) {
	m := &replicaMetrics{}
	var err error

	m.operationCount, err = meter.Int64Counter(
		"config.replica.operations.total",
		metric.WithDescription("Total number of replica store operations"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	m.operationLatency, err = meter.Float64Histogram(
		"config.replica.operation.duration",
		metric.WithDescription("Duration of replica store operations in seconds"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	m.errorCount, err = meter.Int64Counter(
		"config.replica.errors.total",
		metric.WithDescription("Total number of replica store operation errors"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	m.replicationEvents, err = meter.Int64Counter(
		"config.replica.replication.events.total",
		metric.WithDescription("Total change events processed by the async replication goroutine"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	m.replicationLag, err = meter.Float64Histogram(
		"config.replica.replication.lag",
		metric.WithDescription("Seconds between event creation on the primary and application to secondaries"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	m.replicationErrors, err = meter.Int64Counter(
		"config.replica.replication.errors.total",
		metric.WithDescription("Total errors applying replication events to secondaries"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	m.syncDuration, err = meter.Float64Histogram(
		"config.replica.sync.duration",
		metric.WithDescription("Duration of the initial data sync from primary to secondaries in seconds"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	return m, nil
}
