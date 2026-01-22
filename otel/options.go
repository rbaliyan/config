package otel

import (
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type options struct {
	tracerName    string
	meterName     string
	serviceName   string
	backendName   string
	enableTraces  bool
	enableMetrics bool
	tracer        trace.Tracer
	meter         metric.Meter
}

func defaultOptions() options {
	return options{
		tracerName:    "github.com/rbaliyan/config",
		meterName:     "github.com/rbaliyan/config",
		serviceName:   "",
		backendName:   "unknown",
		enableTraces:  false, // Disabled by default, opt-in via WithTracesEnabled(true)
		enableMetrics: false, // Disabled by default, opt-in via WithMetricsEnabled(true)
	}
}

// Option configures the instrumented store.
type Option func(*options)

// WithTracerName sets the tracer name.
func WithTracerName(name string) Option {
	return func(o *options) {
		o.tracerName = name
	}
}

// WithMeterName sets the meter name.
func WithMeterName(name string) Option {
	return func(o *options) {
		o.meterName = name
	}
}

// WithTracer sets a custom tracer.
func WithTracer(t trace.Tracer) Option {
	return func(o *options) {
		o.tracer = t
	}
}

// WithMeter sets a custom meter.
func WithMeter(m metric.Meter) Option {
	return func(o *options) {
		o.meter = m
	}
}

// WithServiceName sets the service name for attributes.
func WithServiceName(name string) Option {
	return func(o *options) {
		o.serviceName = name
	}
}

// WithBackendName sets the backend name for attributes.
// Examples: "memory", "mongodb", "postgres", "env", "file"
func WithBackendName(name string) Option {
	return func(o *options) {
		o.backendName = name
	}
}

// WithTracesEnabled enables/disables tracing.
func WithTracesEnabled(enabled bool) Option {
	return func(o *options) {
		o.enableTraces = enabled
	}
}

// WithMetricsEnabled enables/disables metrics.
func WithMetricsEnabled(enabled bool) Option {
	return func(o *options) {
		o.enableMetrics = enabled
	}
}
