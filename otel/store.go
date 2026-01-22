// Package otel provides OpenTelemetry instrumentation for the config library.
package otel

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/rbaliyan/config"
)

// InstrumentedStore wraps a Store with OpenTelemetry tracing and metrics.
type InstrumentedStore struct {
	store   config.Store
	tracer  trace.Tracer
	meter   metric.Meter
	metrics *Metrics
	opts    options
}

// Compile-time interface checks
var (
	_ config.Store         = (*InstrumentedStore)(nil)
	_ config.HealthChecker = (*InstrumentedStore)(nil)
	_ config.StatsProvider = (*InstrumentedStore)(nil)
	_ config.BulkStore     = (*InstrumentedStore)(nil)
)

// WrapStore wraps a Store with OpenTelemetry instrumentation.
// By default, both tracing and metrics are disabled. Use WithTracesEnabled(true)
// and/or WithMetricsEnabled(true) to enable them.
func WrapStore(store config.Store, opts ...Option) (*InstrumentedStore, error) {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}

	is := &InstrumentedStore{
		store: store,
		opts:  o,
	}

	// Only initialize tracer if tracing is enabled
	if o.enableTraces {
		if o.tracer != nil {
			is.tracer = o.tracer
		} else {
			is.tracer = otel.Tracer(o.tracerName)
		}
	}

	// Only initialize meter and metrics if metrics are enabled
	if o.enableMetrics {
		var meter metric.Meter
		if o.meter != nil {
			meter = o.meter
		} else {
			meter = otel.Meter(o.meterName)
		}
		is.meter = meter

		metrics, err := initMetrics(meter)
		if err != nil {
			return nil, err
		}
		is.metrics = metrics
	}

	return is, nil
}

// Unwrap returns the underlying store.
func (s *InstrumentedStore) Unwrap() config.Store {
	return s.store
}

// Connect establishes connection to the storage backend.
func (s *InstrumentedStore) Connect(ctx context.Context) error {
	if !s.opts.enableTraces {
		return s.store.Connect(ctx)
	}

	ctx, span := s.tracer.Start(ctx, "config.Connect",
		trace.WithAttributes(s.commonAttributes()...))
	defer span.End()

	start := time.Now()
	err := s.store.Connect(ctx)
	s.recordOperation(ctx, "connect", "", "", start, err)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}

	return err
}

// Close releases resources and closes the connection.
func (s *InstrumentedStore) Close(ctx context.Context) error {
	if !s.opts.enableTraces {
		return s.store.Close(ctx)
	}

	ctx, span := s.tracer.Start(ctx, "config.Close",
		trace.WithAttributes(s.commonAttributes()...))
	defer span.End()

	start := time.Now()
	err := s.store.Close(ctx)
	s.recordOperation(ctx, "close", "", "", start, err)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}

	return err
}

// Get retrieves a configuration value by namespace, key, and optional tags.
func (s *InstrumentedStore) Get(ctx context.Context, namespace, key string, tags ...config.Tag) (config.Value, error) {
	if !s.opts.enableTraces {
		start := time.Now()
		value, err := s.store.Get(ctx, namespace, key, tags...)
		s.recordOperation(ctx, "get", namespace, key, start, err)
		return value, err
	}

	attrs := append(s.commonAttributes(),
		attribute.String("config.namespace", namespace),
		attribute.String("config.key", key),
	)
	if len(tags) > 0 {
		attrs = append(attrs, attribute.String("config.tags", config.FormatTags(tags)))
	}

	ctx, span := s.tracer.Start(ctx, "config.Get",
		trace.WithAttributes(attrs...))
	defer span.End()

	start := time.Now()
	value, err := s.store.Get(ctx, namespace, key, tags...)
	s.recordOperation(ctx, "get", namespace, key, start, err)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
		if value != nil {
			span.SetAttributes(
				attribute.Int64("config.version", value.Metadata().Version()),
				attribute.String("config.type", value.Type().String()),
			)
		}
	}

	return value, err
}

// Set creates or updates a configuration value.
func (s *InstrumentedStore) Set(ctx context.Context, namespace, key string, value config.Value) error {
	if !s.opts.enableTraces {
		start := time.Now()
		err := s.store.Set(ctx, namespace, key, value)
		s.recordOperation(ctx, "set", namespace, key, start, err)
		return err
	}

	ctx, span := s.tracer.Start(ctx, "config.Set",
		trace.WithAttributes(
			append(s.commonAttributes(),
				attribute.String("config.namespace", namespace),
				attribute.String("config.key", key),
				attribute.String("config.type", value.Type().String()),
			)...))
	defer span.End()

	start := time.Now()
	err := s.store.Set(ctx, namespace, key, value)
	s.recordOperation(ctx, "set", namespace, key, start, err)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}

	return err
}

// Delete removes a configuration value by namespace, key, and optional tags.
func (s *InstrumentedStore) Delete(ctx context.Context, namespace, key string, tags ...config.Tag) error {
	if !s.opts.enableTraces {
		start := time.Now()
		err := s.store.Delete(ctx, namespace, key, tags...)
		s.recordOperation(ctx, "delete", namespace, key, start, err)
		return err
	}

	attrs := append(s.commonAttributes(),
		attribute.String("config.namespace", namespace),
		attribute.String("config.key", key),
	)
	if len(tags) > 0 {
		attrs = append(attrs, attribute.String("config.tags", config.FormatTags(tags)))
	}

	ctx, span := s.tracer.Start(ctx, "config.Delete",
		trace.WithAttributes(attrs...))
	defer span.End()

	start := time.Now()
	err := s.store.Delete(ctx, namespace, key, tags...)
	s.recordOperation(ctx, "delete", namespace, key, start, err)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}

	return err
}

// Find returns all keys and values matching the filter within a namespace.
func (s *InstrumentedStore) Find(ctx context.Context, namespace string, filter config.Filter) (config.Page, error) {
	if !s.opts.enableTraces {
		start := time.Now()
		page, err := s.store.Find(ctx, namespace, filter)
		s.recordOperation(ctx, "find", namespace, filter.Prefix(), start, err)
		return page, err
	}

	ctx, span := s.tracer.Start(ctx, "config.Find",
		trace.WithAttributes(
			append(s.commonAttributes(),
				attribute.String("config.namespace", namespace),
				attribute.String("config.prefix", filter.Prefix()),
				attribute.Int("config.limit", filter.Limit()),
			)...))
	defer span.End()

	start := time.Now()
	page, err := s.store.Find(ctx, namespace, filter)
	s.recordOperation(ctx, "find", namespace, filter.Prefix(), start, err)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
		span.SetAttributes(
			attribute.Int("config.result_count", len(page.Results())),
			attribute.Int("config.limit", page.Limit()),
		)
	}

	return page, err
}

// Watch returns a channel that receives change events.
// This is an internal method used by the Manager for cache invalidation.
// Only available if the underlying store supports watching.
func (s *InstrumentedStore) Watch(ctx context.Context, filter config.WatchFilter) (<-chan config.ChangeEvent, error) {
	// Check if underlying store supports Watch
	ws, ok := s.store.(interface {
		Watch(ctx context.Context, filter config.WatchFilter) (<-chan config.ChangeEvent, error)
	})
	if !ok {
		return nil, config.ErrWatchNotSupported
	}

	if !s.opts.enableTraces {
		start := time.Now()
		ch, err := ws.Watch(ctx, filter)
		s.recordOperation(ctx, "watch", "", "", start, err)
		return ch, err
	}

	ctx, span := s.tracer.Start(ctx, "config.Watch",
		trace.WithAttributes(s.commonAttributes()...))
	defer span.End()

	start := time.Now()
	ch, err := ws.Watch(ctx, filter)
	s.recordOperation(ctx, "watch", "", "", start, err)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}

	return ch, err
}

// Health performs a health check on the store.
func (s *InstrumentedStore) Health(ctx context.Context) error {
	if checker, ok := s.store.(config.HealthChecker); ok {
		if !s.opts.enableTraces {
			return checker.Health(ctx)
		}

		ctx, span := s.tracer.Start(ctx, "config.Health",
			trace.WithAttributes(s.commonAttributes()...))
		defer span.End()

		err := checker.Health(ctx)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		} else {
			span.SetStatus(codes.Ok, "")
		}
		return err
	}
	return nil
}

// Stats returns store statistics.
func (s *InstrumentedStore) Stats(ctx context.Context) (*config.StoreStats, error) {
	if provider, ok := s.store.(config.StatsProvider); ok {
		return provider.Stats(ctx)
	}
	return nil, nil
}

// GetMany retrieves multiple values in a single operation.
func (s *InstrumentedStore) GetMany(ctx context.Context, namespace string, keys []string) (map[string]config.Value, error) {
	bulk, ok := s.store.(config.BulkStore)
	if !ok {
		// Fallback to individual gets
		results := make(map[string]config.Value, len(keys))
		for _, key := range keys {
			value, err := s.Get(ctx, namespace, key)
			if err == nil {
				results[key] = value
			}
		}
		return results, nil
	}

	if !s.opts.enableTraces {
		start := time.Now()
		results, err := bulk.GetMany(ctx, namespace, keys)
		s.recordOperation(ctx, "get_many", namespace, "", start, err)
		return results, err
	}

	ctx, span := s.tracer.Start(ctx, "config.GetMany",
		trace.WithAttributes(
			append(s.commonAttributes(),
				attribute.String("config.namespace", namespace),
				attribute.Int("config.key_count", len(keys)),
			)...))
	defer span.End()

	start := time.Now()
	results, err := bulk.GetMany(ctx, namespace, keys)
	s.recordOperation(ctx, "get_many", namespace, "", start, err)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
		span.SetAttributes(attribute.Int("config.result_count", len(results)))
	}

	return results, err
}

// SetMany creates or updates multiple values in a single operation.
func (s *InstrumentedStore) SetMany(ctx context.Context, namespace string, values map[string]config.Value) error {
	bulk, ok := s.store.(config.BulkStore)
	if !ok {
		// Fallback to individual sets
		for key, value := range values {
			if err := s.Set(ctx, namespace, key, value); err != nil {
				return err
			}
		}
		return nil
	}

	if !s.opts.enableTraces {
		start := time.Now()
		err := bulk.SetMany(ctx, namespace, values)
		s.recordOperation(ctx, "set_many", namespace, "", start, err)
		return err
	}

	ctx, span := s.tracer.Start(ctx, "config.SetMany",
		trace.WithAttributes(
			append(s.commonAttributes(),
				attribute.String("config.namespace", namespace),
				attribute.Int("config.entry_count", len(values)),
			)...))
	defer span.End()

	start := time.Now()
	err := bulk.SetMany(ctx, namespace, values)
	s.recordOperation(ctx, "set_many", namespace, "", start, err)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}

	return err
}

// DeleteMany removes multiple values in a single operation.
// Note: This method doesn't support tags in the fallback path.
func (s *InstrumentedStore) DeleteMany(ctx context.Context, namespace string, keys []string) (int64, error) {
	bulk, ok := s.store.(config.BulkStore)
	if !ok {
		// Fallback to individual deletes (without tags)
		var deleted int64
		for _, key := range keys {
			if err := s.Delete(ctx, namespace, key); err == nil {
				deleted++
			}
		}
		return deleted, nil
	}

	if !s.opts.enableTraces {
		start := time.Now()
		deleted, err := bulk.DeleteMany(ctx, namespace, keys)
		s.recordOperation(ctx, "delete_many", namespace, "", start, err)
		return deleted, err
	}

	ctx, span := s.tracer.Start(ctx, "config.DeleteMany",
		trace.WithAttributes(
			append(s.commonAttributes(),
				attribute.String("config.namespace", namespace),
				attribute.Int("config.key_count", len(keys)),
			)...))
	defer span.End()

	start := time.Now()
	deleted, err := bulk.DeleteMany(ctx, namespace, keys)
	s.recordOperation(ctx, "delete_many", namespace, "", start, err)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
		span.SetAttributes(attribute.Int64("config.deleted_count", deleted))
	}

	return deleted, err
}

// commonAttributes returns common span attributes
func (s *InstrumentedStore) commonAttributes() []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("config.backend", s.opts.backendName),
	}
	if s.opts.serviceName != "" {
		attrs = append(attrs, attribute.String("service.name", s.opts.serviceName))
	}
	return attrs
}

// recordOperation records metrics for an operation
func (s *InstrumentedStore) recordOperation(ctx context.Context, op, namespace, key string, start time.Time, err error) {
	if !s.opts.enableMetrics {
		return
	}

	latency := time.Since(start).Seconds()
	attrs := []attribute.KeyValue{
		attribute.String("operation", op),
	}
	if namespace != "" {
		attrs = append(attrs, attribute.String("namespace", namespace))
	}

	s.metrics.OperationCount.Add(ctx, 1, metric.WithAttributes(attrs...))
	s.metrics.OperationLatency.Record(ctx, latency, metric.WithAttributes(attrs...))

	if err != nil {
		errorAttrs := append(attrs, attribute.String("error_type", errorType(err)))
		s.metrics.ErrorCount.Add(ctx, 1, metric.WithAttributes(errorAttrs...))
	}
}

// errorType returns a string classification of the error
func errorType(err error) string {
	if config.IsNotFound(err) {
		return "not_found"
	}
	if config.IsTypeMismatch(err) {
		return "type_mismatch"
	}
	return "internal"
}
