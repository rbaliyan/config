package replica

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/memory"
)

// newTestTracer creates an in-memory span recorder and returns the recorder + tracer.
func newTestTracer() (*tracetest.SpanRecorder, trace.Tracer) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	return recorder, tp.Tracer("test")
}

// newTestMeter creates an in-memory metric reader and returns the reader + meter.
func newTestMeter() (*sdkmetric.ManualReader, metric.Meter) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	return reader, mp.Meter("test")
}

// collectMetricNames returns all metric names from the reader's current snapshot.
func collectMetricNames(t *testing.T, reader *sdkmetric.ManualReader) map[string]bool {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	names := make(map[string]bool)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			names[m.Name] = true
		}
	}
	return names
}

// findSpans returns all recorded spans with the given name.
func findSpans(recorder *tracetest.SpanRecorder, name string) []sdktrace.ReadOnlySpan {
	var found []sdktrace.ReadOnlySpan
	for _, s := range recorder.Ended() {
		if s.Name() == name {
			found = append(found, s)
		}
	}
	return found
}

// spanAttrValue returns the string value of a named attribute from a span, or "".
func spanAttrValue(span sdktrace.ReadOnlySpan, key string) string {
	for _, kv := range span.Attributes() {
		if string(kv.Key) == key {
			return kv.Value.AsString()
		}
	}
	return ""
}

func TestOTel_Options(t *testing.T) {
	recorder, tracer := newTestTracer()
	reader, meter := newTestMeter()
	_ = recorder
	_ = reader

	primary := memory.NewStore()
	s := NewStore(primary, nil,
		WithTracesEnabled(true),
		WithMetricsEnabled(true),
		WithTracerName("my-tracer"),
		WithMeterName("my-meter"),
		WithBackendName("mongo->postgres"),
		WithTracer(tracer),
		WithMeter(meter),
	)

	if !s.opts.enableTraces {
		t.Error("Expected enableTraces=true")
	}
	if !s.opts.enableMetrics {
		t.Error("Expected enableMetrics=true")
	}
	if s.opts.tracerName != "my-tracer" {
		t.Errorf("Expected tracerName 'my-tracer', got %s", s.opts.tracerName)
	}
	if s.opts.meterName != "my-meter" {
		t.Errorf("Expected meterName 'my-meter', got %s", s.opts.meterName)
	}
	if s.opts.backendName != "mongo->postgres" {
		t.Errorf("Expected backendName 'mongo->postgres', got %s", s.opts.backendName)
	}
}

func TestOTel_Connect_InitializesTracer(t *testing.T) {
	recorder, tracer := newTestTracer()

	primary := memory.NewStore()
	s := NewStore(primary, nil,
		WithTracesEnabled(true),
		WithTracer(tracer),
	)

	ctx := context.Background()
	if err := s.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer s.Close(ctx)

	if s.tracer == nil {
		t.Error("Expected tracer to be initialized")
	}

	spans := findSpans(recorder, "config.replica.Connect")
	if len(spans) == 0 {
		t.Error("Expected Connect span to be recorded")
	}
}

func TestOTel_Connect_InitializesMetrics(t *testing.T) {
	reader, meter := newTestMeter()

	primary := memory.NewStore()
	s := NewStore(primary, nil,
		WithMetricsEnabled(true),
		WithMeter(meter),
	)

	ctx := context.Background()
	if err := s.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer s.Close(ctx)

	if s.metrics == nil {
		t.Error("Expected metrics to be initialized")
	}

	names := collectMetricNames(t, reader)
	if !names["config.replica.operations.total"] {
		t.Error("Expected config.replica.operations.total metric")
	}
}

func TestOTel_Get_EmitsSpan(t *testing.T) {
	recorder, tracer := newTestTracer()

	primary := memory.NewStore()
	s := NewStore(primary, nil,
		WithTracesEnabled(true),
		WithTracer(tracer),
	)

	ctx := context.Background()
	_ = s.Connect(ctx)
	defer s.Close(ctx)

	_, _ = s.Set(ctx, "ns", "key", config.NewValue("val"))
	_, _ = s.Get(ctx, "ns", "key")

	spans := findSpans(recorder, "config.replica.Get")
	if len(spans) == 0 {
		t.Fatal("Expected config.replica.Get span")
	}

	span := spans[0]
	if got := spanAttrValue(span, "config.namespace"); got != "ns" {
		t.Errorf("Expected namespace 'ns', got %q", got)
	}
	if got := spanAttrValue(span, "config.key"); got != "key" {
		t.Errorf("Expected key 'key', got %q", got)
	}
	if got := spanAttrValue(span, "config.backend"); got != "replica" {
		t.Errorf("Expected backend 'replica', got %q", got)
	}
}

func TestOTel_Set_EmitsSpan(t *testing.T) {
	recorder, tracer := newTestTracer()

	primary := memory.NewStore()
	s := NewStore(primary, nil,
		WithTracesEnabled(true),
		WithTracer(tracer),
	)

	ctx := context.Background()
	_ = s.Connect(ctx)
	defer s.Close(ctx)

	_, err := s.Set(ctx, "ns", "key", config.NewValue(42))
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	spans := findSpans(recorder, "config.replica.Set")
	if len(spans) == 0 {
		t.Fatal("Expected config.replica.Set span")
	}
	if got := spanAttrValue(spans[0], "config.replica.mode"); got != "async" {
		t.Errorf("Expected mode 'async', got %q", got)
	}
}

func TestOTel_Delete_EmitsSpan(t *testing.T) {
	recorder, tracer := newTestTracer()

	primary := memory.NewStore()
	s := NewStore(primary, nil,
		WithTracesEnabled(true),
		WithTracer(tracer),
	)

	ctx := context.Background()
	_ = s.Connect(ctx)
	defer s.Close(ctx)

	_, _ = s.Set(ctx, "ns", "key", config.NewValue("v"))
	_ = s.Delete(ctx, "ns", "key")

	spans := findSpans(recorder, "config.replica.Delete")
	if len(spans) == 0 {
		t.Fatal("Expected config.replica.Delete span")
	}
	if got := spanAttrValue(spans[0], "config.key"); got != "key" {
		t.Errorf("Expected key 'key', got %q", got)
	}
}

func TestOTel_Find_EmitsSpan(t *testing.T) {
	recorder, tracer := newTestTracer()

	primary := memory.NewStore()
	s := NewStore(primary, nil,
		WithTracesEnabled(true),
		WithTracer(tracer),
	)

	ctx := context.Background()
	_ = s.Connect(ctx)
	defer s.Close(ctx)

	_, _ = s.Find(ctx, "ns", config.NewFilter().WithPrefix("app/").Build())

	spans := findSpans(recorder, "config.replica.Find")
	if len(spans) == 0 {
		t.Fatal("Expected config.replica.Find span")
	}
	if got := spanAttrValue(spans[0], "config.prefix"); got != "app/" {
		t.Errorf("Expected prefix 'app/', got %q", got)
	}
}

func TestOTel_Watch_EmitsSpan(t *testing.T) {
	recorder, tracer := newTestTracer()

	primary := memory.NewStore()
	s := NewStore(primary, nil,
		WithTracesEnabled(true),
		WithTracer(tracer),
	)

	ctx := context.Background()
	_ = s.Connect(ctx)
	defer s.Close(ctx)

	_, _ = s.Watch(ctx, config.WatchFilter{})

	spans := findSpans(recorder, "config.replica.Watch")
	if len(spans) == 0 {
		t.Fatal("Expected config.replica.Watch span")
	}
}

func TestOTel_Metrics_OperationCountIncremented(t *testing.T) {
	reader, meter := newTestMeter()

	primary := memory.NewStore()
	s := NewStore(primary, nil,
		WithMetricsEnabled(true),
		WithMeter(meter),
	)

	ctx := context.Background()
	_ = s.Connect(ctx)
	defer s.Close(ctx)

	_, _ = s.Set(ctx, "ns", "key", config.NewValue("v"))
	_, _ = s.Get(ctx, "ns", "key")
	_ = s.Delete(ctx, "ns", "key")

	names := collectMetricNames(t, reader)
	for _, name := range []string{
		"config.replica.operations.total",
		"config.replica.operation.duration",
	} {
		if !names[name] {
			t.Errorf("Expected metric %q to be recorded", name)
		}
	}
}

func TestOTel_Metrics_ErrorCountIncremented(t *testing.T) {
	reader, meter := newTestMeter()

	primary := memory.NewStore()
	s := NewStore(primary, nil,
		WithMetricsEnabled(true),
		WithMeter(meter),
	)

	ctx := context.Background()
	_ = s.Connect(ctx)
	defer s.Close(ctx)

	_, _ = s.Get(ctx, "ns", "missing-key")

	names := collectMetricNames(t, reader)
	if !names["config.replica.errors.total"] {
		t.Error("Expected config.replica.errors.total to be recorded on NotFound error")
	}
}

func TestOTel_Metrics_AsyncReplication(t *testing.T) {
	reader, meter := newTestMeter()

	primary := memory.NewStore()
	secondary := memory.NewStore()
	s := NewStore(primary, []config.Store{secondary},
		WithReplicationMode(ModeAsync),
		WithMetricsEnabled(true),
		WithMeter(meter),
	)

	ctx := context.Background()
	_ = s.Connect(ctx)
	defer s.Close(ctx)

	_, _ = s.Set(ctx, "ns", "key", config.NewValue("v"))

	// Wait for async replication to complete
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, err := secondary.Get(ctx, "ns", "key"); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	names := collectMetricNames(t, reader)
	if !names["config.replica.replication.events.total"] {
		t.Error("Expected config.replica.replication.events.total to be recorded")
	}
	if !names["config.replica.replication.lag"] {
		t.Error("Expected config.replica.replication.lag to be recorded")
	}
}

func TestOTel_Metrics_InitialSync(t *testing.T) {
	reader, meter := newTestMeter()

	primary := memory.NewStore()
	secondary := memory.NewStore()

	ctx := context.Background()
	_ = primary.Connect(ctx)
	_, _ = primary.Set(ctx, "ns", "k1", config.NewValue("v1"))

	s := NewStore(primary, []config.Store{secondary},
		WithReplicationMode(ModeSync),
		WithInitialSync("ns"),
		WithMetricsEnabled(true),
		WithMeter(meter),
	)

	if err := s.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer s.Close(ctx)

	names := collectMetricNames(t, reader)
	if !names["config.replica.sync.duration"] {
		t.Error("Expected config.replica.sync.duration to be recorded after initial sync")
	}
}

func TestOTel_InitialSync_EmitsSpan(t *testing.T) {
	recorder, tracer := newTestTracer()

	primary := memory.NewStore()
	secondary := memory.NewStore()

	ctx := context.Background()
	_ = primary.Connect(ctx)
	_, _ = primary.Set(ctx, "ns", "k1", config.NewValue("v1"))

	s := NewStore(primary, []config.Store{secondary},
		WithReplicationMode(ModeSync),
		WithInitialSync("ns"),
		WithTracesEnabled(true),
		WithTracer(tracer),
	)

	if err := s.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer s.Close(ctx)

	spans := findSpans(recorder, "config.replica.InitialSync")
	if len(spans) == 0 {
		t.Error("Expected config.replica.InitialSync span")
	}
}

func TestOTel_BackendName_AppearsInSpan(t *testing.T) {
	recorder, tracer := newTestTracer()

	primary := memory.NewStore()
	s := NewStore(primary, nil,
		WithTracesEnabled(true),
		WithTracer(tracer),
		WithBackendName("mongo->postgres"),
	)

	ctx := context.Background()
	_ = s.Connect(ctx)
	defer s.Close(ctx)

	_, _ = s.Get(ctx, "ns", "key")

	spans := findSpans(recorder, "config.replica.Get")
	if len(spans) == 0 {
		t.Fatal("Expected config.replica.Get span")
	}
	if got := spanAttrValue(spans[0], "config.replica.backend"); got != "mongo->postgres" {
		t.Errorf("Expected backend name 'mongo->postgres', got %q", got)
	}
}

func TestOTel_ModeAttr_SyncAppearsInSpan(t *testing.T) {
	recorder, tracer := newTestTracer()

	primary := memory.NewStore()
	s := NewStore(primary, nil,
		WithReplicationMode(ModeSync),
		WithTracesEnabled(true),
		WithTracer(tracer),
	)

	ctx := context.Background()
	_ = s.Connect(ctx)
	defer s.Close(ctx)

	_, _ = s.Set(ctx, "ns", "key", config.NewValue("v"))

	spans := findSpans(recorder, "config.replica.Set")
	if len(spans) == 0 {
		t.Fatal("Expected config.replica.Set span")
	}
	if got := spanAttrValue(spans[0], "config.replica.mode"); got != "sync" {
		t.Errorf("Expected mode 'sync', got %q", got)
	}
}

func TestOTel_ReadPreferenceAttr_AppearsInSpan(t *testing.T) {
	recorder, tracer := newTestTracer()

	primary := memory.NewStore()
	secondary := memory.NewStore()
	s := NewStore(primary, []config.Store{secondary},
		WithReadPreference(ReadSecondary),
		WithTracesEnabled(true),
		WithTracer(tracer),
	)

	ctx := context.Background()
	_ = s.Connect(ctx)
	defer s.Close(ctx)

	_, _ = s.Get(ctx, "ns", "key")

	spans := findSpans(recorder, "config.replica.Get")
	if len(spans) == 0 {
		t.Fatal("Expected config.replica.Get span")
	}
	if got := spanAttrValue(spans[0], "config.replica.read_preference"); got != "secondary" {
		t.Errorf("Expected read_preference 'secondary', got %q", got)
	}
}

func TestOTel_NoTraces_NoMetrics_NoOp(t *testing.T) {
	primary := memory.NewStore()
	s := NewStore(primary, nil)

	ctx := context.Background()
	if err := s.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer s.Close(ctx)

	if s.tracer != nil {
		t.Error("Expected nil tracer when tracing disabled")
	}
	if s.metrics != nil {
		t.Error("Expected nil metrics when metrics disabled")
	}
}

func TestOTel_GetMany_EmitsSpan(t *testing.T) {
	recorder, tracer := newTestTracer()

	primary := memory.NewStore()
	s := NewStore(primary, nil,
		WithTracesEnabled(true),
		WithTracer(tracer),
	)

	ctx := context.Background()
	_ = s.Connect(ctx)
	defer s.Close(ctx)

	_, _ = s.GetMany(ctx, "ns", []string{"k1", "k2"})

	spans := findSpans(recorder, "config.replica.GetMany")
	if len(spans) == 0 {
		t.Fatal("Expected config.replica.GetMany span")
	}

	var keyCount int64
	for _, kv := range spans[0].Attributes() {
		if kv.Key == attribute.Key("config.key_count") {
			keyCount = kv.Value.AsInt64()
		}
	}
	if keyCount != 2 {
		t.Errorf("Expected key_count=2, got %d", keyCount)
	}
}

func TestOTel_SetMany_EmitsSpan(t *testing.T) {
	recorder, tracer := newTestTracer()

	primary := memory.NewStore()
	s := NewStore(primary, nil,
		WithTracesEnabled(true),
		WithTracer(tracer),
	)

	ctx := context.Background()
	_ = s.Connect(ctx)
	defer s.Close(ctx)

	vals := map[string]config.Value{"k1": config.NewValue("v1"), "k2": config.NewValue("v2")}
	_ = s.SetMany(ctx, "ns", vals)

	spans := findSpans(recorder, "config.replica.SetMany")
	if len(spans) == 0 {
		t.Fatal("Expected config.replica.SetMany span")
	}
}

func TestOTel_DeleteMany_EmitsSpan(t *testing.T) {
	recorder, tracer := newTestTracer()

	primary := memory.NewStore()
	s := NewStore(primary, nil,
		WithTracesEnabled(true),
		WithTracer(tracer),
	)

	ctx := context.Background()
	_ = s.Connect(ctx)
	defer s.Close(ctx)

	_, _ = s.Set(ctx, "ns", "k1", config.NewValue("v1"))
	_, _ = s.DeleteMany(ctx, "ns", []string{"k1"})

	spans := findSpans(recorder, "config.replica.DeleteMany")
	if len(spans) == 0 {
		t.Fatal("Expected config.replica.DeleteMany span")
	}
}

func TestOTel_Close_EmitsSpan(t *testing.T) {
	recorder, tracer := newTestTracer()

	primary := memory.NewStore()
	s := NewStore(primary, nil,
		WithTracesEnabled(true),
		WithTracer(tracer),
	)

	ctx := context.Background()
	_ = s.Connect(ctx)
	_ = s.Close(ctx)

	spans := findSpans(recorder, "config.replica.Close")
	if len(spans) == 0 {
		t.Fatal("Expected config.replica.Close span")
	}
}
