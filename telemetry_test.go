package wal

import (
	"context"
	"fmt"
	"os"
	"testing"

	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.uber.org/zap"
)

func TestWALOpenTelemetryTracing(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal-test-otel-trace-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	spanRecorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder))

	opts := DefaultOptions()
	opts.Logger = zap.NewNop()
	opts.TracerProvider = tp
	opts.MaxSegmentSize = 64 // Force rotation quickly

	ctx := context.Background()

	// 1. OpenContext
	w, err := OpenContext(ctx, dir, opts)
	if err != nil {
		t.Fatalf("OpenContext failed: %v", err)
	}

	// 2. AppendContext (enough to rotate)
	for i := range 5 {
		_, err := w.AppendContext(ctx, []byte(fmt.Sprintf("trace-data-%d", i)))
		if err != nil {
			t.Fatalf("AppendContext %d failed: %v", i, err)
		}
	}

	// 3. ReadContext
	data, err := w.ReadContext(ctx, 0)
	if err != nil && err != ErrNotFound {
		t.Fatalf("ReadContext 0 failed: %v", err)
	}
	if err == nil && len(data) == 0 {
		t.Fatalf("Expected non-empty data from ReadContext")
	}

	// 4. ReadContext for non-existent sequence
	_, err = w.ReadContext(ctx, 99999)
	if err != ErrNotFound {
		t.Fatalf("Expected ErrNotFound, got %v", err)
	}

	// 5. SyncContext
	err = w.SyncContext(ctx)
	if err != nil {
		t.Fatalf("SyncContext failed: %v", err)
	}

	// 6. CloseContext
	err = w.CloseContext(ctx)
	if err != nil {
		t.Fatalf("CloseContext failed: %v", err)
	}

	spans := spanRecorder.Ended()
	if len(spans) == 0 {
		t.Fatal("Expected recorded spans, got 0")
	}

	spanNames := make(map[string]int)
	var foundRotationEvent bool
	var foundNotFoundErrorSpan bool

	for _, s := range spans {
		spanNames[s.Name()]++

		for _, ev := range s.Events() {
			if ev.Name == "wal.segment_rotation" {
				foundRotationEvent = true
			}
		}

		if s.Name() == "wal.read" && s.Status().Code == codes.Error {
			foundNotFoundErrorSpan = true
		}
	}

	expectedSpans := []string{
		"wal.open",
		"wal.append",
		"wal.read",
		"wal.sync",
		"wal.close",
	}

	for _, name := range expectedSpans {
		if spanNames[name] == 0 {
			t.Errorf("Expected span %q was not recorded", name)
		}
	}

	if !foundRotationEvent {
		t.Errorf("Expected 'wal.segment_rotation' span event to be recorded during rapid append")
	}

	if !foundNotFoundErrorSpan {
		t.Errorf("Expected span error for non-existent read")
	}
}

func TestWALOpenTelemetryMetrics(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal-test-otel-metrics-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	metricReader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader))

	opts := DefaultOptions()
	opts.Logger = zap.NewNop()
	opts.MeterProvider = mp
	opts.MaxSegmentSize = 64 // Trigger rotations

	ctx := context.Background()

	w, err := OpenContext(ctx, dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	for i := range 5 {
		_, err := w.Append([]byte(fmt.Sprintf("metric-data-%d", i)))
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	_, _ = w.Read(0)
	_, _ = w.Read(99999) // not found
	_ = w.Sync()

	// Collect metrics before closing WAL to observe gauge values
	var rm metricdata.ResourceMetrics
	if err := metricReader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Failed to collect metrics: %v", err)
	}

	err = w.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	metricNames := make(map[string]bool)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			metricNames[m.Name] = true
		}
	}

	expectedMetrics := []string{
		"wal.appends",
		"wal.append.bytes",
		"wal.append.duration",
		"wal.reads",
		"wal.read.bytes",
		"wal.read.duration",
		"wal.rotations",
		"wal.syncs",
		"wal.sync.duration",
		"wal.active_size",
		"wal.segments",
	}

	for _, name := range expectedMetrics {
		if !metricNames[name] {
			t.Errorf("Expected metric %q was not found in collected metrics", name)
		}
	}
}

func TestWALOpenTelemetryContextPropagation(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal-test-otel-prop-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	spanRecorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder))

	opts := DefaultOptions()
	opts.Logger = zap.NewNop()
	opts.TracerProvider = tp

	tracer := tp.Tracer("test-parent")
	ctx, parentSpan := tracer.Start(context.Background(), "parent-span")

	w, err := OpenContext(ctx, dir, opts)
	if err != nil {
		t.Fatalf("OpenContext failed: %v", err)
	}

	seq, err := w.AppendContext(ctx, []byte("propagated-payload"))
	if err != nil {
		t.Fatalf("AppendContext failed: %v", err)
	}

	_, err = w.ReadContext(ctx, seq)
	if err != nil {
		t.Fatalf("ReadContext failed: %v", err)
	}

	_ = w.CloseContext(ctx)
	parentSpan.End()

	parentTraceID := parentSpan.SpanContext().TraceID()
	parentSpanID := parentSpan.SpanContext().SpanID()

	spans := spanRecorder.Ended()
	var walAppendSpan sdktrace.ReadOnlySpan
	for _, s := range spans {
		if s.Name() == "wal.append" {
			walAppendSpan = s
			break
		}
	}

	if walAppendSpan == nil {
		t.Fatal("Expected wal.append span, not found")
	}

	if walAppendSpan.SpanContext().TraceID() != parentTraceID {
		t.Errorf("Expected TraceID %s, got %s", parentTraceID, walAppendSpan.SpanContext().TraceID())
	}

	if walAppendSpan.Parent().SpanID() != parentSpanID {
		t.Errorf("Expected ParentSpanID %s, got %s", parentSpanID, walAppendSpan.Parent().SpanID())
	}
}
