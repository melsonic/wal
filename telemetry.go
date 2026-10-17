package wal

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	noopmetric "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	nooptrace "go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
)

const (
	instrumentationScope = "melsonic/wal"

	// Span & metric attribute keys
	attrWalPath         = "wal.path"
	attrWalSeq          = "wal.seq"
	attrWalPayloadBytes = "wal.payload_bytes"
	attrWalRecordBytes  = "wal.record_bytes"
	attrWalSlotID       = "wal.slot_id"
	attrWalActiveSize   = "wal.active_size"
	attrWalSyncOnWrite  = "wal.sync_on_write"
	attrWalStatus       = "wal.status"
	attrWalNextSeq      = "wal.next_seq"
	attrWalSegments     = "wal.segments_count"

	// Status attribute values
	statusOK       = "ok"
	statusError    = "error"
	statusNotFound = "not_found"
)

type walTelemetry struct {
	tracer trace.Tracer
	meter  metric.Meter

	// Metrics
	appendsCounter     metric.Int64Counter
	appendBytesCounter metric.Int64Counter
	appendDuration     metric.Float64Histogram

	readsCounter     metric.Int64Counter
	readBytesCounter metric.Int64Counter
	readDuration     metric.Float64Histogram

	rotationsCounter metric.Int64Counter

	syncsCounter metric.Int64Counter
	syncDuration metric.Float64Histogram

	activeSizeGauge metric.Int64ObservableGauge
	segmentsGauge   metric.Int64ObservableGauge

	regCallback metric.Registration
}

func newWALTelemetry(w *WAL, tp trace.TracerProvider, mp metric.MeterProvider, logger *zap.Logger) *walTelemetry {
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	if tp == nil {
		tp = nooptrace.NewTracerProvider()
	}

	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	if mp == nil {
		mp = noopmetric.NewMeterProvider()
	}

	tracer := tp.Tracer(instrumentationScope)
	meter := mp.Meter(instrumentationScope)

	t := &walTelemetry{
		tracer: tracer,
		meter:  meter,
	}

	var err error
	t.appendsCounter, err = meter.Int64Counter(
		"wal.appends",
		metric.WithDescription("Total number of append operations"),
		metric.WithUnit("{operations}"),
	)
	if err != nil && logger != nil {
		logger.Warn("failed to initialize wal.appends metric", zap.Error(err))
	}

	t.appendBytesCounter, err = meter.Int64Counter(
		"wal.append.bytes",
		metric.WithDescription("Total payload bytes written to the log"),
		metric.WithUnit("By"),
	)
	if err != nil && logger != nil {
		logger.Warn("failed to initialize wal.append.bytes metric", zap.Error(err))
	}

	t.appendDuration, err = meter.Float64Histogram(
		"wal.append.duration",
		metric.WithDescription("Duration of append operations in seconds"),
		metric.WithUnit("s"),
	)
	if err != nil && logger != nil {
		logger.Warn("failed to initialize wal.append.duration metric", zap.Error(err))
	}

	t.readsCounter, err = meter.Int64Counter(
		"wal.reads",
		metric.WithDescription("Total number of read operations"),
		metric.WithUnit("{operations}"),
	)
	if err != nil && logger != nil {
		logger.Warn("failed to initialize wal.reads metric", zap.Error(err))
	}

	t.readBytesCounter, err = meter.Int64Counter(
		"wal.read.bytes",
		metric.WithDescription("Total payload bytes read from the log"),
		metric.WithUnit("By"),
	)
	if err != nil && logger != nil {
		logger.Warn("failed to initialize wal.read.bytes metric", zap.Error(err))
	}

	t.readDuration, err = meter.Float64Histogram(
		"wal.read.duration",
		metric.WithDescription("Duration of read operations in seconds"),
		metric.WithUnit("s"),
	)
	if err != nil && logger != nil {
		logger.Warn("failed to initialize wal.read.duration metric", zap.Error(err))
	}

	t.rotationsCounter, err = meter.Int64Counter(
		"wal.rotations",
		metric.WithDescription("Total number of segment rotations"),
		metric.WithUnit("{rotations}"),
	)
	if err != nil && logger != nil {
		logger.Warn("failed to initialize wal.rotations metric", zap.Error(err))
	}

	t.syncsCounter, err = meter.Int64Counter(
		"wal.syncs",
		metric.WithDescription("Total number of sync operations"),
		metric.WithUnit("{operations}"),
	)
	if err != nil && logger != nil {
		logger.Warn("failed to initialize wal.syncs metric", zap.Error(err))
	}

	t.syncDuration, err = meter.Float64Histogram(
		"wal.sync.duration",
		metric.WithDescription("Duration of sync operations in seconds"),
		metric.WithUnit("s"),
	)
	if err != nil && logger != nil {
		logger.Warn("failed to initialize wal.sync.duration metric", zap.Error(err))
	}

	t.activeSizeGauge, err = meter.Int64ObservableGauge(
		"wal.active_size",
		metric.WithDescription("Current size in bytes of the active segment file"),
		metric.WithUnit("By"),
	)
	if err != nil && logger != nil {
		logger.Warn("failed to initialize wal.active_size metric", zap.Error(err))
	}

	t.segmentsGauge, err = meter.Int64ObservableGauge(
		"wal.segments",
		metric.WithDescription("Total number of segments (active and sealed) managed by WAL"),
		metric.WithUnit("{segments}"),
	)
	if err != nil && logger != nil {
		logger.Warn("failed to initialize wal.segments metric", zap.Error(err))
	}

	if t.activeSizeGauge != nil && t.segmentsGauge != nil {
		reg, err := meter.RegisterCallback(
			func(ctx context.Context, obs metric.Observer) error {
				w.mu.RLock()
				activeSize := int64(w.activeSize)
				segCount := int64(len(w.oldSegments) + 1)
				walPath := w.path
				w.mu.RUnlock()

				pathAttr := metric.WithAttributes(attribute.String(attrWalPath, walPath))
				obs.ObserveInt64(t.activeSizeGauge, activeSize, pathAttr)
				obs.ObserveInt64(t.segmentsGauge, segCount, pathAttr)
				return nil
			},
			t.activeSizeGauge,
			t.segmentsGauge,
		)
		if err != nil {
			if logger != nil {
				logger.Warn("failed to register observable gauge callbacks", zap.Error(err))
			}
		} else {
			t.regCallback = reg
		}
	}

	return t
}

func (t *walTelemetry) Close() error {
	if t == nil {
		return nil
	}
	if t.regCallback != nil {
		return t.regCallback.Unregister()
	}
	return nil
}
