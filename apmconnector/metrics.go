package apmconnector

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type MeterProvider struct {
	Metrics pmetric.Metrics
}

func NewMeterProvider() *MeterProvider {
	return &MeterProvider{Metrics: pmetric.NewMetrics()}
}

func (meterProvider *MeterProvider) RecordHistogramFromSpan(metricName string, resourceAttributes, attributes pcommon.Map,
	span ptrace.Span) pmetric.HistogramDataPoint {
	return meterProvider.RecordHistogram(metricName, resourceAttributes, attributes, span.StartTimestamp(), span.EndTimestamp(), (span.EndTimestamp() - span.StartTimestamp()).AsTime().UnixNano())
}

func (meterProvider *MeterProvider) RecordHistogram(metricName string, resourceAttributes, attributes pcommon.Map,
	startTimestamp, endTimestamp pcommon.Timestamp, durationNanos int64) pmetric.HistogramDataPoint {
	resourceMetrics := meterProvider.Metrics.ResourceMetrics().AppendEmpty()

	resourceAttributes.CopyTo(resourceMetrics.Resource().Attributes())

	scopeMetric := resourceMetrics.ScopeMetrics().AppendEmpty()

	metric := scopeMetric.Metrics().AppendEmpty()
	metric.SetName(metricName)
	metric.SetUnit("s")

	histogram := metric.SetEmptyHistogram()
	histogram.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	dp := histogram.DataPoints().AppendEmpty()
	dp.SetStartTimestamp(startTimestamp)
	dp.SetTimestamp(endTimestamp)
	attributes.CopyTo(dp.Attributes())

	duration := NanosToSeconds(durationNanos)
	dp.SetSum(duration)
	dp.SetCount(1)
	dp.SetMin(duration)
	dp.SetMax(duration)
	return dp
}

func (meterProvider *MeterProvider) IncrementSum(metricName string, resourceAttributes, attributes pcommon.Map,
	timestamp pcommon.Timestamp) pmetric.NumberDataPoint {
	resourceMetrics := meterProvider.Metrics.ResourceMetrics().AppendEmpty()

	resourceAttributes.CopyTo(resourceMetrics.Resource().Attributes())

	scopeMetric := resourceMetrics.ScopeMetrics().AppendEmpty()

	metric := scopeMetric.Metrics().AppendEmpty()
	metric.SetName(metricName)
	sum := metric.SetEmptySum()
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	sum.SetIsMonotonic(false)
	dp := sum.DataPoints().AppendEmpty()
	attributes.CopyTo(dp.Attributes())

	dp.SetTimestamp(timestamp)

	dp.SetIntValue(1)
	return dp
}

func NanosToSeconds(nanos int64) float64 {
	return float64(nanos) / 1e9
}
