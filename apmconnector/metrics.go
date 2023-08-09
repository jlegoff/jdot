package apmconnector

import (
	"crypto"
	"fmt"
	"sort"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type MeterProvider struct {
	Metrics         pmetric.Metrics
	resourceMetrics map[string]ResourceMetrics
}

type ResourceMetrics struct {
	metrics      pmetric.MetricSlice
	nameToMetric map[string]pmetric.Metric
}

func NewMeterProvider() *MeterProvider {
	return &MeterProvider{Metrics: pmetric.NewMetrics(), resourceMetrics: make(map[string]ResourceMetrics)}
}

func (meterProvider *MeterProvider) RecordHistogramFromSpan(metricName string, resourceAttributes, attributes pcommon.Map,
	span ptrace.Span) pmetric.HistogramDataPoint {
	return meterProvider.RecordHistogram(metricName, resourceAttributes, attributes, span.StartTimestamp(), span.EndTimestamp(), (span.EndTimestamp() - span.StartTimestamp()).AsTime().UnixNano())
}

func (meterProvider *MeterProvider) RecordHistogram(metricName string, resourceAttributes, attributes pcommon.Map,
	startTimestamp, endTimestamp pcommon.Timestamp, durationNanos int64) pmetric.HistogramDataPoint {

	metrics := meterProvider.getResourceMetrics(resourceAttributes)

	histogram := metrics.GetOrCreateHistogramMetric(metricName)
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

func (meterProvider *MeterProvider) getResourceMetrics(attributes pcommon.Map) ResourceMetrics {
	key := GetKeyFromMap(attributes)
	if metrics, exists := meterProvider.resourceMetrics[key]; exists {
		return metrics
	} else {
		resourceMetrics := meterProvider.Metrics.ResourceMetrics().AppendEmpty()
		attributes.CopyTo(resourceMetrics.Resource().Attributes())
		metrics := resourceMetrics.ScopeMetrics().AppendEmpty().Metrics()
		rm := ResourceMetrics{metrics: metrics, nameToMetric: make(map[string]pmetric.Metric)}
		meterProvider.resourceMetrics[key] = rm
		return rm
	}
}

func (metrics ResourceMetrics) GetOrCreateHistogramMetric(metricName string) pmetric.Histogram {
	init := func(metric pmetric.Metric) {
		metric.SetUnit("s")

		histogram := metric.SetEmptyHistogram()
		histogram.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	}
	metric := metrics.GetOrCreateMetric(metricName, init)
	return metric.Histogram()
}

func (metrics ResourceMetrics) GetOrCreateSumMetric(metricName string) pmetric.Sum {
	init := func(metric pmetric.Metric) {
		sum := metric.SetEmptySum()
		sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		sum.SetIsMonotonic(false)
	}
	metric := metrics.GetOrCreateMetric(metricName, init)
	return metric.Sum()
}

func (metrics ResourceMetrics) GetOrCreateMetric(metricName string, init func(pmetric.Metric)) pmetric.Metric {
	if metric, exists := metrics.nameToMetric[metricName]; exists {
		return metric
	} else {
		metric := metrics.metrics.AppendEmpty()
		metric.SetName(metricName)
		metrics.nameToMetric[metricName] = metric
		init(metric)
		return metric
	}
}

func (meterProvider *MeterProvider) IncrementSum(metricName string, resourceAttributes, attributes pcommon.Map,
	timestamp pcommon.Timestamp) pmetric.NumberDataPoint {
	metrics := meterProvider.getResourceMetrics(resourceAttributes)

	sum := metrics.GetOrCreateSumMetric(metricName)
	dp := sum.DataPoints().AppendEmpty()
	attributes.CopyTo(dp.Attributes())

	dp.SetTimestamp(timestamp)

	dp.SetIntValue(1)
	return dp
}

func NanosToSeconds(nanos int64) float64 {
	return float64(nanos) / 1e9
}

func GetKeyFromMap(pMap pcommon.Map) string {
	m := make(map[string]string, pMap.Len())
	pMap.Range(func(k string, v pcommon.Value) bool {
		m[k] = v.AsString()
		return true
	})
	return GetKey(m)
}

func GetKey(m map[string]string) string {
	// map order is not guaranteed, we need to hash key values in order
	allKeys := make([]string, len(m))
	for k, _ := range m {
		allKeys = append(allKeys, k)
	}
	sort.Strings(allKeys)
	toHash := make([]string, 2*len(m))
	for _, k := range allKeys {
		toHash = append(toHash, k)
		toHash = append(toHash, m[k])
	}
	return Hash(toHash)
}

func Hash(objs []string) string {
	digester := crypto.MD5.New()
	for _, ob := range objs {
		// do we need this?
		//fmt.Fprint(digester, reflect.TypeOf(ob))
		fmt.Fprint(digester, ob)
	}
	return string(digester.Sum(nil))
}
