package apmconnector

import (
	"context"
	"crypto"
	"fmt"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	"reflect"
	"sort"
)

// A metric map is a data structure used by the connector while it is
// processing spans. Once the processing is done, the map is converted
// into OTEL metrics
// The map roughly follows the structure of an OTEL resource metrics:
// resource -> scope -> metric -> datapoints

type MetricMap map[string]*ResourceMetrics

func NewMetrics() MetricMap {
	return make(MetricMap)
}

func (allMetrics *MetricMap) GetOrCreateResource(resource pcommon.Resource) *ResourceMetrics {
	key := GetKeyFromMap(resource.Attributes())
	res, resourcePresent := (*allMetrics)[key]
	if resourcePresent {
		return res
	}
	attrs := make([]attribute.KeyValue, resource.Attributes().Len())
	resource.Attributes().Range(func(k string, v pcommon.Value) bool {
		// TODO handle more types (not only string)
		attrs = append(attrs, attribute.KeyValue{Key: attribute.Key(k), Value: attribute.StringValue(v.Str())})
		return true
	})
	newResource := sdkresource.NewWithAttributes("", attrs...)
	res = &ResourceMetrics{
		origin:        resource,
		scopeMetrics:  make(map[string]*ScopeMetrics),
		meterProvider: sdkmetric.NewMeterProvider(sdkmetric.WithResource(newResource), sdkmetric.WithReader()),
	}
	(*allMetrics)[key] = res
	return res
}

type ResourceMetrics struct {
	origin        pcommon.Resource
	scopeMetrics  map[string]*ScopeMetrics
	meterProvider *sdkmetric.MeterProvider
}

func (rm *ResourceMetrics) GetOrCreateScope(scope pcommon.InstrumentationScope) *ScopeMetrics {
	key := GetKeyFromMap(scope.Attributes())
	scopeMetrics, scopeMetricsPresent := rm.scopeMetrics[key]
	if scopeMetricsPresent {
		return scopeMetrics
	}
	meter := rm.meterProvider.Meter(scope.Name())
	scopeMetrics = &ScopeMetrics{
		origin:  scope,
		metrics: make(map[string]*Metric),
		meter:   &meter,
	}
	rm.scopeMetrics[key] = scopeMetrics
	return scopeMetrics
}

type ScopeMetrics struct {
	origin  pcommon.InstrumentationScope
	metrics map[string]*Metric
	meter   *metric.Meter
}

func (sm *ScopeMetrics) GetOrCreateMetric(metricName string) *Metric {
	m, metricPresent := sm.metrics[metricName]
	if metricPresent {
		return m
	}
	h, _ := (*sm.meter).Float64Histogram(metricName, metric.WithUnit("s"))
	m = &Metric{
		metricName: metricName,
		datapoints: make(map[string]Datapoint),
		histogram:  h,
	}
	sm.metrics[metricName] = m
	return m
}

type Metric struct {
	datapoints map[string]Datapoint
	metricName string
	histogram  metric.Float64Histogram
}

func (m *Metric) AddDatapoint(span ptrace.Span, dimensions map[string]string) {
	attributes := make(map[string]string)
	for k, v := range dimensions {
		attributes[k] = v
	}
	span.Attributes().Range(func(k string, v pcommon.Value) bool {
		attributes[k] = v.AsString()
		return true
	})
	kvattrs := make([]attribute.KeyValue, len(attributes))
	for k, v := range attributes {
		kvattrs = append(kvattrs, attribute.KeyValue{Key: attribute.Key(k), Value: attribute.StringValue(v)})
	}
	duration := float64((span.EndTimestamp() - span.StartTimestamp()).AsTime().UnixNano()) / 1e9
	m.histogram.Record(context.Background(), duration, metric.WithAttributes(kvattrs...))

	dp, dpPresent := m.datapoints[GetKey(attributes)]
	if !dpPresent {
		histogram := NewHistogram()
		dp = Datapoint{histogram: histogram, attributes: attributes, startTimestamp: span.StartTimestamp(), timestamp: span.EndTimestamp()}
		m.datapoints[GetKey(attributes)] = dp
	}
	dp.histogram.Update(duration)
	if dp.startTimestamp.AsTime().After(span.StartTimestamp().AsTime()) {
		dp.startTimestamp = span.StartTimestamp()
	}
	if dp.timestamp.AsTime().Before(span.EndTimestamp().AsTime()) {
		// FIXME set the timestamp to now?
		dp.timestamp = span.EndTimestamp()
	}
}

type Datapoint struct {
	histogram      Histogram
	attributes     map[string]string
	startTimestamp pcommon.Timestamp
	timestamp      pcommon.Timestamp
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
		fmt.Fprint(digester, reflect.TypeOf(ob))
		fmt.Fprint(digester, ob)
	}
	return string(digester.Sum(nil))
}
