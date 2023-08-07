package apmconnector

import (
	"crypto"
	"fmt"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
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
	res = &ResourceMetrics{
		origin:       resource,
		scopeMetrics: make(map[string]*ScopeMetrics),
	}
	(*allMetrics)[key] = res
	return res
}

type ResourceMetrics struct {
	origin       pcommon.Resource
	scopeMetrics map[string]*ScopeMetrics
}

func (rm *ResourceMetrics) GetOrCreateScope(scope pcommon.InstrumentationScope) *ScopeMetrics {
	key := GetKeyFromMap(scope.Attributes())
	scopeMetrics, scopeMetricsPresent := rm.scopeMetrics[key]
	if scopeMetricsPresent {
		return scopeMetrics
	}
	scopeMetrics = &ScopeMetrics{
		origin:  scope,
		metrics: make(map[string]*Metric),
	}
	rm.scopeMetrics[key] = scopeMetrics
	return scopeMetrics
}

type ScopeMetrics struct {
	origin  pcommon.InstrumentationScope
	metrics map[string]*Metric
}

func (sm *ScopeMetrics) GetOrCreateMetric(metricName string) *Metric {
	metric, metricPresent := sm.metrics[metricName]
	if metricPresent {
		return metric
	}
	metric = &Metric{
		metricName: metricName,
		datapoints: make(map[string]Datapoint),
	}
	sm.metrics[metricName] = metric
	return metric
}

type Metric struct {
	datapoints map[string]Datapoint
	metricName string
}

func (m *Metric) AddDatapoint(span ptrace.Span, dimensions map[string]string) {
	duration := float64((span.EndTimestamp() - span.StartTimestamp()).AsTime().UnixNano()) / 1e9
	m.AddDatapointWithValue(span, dimensions, duration)
}

func (m *Metric) AddDatapointWithValue(span ptrace.Span, dimensions map[string]string, value float64) {
	attributes := make(map[string]string)
	for k, v := range dimensions {
		attributes[k] = v
	}

	dp, dpPresent := m.datapoints[GetKey(attributes)]
	if !dpPresent {
		histogram := NewHistogram()
		dp = Datapoint{histogram: histogram, attributes: attributes, startTimestamp: span.StartTimestamp(), timestamp: span.EndTimestamp()}
		m.datapoints[GetKey(attributes)] = dp
	}
	dp.histogram.Update(value)
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
