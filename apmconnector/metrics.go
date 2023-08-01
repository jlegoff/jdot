package apmconnector

import (
	"crypto"
	"fmt"
	"github.com/lightstep/go-expohisto/structure"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
	"reflect"
	"sort"
)

type MetricBuilder interface {
	// Record a new data point
	Record(resource pcommon.Resource, scope pcommon.InstrumentationScope, span ptrace.Span)
	// GetMetrics returns the list of created metrics
	GetMetrics() pmetric.Metrics
	// Reset the cache
	Reset()
}

type MetricBuilderImpl struct {
	logger  *zap.Logger
	metrics AllMetrics
}

func NewMetricBuilder(logger *zap.Logger) MetricBuilder {
	mb := &MetricBuilderImpl{
		logger: logger,
	}
	mb.Reset()
	return mb
}

func (mb *MetricBuilderImpl) Reset() {
	mb.metrics = NewMetrics()
}

func (mb *MetricBuilderImpl) GetMetrics() pmetric.Metrics {
	metrics := pmetric.NewMetrics()
	for _, rm := range mb.metrics {
		resourceMetrics := metrics.ResourceMetrics().AppendEmpty()
		rm.origin.CopyTo(resourceMetrics.Resource())
		for _, sm := range rm.scopeMetrics {
			scopeMetrics := resourceMetrics.ScopeMetrics().AppendEmpty()
			sm.origin.CopyTo(scopeMetrics.Scope())
			for _, m := range sm.metrics {
				addMetric(*m, scopeMetrics)
			}
		}
	}
	return metrics
}

func addMetric(metric Metric, scopeMetrics pmetric.ScopeMetrics) {
	otelMetric := scopeMetrics.Metrics().AppendEmpty()
	otelMetric.SetName(metric.metricName)
	otelMetric.SetUnit("s")

	histogram := otelMetric.SetEmptyExponentialHistogram()
	histogram.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	otelDatapoints := histogram.DataPoints()
	for _, dp := range metric.datapoints {
		histoDp := otelDatapoints.AppendEmpty()
		expoHistToExponentialDataPoint(dp.histogram, histoDp)
		histoDp.SetStartTimestamp(dp.startTimestamp)
		histoDp.SetTimestamp(dp.timestamp)
	}

}

// expoHistToExponentialDataPoint copies `lightstep/go-expohisto` structure.Histogram to
// pmetric.ExponentialHistogramDataPoint
func expoHistToExponentialDataPoint(agg *structure.Histogram[float64], dp pmetric.ExponentialHistogramDataPoint) {
	dp.SetCount(agg.Count())
	dp.SetSum(agg.Sum())
	if agg.Count() != 0 {
		dp.SetMin(agg.Min())
		dp.SetMax(agg.Max())
	}

	dp.SetZeroCount(agg.ZeroCount())
	dp.SetScale(agg.Scale())

	for _, half := range []struct {
		inFunc  func() *structure.Buckets
		outFunc func() pmetric.ExponentialHistogramDataPointBuckets
	}{
		{agg.Positive, dp.Positive},
		{agg.Negative, dp.Negative},
	} {
		in := half.inFunc()
		out := half.outFunc()
		out.SetOffset(in.Offset())
		out.BucketCounts().EnsureCapacity(int(in.Len()))

		for i := uint32(0); i < in.Len(); i++ {
			out.BucketCounts().Append(in.At(i))
		}
	}
}

func (mb *MetricBuilderImpl) Record(resource pcommon.Resource, scope pcommon.InstrumentationScope, span ptrace.Span) {
	resourceMetrics := mb.metrics.GetOrCreateResource(resource)
	sdkLanguage := GetSdkLanguage(resource.Attributes())
	scopeMetric := resourceMetrics.GetOrCreateScope(scope)
	if span.Kind() == ptrace.SpanKindServer {
		ProcessServerSpan(span, scopeMetric, sdkLanguage)
	} else if span.Kind() == ptrace.SpanKindClient {
		// filter out db calls that have no parent (so no transaction)
		if !span.ParentSpanID().IsEmpty() {
			ProcessClientSpan(span, scopeMetric, sdkLanguage)
		}
	}

}

func GetSdkLanguage(attributes pcommon.Map) string {
	sdkLanguage, sdkLanguagePresent := attributes.Get("telemetry.sdk.language")
	if sdkLanguagePresent {
		return sdkLanguage.AsString()
	}
	return "unknown"
}

func ProcessDatabaseSpan(span ptrace.Span, scopeMetric *ScopeMetrics, sdkLanguage string) bool {
	dbSystem, dbSystemPresent := span.Attributes().Get("db.system")
	if dbSystemPresent {
		dbOperation, dbOperationPresent := span.Attributes().Get("db.operation")
		if dbOperationPresent {
			dbTable, dbTablePresent := span.Attributes().Get("db.sql.table")
			if dbTablePresent {
				attributes := map[string]string{
					"db.operation": dbOperation.AsString(),
					"db.system":    dbSystem.AsString(),
					"db.sql.table": dbTable.AsString(),
				}
				metric := scopeMetric.GetOrCreateMetric("apm.service.datastore.operation.duration", span, attributes)
				metric.AddDatapoint(span, attributes)
				return true
			}
		}
	}
	return false
}

func ProcessExternalSpan(span ptrace.Span, scopeMetric *ScopeMetrics, sdkLanguage string) bool {
	serverAddress, serverAddressPresent := span.Attributes().Get("server.address")
	if serverAddressPresent {
		attributes := map[string]string{
			"external.host": serverAddress.AsString(),
			// FIXME
			"transactionType": "Web",
		}
		metric := scopeMetric.GetOrCreateMetric("apm.service.transaction.external.duration", span, attributes)
		metric.AddDatapoint(span, attributes)
		return true
	}
	return false
}

func ProcessClientSpan(span ptrace.Span, scopeMetric *ScopeMetrics, sdkLanguage string) {
	if !ProcessDatabaseSpan(span, scopeMetric, sdkLanguage) {
		ProcessExternalSpan(span, scopeMetric, sdkLanguage)
	}
}

func ProcessServerSpan(span ptrace.Span, scopeMetric *ScopeMetrics, sdkLanguage string) {
	attributes := map[string]string{
		"transactionType": "Web",
		"transactionName": GetTransactionMetricName(span),
	}
	metric := scopeMetric.GetOrCreateMetric("apm.service.transaction.duration", span, attributes)
	metric.AddDatapoint(span, attributes)

	overviewAttributes := map[string]string{
		"segmentName": sdkLanguage,
	}
	overviewWeb := scopeMetric.GetOrCreateMetric("apm.service.overview.web", span, overviewAttributes)
	overviewWeb.AddDatapoint(span, overviewAttributes)

	txBreakdownAttributes := map[string]string{
		"metricTimesliceName": sdkLanguage,
		"transactionName":     GetTransactionMetricName(span),
	}
	txBreakdownMetric := scopeMetric.GetOrCreateMetric("apm.service.transaction.overview", span, txBreakdownAttributes)
	txBreakdownMetric.AddDatapoint(span, txBreakdownAttributes)
}

func GetTransactionMetricName(span ptrace.Span) string {
	httpRoute, routePresent := span.Attributes().Get("http.route")
	if routePresent {
		// http.request.method
		method, methodPresent := span.Attributes().Get("http.method")
		// http.route starts with a /
		if methodPresent {
			return fmt.Sprintf("WebTransaction/http.route%s (%s)", httpRoute.Str(), method.Str())
		} else {
			return fmt.Sprintf("WebTransaction/http.route%s", httpRoute.Str())
		}
	}
	return "WebTransaction/Other/unknown"
}

type AllMetrics map[string]*ResourceMetrics

func NewMetrics() AllMetrics {
	return make(AllMetrics)
}

func (allMetrics *AllMetrics) GetOrCreateResource(resource pcommon.Resource) *ResourceMetrics {
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

func (sm *ScopeMetrics) GetOrCreateMetric(metricName string, span ptrace.Span, attributes map[string]string) *Metric {
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
	attributes := make(map[string]string)
	for k, v := range dimensions {
		attributes[k] = v
	}
	span.Attributes().Range(func(k string, v pcommon.Value) bool {
		attributes[k] = v.AsString()
		return true
	})

	dp, dpPresent := m.datapoints[GetKey(attributes)]
	if !dpPresent {
		histogram := new(structure.Histogram[float64])
		histogram.Init(structure.NewConfig())
		dp = Datapoint{histogram: histogram, attributes: attributes, startTimestamp: span.StartTimestamp(), timestamp: span.EndTimestamp()}
		m.datapoints[GetKey(attributes)] = dp
	}
	duration := float64((span.EndTimestamp() - span.StartTimestamp()).AsTime().UnixNano()) / 1e9
	dp.histogram.Update(duration)
	if dp.startTimestamp.AsTime().After(span.StartTimestamp().AsTime()) {
		dp.startTimestamp = span.StartTimestamp()
	}
	if dp.timestamp.AsTime().Before(span.EndTimestamp().AsTime()) {
		dp.timestamp = span.EndTimestamp()
	}
}

type Datapoint struct {
	histogram      *structure.Histogram[float64]
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
	for k, v := range m {
		toHash = append(toHash, k)
		toHash = append(toHash, v)
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
