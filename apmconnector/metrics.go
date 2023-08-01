package apmconnector

import (
	"fmt"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
	"math"
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
	otelDatapoints.EnsureCapacity(len(metric.datapoints))
	for _, dp := range metric.datapoints {
		createHistogramDatapoint(histogram, dp)
	}

}

func createHistogramDatapoint(histogram pmetric.ExponentialHistogram, dp Datapoint) {
	otelDatapoints := histogram.DataPoints()
	otelDatapoint := otelDatapoints.AppendEmpty()
	otelDatapoint.SetStartTimestamp(dp.span.StartTimestamp())
	otelDatapoint.SetTimestamp(dp.span.EndTimestamp())
	dp.span.Attributes().CopyTo(otelDatapoint.Attributes())
	for k, v := range dp.attributes {
		otelDatapoint.Attributes().PutStr(k, v)
	}
	duration := float64((dp.span.EndTimestamp() - dp.span.StartTimestamp()).AsTime().UnixNano()) / 1e9
	otelDatapoint.SetSum(otelDatapoint.Sum() + duration)
	otelDatapoint.SetCount(otelDatapoint.Count() + 1)
	otelDatapoint.SetMin(math.Min(otelDatapoint.Min(), duration))
	otelDatapoint.SetMax(math.Max(otelDatapoint.Max(), duration))
	// FIXME missing scale, zeros, etc.
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
				metric := scopeMetric.GetOrCreateMetric("apm.service.datastore.operation.duration", sdkLanguage)
				attributes := map[string]string{
					"db.operation": dbOperation.AsString(),
					"db.system":    dbSystem.AsString(),
					"db.sql.table": dbTable.AsString(),
				}
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
		metric := scopeMetric.GetOrCreateMetric("apm.service.transaction.external.duration", sdkLanguage)
		attributes := map[string]string{
			"external.host": serverAddress.AsString(),
			// FIXME
			"transactionType": "Web",
		}
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
	metric := scopeMetric.GetOrCreateMetric("apm.service.transaction.duration", sdkLanguage)
	attributes := map[string]string{
		"transactionType": "Web",
		"transactionName": GetTransactionMetricName(span),
	}
	metric.AddDatapoint(span, attributes)

	overviewWeb := scopeMetric.GetOrCreateMetric("apm.service.overview.web", sdkLanguage)
	overviewAttributes := map[string]string{
		"segmentName": sdkLanguage,
	}
	overviewWeb.AddDatapoint(span, overviewAttributes)

	txBreakdownMetric := scopeMetric.GetOrCreateMetric("apm.service.transaction.overview", sdkLanguage)
	txBreakdownAttributes := map[string]string{
		"metricTimesliceName": sdkLanguage,
		"transactionName":     GetTransactionMetricName(span),
	}
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
	// TODO
	key := ""
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
	// TODO
	key := ""
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

func (sm *ScopeMetrics) GetOrCreateMetric(metricName string, sdkLanguage string) *Metric {
	key := metricName
	metric, metricPresent := sm.metrics[metricName]
	if metricPresent {
		return metric
	}
	metric = &Metric{
		metricName:  metricName,
		sdkLanguage: sdkLanguage,
		datapoints:  make([]Datapoint, 0),
	}
	sm.metrics[key] = metric
	return metric
}

type Metric struct {
	datapoints  []Datapoint
	sdkLanguage string
	metricName  string
}

func (m *Metric) AddDatapoint(span ptrace.Span, attributes map[string]string) {
	m.datapoints = append(m.datapoints, Datapoint{span: span, attributes: attributes})
}

type Datapoint struct {
	span       ptrace.Span
	attributes map[string]string
}
