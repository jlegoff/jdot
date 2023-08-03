package apmconnector

import (
	"fmt"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
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
	metrics MetricMap
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
				addMetricToScope(*m, scopeMetrics)
			}
		}
	}
	return metrics
}

func addMetricToScope(metric Metric, scopeMetrics pmetric.ScopeMetrics) {
	otelMetric := scopeMetrics.Metrics().AppendEmpty()
	otelMetric.SetName(metric.metricName)
	otelMetric.SetUnit("s")

	histogram := otelMetric.SetEmptyExponentialHistogram()
	histogram.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	otelDatapoints := histogram.DataPoints()
	for _, dp := range metric.datapoints {
		histoDp := otelDatapoints.AppendEmpty()
		dp.histogram.AddDatapointToHistogram(histoDp)
		histoDp.SetStartTimestamp(dp.startTimestamp)
		histoDp.SetTimestamp(dp.timestamp)
		for k, v := range dp.attributes {
			histoDp.Attributes().PutStr(k, v)
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
