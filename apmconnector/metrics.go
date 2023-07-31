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
	// GetMetrics Return the list of created metrics
	GetMetrics() pmetric.Metrics
	// Reset the cache
	Reset()
}

type MetricBuilderImpl struct {
	logger  *zap.Logger
	metrics pmetric.Metrics
}

func NewMetricBuilder(logger *zap.Logger) MetricBuilder {
	mb := &MetricBuilderImpl{
		logger: logger,
	}
	mb.Reset()
	return mb
}

func (mb *MetricBuilderImpl) Reset() {
	mb.metrics = pmetric.NewMetrics()
}

func (mb *MetricBuilderImpl) GetMetrics() pmetric.Metrics {
	return mb.metrics
}

func (mb *MetricBuilderImpl) Record(resource pcommon.Resource, scope pcommon.InstrumentationScope, span ptrace.Span) {
	resourceMetrics := mb.metrics.ResourceMetrics().AppendEmpty()
	resource.CopyTo(resourceMetrics.Resource())
	sdkLanguage := GetSdkLanguage(resource.Attributes())

	scopeMetric := resourceMetrics.ScopeMetrics().AppendEmpty()
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

func ProcessDatabaseSpan(span ptrace.Span, scopeMetric pmetric.ScopeMetrics, sdkLanguage string) bool {
	dbSystem, dbSystemPresent := span.Attributes().Get("db.system")
	if dbSystemPresent {
		dbOperation, dbOperationPresent := span.Attributes().Get("db.operation")
		if dbOperationPresent {
			dbTable, dbTablePresent := span.Attributes().Get("db.sql.table")
			if dbTablePresent {
				metric := AddMetric(scopeMetric.Metrics(), "apm.service.datastore.operation.duration")
				dp := SetHistogramFromSpan(span, metric)
				span.Attributes().CopyTo(dp.Attributes())
				dp.Attributes().PutStr("db.operation", dbOperation.AsString())
				dp.Attributes().PutStr("db.system", dbSystem.AsString())
				dp.Attributes().PutStr("db.sql.table", dbTable.AsString())
				return true
			}
		}
	}
	return false
}

func ProcessExternalSpan(span ptrace.Span, scopeMetric pmetric.ScopeMetrics, sdkLanguage string) bool {
	serverAddress, serverAddressPresent := span.Attributes().Get("server.address")
	if serverAddressPresent {
		metric := AddMetric(scopeMetric.Metrics(), "apm.service.transaction.external.duration")
		dp := SetHistogramFromSpan(span, metric)
		span.Attributes().CopyTo(dp.Attributes())
		dp.Attributes().PutStr("external.host", serverAddress.AsString())

		// FIXME
		dp.Attributes().PutStr("transactionType", "Web")
		return true
	}
	return false
}

func ProcessClientSpan(span ptrace.Span, scopeMetric pmetric.ScopeMetrics, sdkLanguage string) {
	if !ProcessDatabaseSpan(span, scopeMetric, sdkLanguage) {
		ProcessExternalSpan(span, scopeMetric, sdkLanguage)
	}
}

func ProcessServerSpan(span ptrace.Span, scopeMetric pmetric.ScopeMetrics, sdkLanguage string) {

	metric := AddMetric(scopeMetric.Metrics(), "apm.service.transaction.duration")
	dp := SetHistogramFromSpan(span, metric)
	span.Attributes().CopyTo(dp.Attributes())
	dp.Attributes().PutStr("transactionType", "Web")
	transactionName := GetTransactionMetricName(span)
	dp.Attributes().PutStr("transactionName", transactionName)

	overviewWeb := AddMetric(scopeMetric.Metrics(), "apm.service.overview.web")
	overviewDp := SetHistogramFromSpan(span, overviewWeb)
	span.Attributes().CopyTo(overviewDp.Attributes())

	overviewDp.Attributes().PutStr("segmentName", sdkLanguage)

	txBreakdownMetric := AddMetric(scopeMetric.Metrics(), "apm.service.transaction.overview")
	txBreakdownDp := SetHistogramFromSpan(span, txBreakdownMetric)
	span.Attributes().CopyTo(txBreakdownDp.Attributes())
	txBreakdownDp.Attributes().PutStr("metricTimesliceName", sdkLanguage)
	txBreakdownDp.Attributes().PutStr("transactionName", transactionName)
}

func AddMetric(metrics pmetric.MetricSlice, metricName string) pmetric.Metric {
	metric := metrics.AppendEmpty()
	metric.SetName(metricName)
	metric.SetUnit("s")
	return metric
}

func SetHistogramFromSpan(span ptrace.Span, metric pmetric.Metric) pmetric.HistogramDataPoint {
	histogram := metric.SetEmptyHistogram()
	histogram.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	dp := histogram.DataPoints().AppendEmpty()
	dp.SetStartTimestamp(span.StartTimestamp())
	dp.SetTimestamp(span.EndTimestamp())

	duration := float64((span.EndTimestamp() - span.StartTimestamp()).AsTime().UnixNano()) / 1e9
	dp.SetSum(duration)
	dp.SetCount(1)
	dp.SetMin(duration)
	dp.SetMax(duration)
	return dp
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
