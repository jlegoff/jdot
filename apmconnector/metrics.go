package apmconnector

import (
	"fmt"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type MetricBuilder interface {
	// record a new data point
	Record(resource pcommon.Resource, scope pcommon.InstrumentationScope, span ptrace.Span)
	// return the list of created metrics
	GetMetrics() pmetric.Metrics
	// reset the cache
	Reset()
}

type MetricBuilderImpl struct {
	logger *zap.Logger
	//metrics map[string]*explicitHistogram
}

func ConvertTraces(td ptrace.Traces) pmetric.Metrics {
	metrics := pmetric.NewMetrics()
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		resourceMetrics := metrics.ResourceMetrics().AppendEmpty()
		rs := td.ResourceSpans().At(i)
		instrumentationProvider, instrumentationProviderPresent := rs.Resource().Attributes().Get("instrumentation.provider")
		if instrumentationProviderPresent && instrumentationProvider.AsString() != "opentelemetry" {
			logger.Debug("Skipping resource spans", zap.String("instrumentation.provider", instrumentationProvider.AsString()))
			continue
		}
		rs.Resource().CopyTo(resourceMetrics.Resource())
		sdkLanguage := GetSdkLanguage(rs.Resource().Attributes())
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			scopeSpan := rs.ScopeSpans().At(j)
			scopeMetric := resourceMetrics.ScopeMetrics().AppendEmpty()
			for k := 0; k < scopeSpan.Spans().Len(); k++ {
				span := scopeSpan.Spans().At(k)
				if span.Kind() == ptrace.SpanKindServer {
					ProcessServerSpan(span, scopeMetric, sdkLanguage)
				} else if span.Kind() == ptrace.SpanKindClient {
					// filter out db calls that have no parent (so no transaction)
					if !span.ParentSpanID().IsEmpty() {
						ProcessClientSpan(span, scopeMetric, sdkLanguage)
					}
				}
			}
		}

	}
	return metrics
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
