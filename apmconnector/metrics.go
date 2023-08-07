package apmconnector

import (
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
	logger         *zap.Logger
	metrics        MetricMap
	transactionMap *TransactionsMap
}

func NewMetricBuilder(logger *zap.Logger) MetricBuilder {
	mb := &MetricBuilderImpl{
		logger:         logger,
		transactionMap: NewTransactionsMap(),
	}
	mb.Reset()
	return mb
}

func (mb *MetricBuilderImpl) Reset() {
	mb.metrics = NewMetrics()
	mb.transactionMap = NewTransactionsMap()
}

func (mb *MetricBuilderImpl) GetMetrics() pmetric.Metrics {
	mb.transactionMap.ProcessTransactions()
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

	transaction, traceId := mb.transactionMap.GetOrCreateTransaction(sdkLanguage, span, scopeMetric)
	mb.transactionMap.Transactions[traceId] = transaction
	transaction.AddSpan(span)
}

func GetSdkLanguage(attributes pcommon.Map) string {
	sdkLanguage, sdkLanguagePresent := attributes.Get("telemetry.sdk.language")
	if sdkLanguagePresent {
		return sdkLanguage.AsString()
	}
	return "unknown"
}
