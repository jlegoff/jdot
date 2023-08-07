package apmconnector

import (
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
	"testing"
	"time"
)

func TestConvertOneSpanToMetrics(t *testing.T) {
	connector := NewMetricApmConnector(nil, &Config{}, &zap.Logger{})

	traces := ptrace.NewTraces()
	resourceSpans := traces.ResourceSpans().AppendEmpty()
	resourceSpans.Resource().Attributes().PutStr("service.name", "service")
	scopeSpans := resourceSpans.ScopeSpans().AppendEmpty().Spans()
	attrs := map[string]string{
		"attrKey": "attrValue",
	}
	end := time.Now()
	start := end.Add(-time.Second)
	spanValues := []TestSpan{{Start: start, End: end, Name: "span", Kind: ptrace.SpanKindServer}}
	addSpan(scopeSpans, attrs, spanValues)

	metrics, _ := connector.ConvertDataPoints(traces)
	assert.Equal(t, 2, metrics.MetricCount())

	scopeMetrics := metrics.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
	transactionDuration, transactionDurationPresent := getMetrics(scopeMetrics, "apm.service.transaction.duration")
	assert.True(t, transactionDurationPresent)
	assert.Equal(t, 1.0, transactionDuration.ExponentialHistogram().DataPoints().At(0).Sum())

	transactionOverview, transactionOverviewPresent := getMetrics(scopeMetrics, "apm.service.overview.web")
	assert.True(t, transactionOverviewPresent)
	assert.Equal(t, 1.0, transactionOverview.ExponentialHistogram().DataPoints().At(0).Sum())
}

func TestConvertMultipleSpansToMetrics(t *testing.T) {
	connector := NewMetricApmConnector(nil, &Config{}, &zap.Logger{})

	traces := ptrace.NewTraces()
	resourceSpans := traces.ResourceSpans().AppendEmpty()
	resourceSpans.Resource().Attributes().PutStr("service.name", "service")
	scopeSpans := resourceSpans.ScopeSpans().AppendEmpty().Spans()
	attrs := map[string]string{
		"attrKey": "attrValue",
	}
	end := time.Now()
	start := end.Add(-time.Second)
	spanValues := []TestSpan{
		{Start: start, End: end, Name: "span", Kind: ptrace.SpanKindServer},
		{Start: start, End: end, Name: "span", Kind: ptrace.SpanKindServer},
	}
	addSpan(scopeSpans, attrs, spanValues)

	metrics, _ := connector.ConvertDataPoints(traces)
	assert.Equal(t, 3, metrics.MetricCount())

	scopeMetrics := metrics.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
	transactionDuration, transactionDurationPresent := getMetrics(scopeMetrics, "apm.service.transaction.duration")
	assert.True(t, transactionDurationPresent)
	assert.Equal(t, 1, transactionDuration.ExponentialHistogram().DataPoints().Len())
	assert.Equal(t, uint64(2), transactionDuration.ExponentialHistogram().DataPoints().At(0).Count())
}

func getMetrics(metrics pmetric.MetricSlice, metricName string) (*pmetric.Metric, bool) {
	for i := 0; i < metrics.Len(); i++ {
		m := metrics.At(i)
		if metricName == m.Name() {
			return &m, true
		}
	}
	return nil, false
}

func TestConvertOneSpanToLogs(t *testing.T) {
	traces := ptrace.NewTraces()
	resourceSpans := traces.ResourceSpans().AppendEmpty()
	resourceSpans.Resource().Attributes().PutStr("service.name", "service")
	scopeSpans := resourceSpans.ScopeSpans().AppendEmpty().Spans()
	attrs := map[string]string{
		"attrKey": "attrValue",
	}
	end := time.Now()
	start := end.Add(-time.Second)
	spanValues := []TestSpan{{Start: start, End: end, Name: "span", Kind: ptrace.SpanKindServer}}
	addSpan(scopeSpans, attrs, spanValues)

	logs := BuildTransactions(traces)
	assert.Equal(t, 1, logs.LogRecordCount())
}

func addSpan(spanSlice ptrace.SpanSlice, attributes map[string]string, spanValues []TestSpan) {
	for _, spanValue := range spanValues {
		span := spanSlice.AppendEmpty()
		span.SetName(spanValue.Name)
		span.SetEndTimestamp(pcommon.NewTimestampFromTime(time.Unix(spanValue.End.Unix(), 0)))
		span.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Unix(spanValue.Start.Unix(), 0)))
		span.SetKind(spanValue.Kind)
		for k, v := range attributes {
			span.Attributes().PutStr(k, v)
		}
	}
}

type TestSpan struct {
	Start time.Time
	End   time.Time
	Name  string
	Kind  ptrace.SpanKind
}
