package apmconnector

import (
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
	"testing"
	"time"
)

func TestConvertOneSpanToMetrics(t *testing.T) {
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

	logger, _ := zap.NewDevelopment()
	config := Config{ApdexT: 0.5}
	metrics := ConvertTraces(logger, &config, traces)
	assert.Equal(t, 3, metrics.MetricCount())
	rm := metrics.ResourceMetrics().At(0)
	serviceName, _ := rm.Resource().Attributes().Get("service.name")
	assert.Equal(t, "service", serviceName.AsString())
	sm := rm.ScopeMetrics().At(0)
	metric := sm.Metrics().At(0)
	assert.Equal(t, "apm.service.transaction.duration", metric.Name())
	dp := metric.Histogram().DataPoints().At(0)
	assert.Equal(t, 1.0, dp.Sum())
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
