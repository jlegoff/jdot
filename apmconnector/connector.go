package apmconnector

import (
	"context"
	"fmt"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type ApmConnector struct {
	config *Config
	logger *zap.Logger

	metricsConsumer consumer.Metrics
	logsConsumer    consumer.Logs
}

func (c *ApmConnector) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (c *ApmConnector) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	if c.metricsConsumer != nil {
		metrics := ConvertTraces(c.logger, td)
		err := c.metricsConsumer.ConsumeMetrics(ctx, metrics)
		if err != nil {
			return err
		}
	}
	if c.logsConsumer != nil {
		logs := BuildTransactions(td)
		return c.logsConsumer.ConsumeLogs(ctx, logs)
	}
	return nil
}

func (c *ApmConnector) Start(_ context.Context, host component.Host) error {
	c.logger.Info("Starting the APM Connector")
	return nil
}

func (c *ApmConnector) Shutdown(context.Context) error {
	c.logger.Info("Stopping the APM Connector")
	return nil
}

func ConvertTraces(logger *zap.Logger, td ptrace.Traces) pmetric.Metrics {
	metrics := pmetric.NewMetrics()
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		resourceMetrics := metrics.ResourceMetrics().AppendEmpty()
		rs := td.ResourceSpans().At(i)
		rs.Resource().CopyTo(resourceMetrics.Resource())
		sdkLanguage, sdkLanguagePresent := rs.Resource().Attributes().Get("telemetry.sdk.language")
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			scopeSpan := rs.ScopeSpans().At(j)
			scopeMetric := resourceMetrics.ScopeMetrics().AppendEmpty()
			for k := 0; k < scopeSpan.Spans().Len(); k++ {
				span := scopeSpan.Spans().At(k)
				if span.Kind() != ptrace.SpanKindServer {
					continue
				}

				metric := AddMetric(scopeMetric.Metrics(), "apm.service.transaction.duration")
				dp := SetHistogramFromSpan(span, metric)
				span.Attributes().CopyTo(dp.Attributes())
				dp.Attributes().PutStr("transactionType", "Web")
				transactionName := GetTransactionMetricName(span)
				dp.Attributes().PutStr("transactionName", transactionName)

				overviewWeb := AddMetric(scopeMetric.Metrics(), "apm.service.overview.web")
				overviewDp := SetHistogramFromSpan(span, overviewWeb)
				span.Attributes().CopyTo(overviewDp.Attributes())
				if sdkLanguagePresent {
					overviewDp.Attributes().PutStr("segmentName", sdkLanguage.AsString())

					txBreakdownMetric := AddMetric(scopeMetric.Metrics(), "apm.service.transaction.overview")
					txBreakdownDp := SetHistogramFromSpan(span, txBreakdownMetric)
					span.Attributes().CopyTo(txBreakdownDp.Attributes())
					txBreakdownDp.Attributes().PutStr("metricTimesliceName", sdkLanguage.AsString())
					txBreakdownDp.Attributes().PutStr("transactionName", transactionName)
				}
			}
		}

	}
	return metrics
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
