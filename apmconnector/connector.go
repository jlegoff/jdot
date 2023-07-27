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
}

func (c *ApmConnector) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (c *ApmConnector) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	metrics := ConvertTraces(td)
	return c.metricsConsumer.ConsumeMetrics(ctx, metrics)
}

func (c *ApmConnector) Start(_ context.Context, host component.Host) error {
	c.logger.Info("Starting the APM Connector")
	return nil
}

func (c *ApmConnector) Shutdown(context.Context) error {
	c.logger.Info("Stopping the APM Connector")
	return nil
}

func ConvertTraces(td ptrace.Traces) pmetric.Metrics {
	metrics := pmetric.NewMetrics()
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		resourceMetrics := metrics.ResourceMetrics().AppendEmpty()
		rs := td.ResourceSpans().At(i)
		rs.Resource().CopyTo(resourceMetrics.Resource())
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			scopeSpan := rs.ScopeSpans().At(j)
			scopeMetric := resourceMetrics.ScopeMetrics().AppendEmpty()
			for k := 0; k < scopeSpan.Spans().Len(); k++ {
				span := scopeSpan.Spans().At(k)
				if span.Kind() != ptrace.SpanKindServer {
					continue
				}
				metric := scopeMetric.Metrics().AppendEmpty()
				metric.SetName("apm.service.transaction.duration")
				metric.SetUnit("s")

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
				span.Attributes().CopyTo(dp.Attributes())
				httpRoute, routePresent := span.Attributes().Get("http.route")
				dp.Attributes().PutStr("transactionType", "Web")
				if routePresent {
					method, methodPresent := span.Attributes().Get("http.request.method")
					if methodPresent {
						dp.Attributes().PutStr("transactionName", fmt.Sprintf("WebTransaction/http.route/%s %s", httpRoute.Str(), method.Str()))
					} else {
						dp.Attributes().PutStr("transactionName", fmt.Sprintf("WebTransaction/http.route/%s", httpRoute.Str()))
					}
				}
			}
		}

	}
	return metrics
}
