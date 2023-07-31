package apmconnector

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type ApmConnector struct {
	config        *Config
	logger        *zap.Logger
	metricBuilder MetricBuilder

	metricsConsumer consumer.Metrics
	logsConsumer    consumer.Logs
}

func (c *ApmConnector) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (c *ApmConnector) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	metrics, logs := c.ConvertDataPoints(td)
	if c.metricsConsumer != nil {
		err := c.metricsConsumer.ConsumeMetrics(ctx, metrics)
		if err != nil {
			return err
		}
	}
	if c.logsConsumer != nil {
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

func (c *ApmConnector) ConvertDataPoints(td ptrace.Traces) (pmetric.Metrics, plog.Logs) {
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			scopeSpan := rs.ScopeSpans().At(j)
			for k := 0; k < scopeSpan.Spans().Len(); k++ {
				span := scopeSpan.Spans().At(k)
				c.metricBuilder.Record(rs.Resource(), scopeSpan.Scope(), span)
			}
		}
	}
	return c.metricBuilder.GetMetrics(), BuildTransactions(td)
}
