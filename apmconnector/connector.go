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

func NewMetricApmConnector(nextConsumer consumer.Metrics, config *Config, logger *zap.Logger) *ApmConnector {
	return &ApmConnector{
		config:          config,
		metricsConsumer: nextConsumer,
		logger:          logger,
		metricBuilder:   NewMetricBuilder(logger),
	}
}

func NewLoggerApmConnector(nextConsumer consumer.Logs, config *Config, logger *zap.Logger) *ApmConnector {
	return &ApmConnector{
		config:       config,
		logsConsumer: nextConsumer,
		logger:       logger,
	}
}

func (c *ApmConnector) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (c *ApmConnector) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	metrics, logs := c.ConvertDataPoints(td)
	if c.metricsConsumer != nil {
		err := c.metricsConsumer.ConsumeMetrics(ctx, metrics)
		c.metricBuilder.Reset()
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
		instrumentationProvider, instrumentationProviderPresent := rs.Resource().Attributes().Get("instrumentation.provider")
		if instrumentationProviderPresent && instrumentationProvider.AsString() != "opentelemetry" {
			c.logger.Debug("Skipping resource spans", zap.String("instrumentation.provider", instrumentationProvider.AsString()))
			continue
		}
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			scopeSpan := rs.ScopeSpans().At(j)
			for k := 0; k < scopeSpan.Spans().Len(); k++ {
				span := scopeSpan.Spans().At(k)
				if c.metricBuilder != nil {
					c.metricBuilder.Record(rs.Resource(), scopeSpan.Scope(), span)
				}
			}
		}
	}
	metrics := pmetric.NewMetrics()
	if c.metricBuilder != nil {
		metrics = c.metricBuilder.GetMetrics()
	}
	logs := BuildTransactions(td)
	return metrics, logs
}
