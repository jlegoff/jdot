package apmconnector

import (
	"context"

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
		metrics := ConvertTraces(c.logger, c.config, td)
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
	if c.config.ApdexT == 0 {
		c.config.ApdexT = 0.5
	}
	return nil
}

func (c *ApmConnector) Shutdown(context.Context) error {
	c.logger.Info("Stopping the APM Connector")
	return nil
}

func ConvertTraces(logger *zap.Logger, config *Config, td ptrace.Traces) pmetric.Metrics {
	attributesFilter := NewAttributeFilter()
	transactions := NewTransactionsMap(config.ApdexT)
	metrics := pmetric.NewMetrics()
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		resourceMetrics := metrics.ResourceMetrics().AppendEmpty()
		rs := td.ResourceSpans().At(i)
		instrumentationProvider, instrumentationProviderPresent := rs.Resource().Attributes().Get("instrumentation.provider")
		if instrumentationProviderPresent && instrumentationProvider.AsString() != "opentelemetry" {
			logger.Debug("Skipping resource spans", zap.String("instrumentation.provider", instrumentationProvider.AsString()))
			continue
		}

		attributesFilter.FilterAttributes(rs.Resource().Attributes()).CopyTo(resourceMetrics.Resource().Attributes())

		sdkLanguage := GetSdkLanguage(rs.Resource().Attributes())
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			scopeSpan := rs.ScopeSpans().At(j)
			scopeMetric := resourceMetrics.ScopeMetrics().AppendEmpty()
			for k := 0; k < scopeSpan.Spans().Len(); k++ {
				span := scopeSpan.Spans().At(k)
				if k == 0 {
					if hostName, exists := resourceMetrics.Resource().Attributes().Get("host.name"); exists {
						GenerateInstanceMetric(scopeMetric, hostName.AsString(), span.EndTimestamp())
					}
				}

				transaction, _ := transactions.GetOrCreateTransaction(sdkLanguage, span, scopeMetric.Metrics())

				//fmt.Printf("Span kind: %s Name: %s Trace Id: %s Span id: %s Parent: %s\n", span.Kind(), span.Name(), span.TraceID().String(), span.SpanID().String(), span.ParentSpanID().String())

				transaction.AddSpan(span)
			}
		}

	}

	transactions.ProcessTransactions()

	return metrics
}
