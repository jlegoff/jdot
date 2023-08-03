package apmconnector // import "github.com/jlegoff/jdot/apmconnector"

//go:generate mdatagen metadata.yaml

import (
	"context"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
)

const (
	typeStr   = "apmconnector"
	stability = component.StabilityLevelBeta
)

// NewFactory returns a ConnectorFactory.
func NewFactory() connector.Factory {
	return connector.NewFactory(
		typeStr,
		createDefaultConfig,
		connector.WithTracesToMetrics(createTracesToMetrics, stability),
		connector.WithTracesToLogs(createTracesToLogs, stability),
	)
}

// createDefaultConfig creates the default configuration.
func createDefaultConfig() component.Config {
	return &Config{}
}

// createTracesToMetrics creates a traces to metrics connector based on provided config.
func createTracesToMetrics(
	_ context.Context,
	set connector.CreateSettings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (connector.Traces, error) {
	c := cfg.(*Config)

	return NewMetricApmConnector(nextConsumer, c, set.Logger), nil
}

// createTracesToMetrics creates a traces to logs connector based on provided config.
func createTracesToLogs(
	_ context.Context,
	set connector.CreateSettings,
	cfg component.Config,
	nextConsumer consumer.Logs,
) (connector.Traces, error) {
	c := cfg.(*Config)

	return NewLoggerApmConnector(nextConsumer, c, set.Logger), nil
}
