package nrinfrareceiver

import (
	"context"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
)

const (
	typeStr           = "nrinfra"
	defaultConfigPath = "/etc/newrelic-infra.yaml"
	stability         = component.StabilityLevelBeta
)

// NewFactory creates a factory for tailtracer receiver.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(typeStr,
		createDefaultConfig,
		receiver.WithMetrics(createMetricsReceiver, stability))
}

func createDefaultConfig() component.Config {
	return &Config{
		AgentConfigPath: defaultConfigPath,
	}
}

func createMetricsReceiver(
	_ context.Context,
	params receiver.CreateSettings,
	rConf component.Config,
	consumer consumer.Metrics,
) (receiver.Metrics, error) {
	if consumer == nil {
		return nil, component.ErrNilNextConsumer
	}

	nrinfraReceiverCfg := rConf.(*Config)
	nrinfraReceiver := &nrinfraReceiver{
		logger:       params.Logger,
		nextConsumer: consumer,
		config:       nrinfraReceiverCfg,
	}
	return nrinfraReceiver, nil
}
