package nrinfraexporter

import (
	"context"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
)

const (
	typeStr   = "nrinfra"
	stability = component.StabilityLevelBeta
)

func NewFactory() exporter.Factory {
	return exporter.NewFactory(typeStr,
		createDefaultConfig,
		exporter.WithMetrics(createInfraExporter, stability))
}

func createDefaultConfig() component.Config {
	return &Config{}
}

func createInfraExporter(
	_ context.Context,
	params exporter.CreateSettings,
	rConf component.Config,
) (exporter.Metrics, error) {

	nrInfraExporterCfg := rConf.(*Config)
	nrInfraExporter := &nrInfraExporter{
		config: nrInfraExporterCfg,
	}
	return nrInfraExporter, nil
}
