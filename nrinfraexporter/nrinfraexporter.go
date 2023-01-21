package nrinfraexporter

import (
	"context"
	"fmt"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

type nrInfraExporter struct {
	config *Config
	logger *zap.Logger
}

func (pe *nrInfraExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (nrInfraExporter *nrInfraExporter) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	nrInfraExporter.logger.Info(fmt.Sprintf("Received %d metrics", md.MetricCount()))
	samples := ConvertMetrics(md)
	SendEvents("https://staging-infra-api.newrelic.com", nrInfraExporter.config.LicenseKey, samples)
	return nil
}

func (nrInfraExporter *nrInfraExporter) Start(_ context.Context, host component.Host) error {
	nrInfraExporter.logger.Info("Starting the infra exporter")
	return nil
}

func (pe *nrInfraExporter) Shutdown(context.Context) error {
	return nil
}
