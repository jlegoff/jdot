package nrinfraexporter

import (
	"context"
	"fmt"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type nrInfraExporter struct {
	config *Config
}

func (pe *nrInfraExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (pe *nrInfraExporter) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	fmt.Println("Received ", md.MetricCount(), " metrics")
	return nil
}

func (pe *nrInfraExporter) Start(_ context.Context, host component.Host) error {
	fmt.Println("Starting the infra exporter")
	return nil
}

func (pe *nrInfraExporter) Shutdown(context.Context) error {
	return nil
}
