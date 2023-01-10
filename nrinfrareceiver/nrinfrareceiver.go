package nrinfrareceiver

import (
	"context"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.uber.org/zap"
)

type nrinfraReceiver struct {
	host         component.Host
	cancel       context.CancelFunc
	logger       *zap.Logger
	nextConsumer consumer.Metrics
	config       *Config
}

func (nrinfraReceiver *nrinfraReceiver) Start(ctx context.Context, host component.Host) error {
	nrinfraReceiver.host = host
	ctx = context.Background()
	ctx, nrinfraReceiver.cancel = context.WithCancel(ctx)

	nrinfraReceiver.logger.Info("I should start processing metrics now!")
	return nil
}

func (nrinfraReceiver *nrinfraReceiver) Shutdown(context.Context) error {
	nrinfraReceiver.cancel()
	return nil
}
