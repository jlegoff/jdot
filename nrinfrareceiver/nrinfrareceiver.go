package nrinfrareceiver

import (
	"bufio"
	"context"
	"fmt"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.uber.org/zap"
	"os"
	"os/exec"
)

type nrinfraReceiver struct {
	host         component.Host
	cancel       context.CancelFunc
	logger       *zap.Logger
	nextConsumer consumer.Metrics
	config       *Config
	ctx          context.Context
}

func (nrinfraReceiver *nrinfraReceiver) Start(ctx context.Context, host component.Host) error {
	nrinfraReceiver.host = host
	ctx = context.Background()
	ctx, nrinfraReceiver.cancel = context.WithCancel(ctx)
	nrinfraReceiver.ctx = ctx

	agentPath := os.Getenv("NRIA_AGENT_PATH")
	if agentPath == "" {
		agentPath = "newrelic-infra"
	}

	agentConfig := os.Getenv("NRIA_CONFIG_PATH")
	if agentConfig == "" {
		agentConfig = "newrelic-infra.yml"
	}
	go nrinfraReceiver.runAgent(agentPath, agentConfig)

	nrinfraReceiver.logger.Info("I should start processing metrics now!")
	return nil
}

func (nrinfraReceiver *nrinfraReceiver) runAgent(agentPath string, configPath string) {
	// run the agent as a binary!
	cmd := exec.Command("sh", "-c", fmt.Sprintf("./%s --config %s", agentPath, configPath))
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		fmt.Println("Error reading from stdout", err)
	}

	err = cmd.Start()
	if err != nil {
		fmt.Println("Error starting infra agent", err)
	}

	// scan the agent output and convert it to metrics
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		agentLine := scanner.Bytes()
		metrics := ConvertLine(agentLine)
		if metrics.ResourceMetrics().Len() > 0 {
			err = nrinfraReceiver.nextConsumer.ConsumeMetrics(nrinfraReceiver.ctx, metrics)
			if err != nil {
				fmt.Println("Error consuming metrics", err)
			}
		}
	}
	err = cmd.Wait()
	if err != nil {
		fmt.Println("Error waiting for infra agent to execute", err)
	}
}

func (nrinfraReceiver *nrinfraReceiver) Shutdown(context.Context) error {
	nrinfraReceiver.cancel()
	return nil
}
