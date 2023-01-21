package nrinfrareceiver

import (
	"fmt"
	"os"
)

type Config struct {
	AgentConfigPath string `mapstructure:"agent_config_path"`
}

func (cfg *Config) Validate() error {
	if cfg.AgentConfigPath != "" {
		if _, err := os.Stat(cfg.AgentConfigPath); os.IsNotExist(err) {
			return fmt.Errorf("Unable to access Infra agent path: %s", cfg.AgentConfigPath)
		}
	}
	return nil
}
