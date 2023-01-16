package nrinfraexporter

import (
	"fmt"
)

type Config struct {
	LicenseKey string `mapstructure:"license_key"`
}

func (cfg *Config) Validate() error {
	if cfg.LicenseKey == "" {
		return fmt.Errorf("License key is mandatory")
	}
	return nil
}
