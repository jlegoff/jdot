package nrinfraexporter

import "encoding/json"

// copied from https://github.com/newrelic/infrastructure-agent/blob/d9c6f5f/internal/agent/event_sender.go#L277
type MetricPost struct {
	ExternalKeys     []string          `json:"ExternalKeys,omitempty"`
	EntityID         int64             `json:"EntityID,omitempty"`
	IsAgent          bool              `json:"IsAgent"`
	Events           []json.RawMessage `json:"Events"`
	ReportingAgentID int64             `json:"ReportingAgentID,omitempty"`
}
