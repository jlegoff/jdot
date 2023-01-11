package nrinfrareceiver

import (
	"encoding/json"
	"fmt"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"strings"
	"time"
)

func ConvertLine(line []byte) pmetric.Metrics {
	entities, err := extractBatch(line)
	if err != nil {
		return pmetric.NewMetrics()
	}

	return createMetrics(entities)
}

func ConvertFromRawEntity(rawEntities []RawEntities) pmetric.Metrics {
	entities, err := extractEntities(rawEntities)
	if err != nil {
		return pmetric.NewMetrics()
	}

	return createMetrics(entities)
}

func createMetrics(entities []Entities) pmetric.Metrics {
	metrics := pmetric.NewMetrics()
	for _, entity := range entities {
		resourceMetrics := metrics.ResourceMetrics().AppendEmpty()
		resourceMetrics.Resource().Attributes().PutInt("EntityId", entity.EntityID)
		ms := resourceMetrics.ScopeMetrics().AppendEmpty().Metrics()
		for _, event := range entity.Events {
			for sampleMetricName, sampleMetricValue := range event.Metrics {
				m := ms.AppendEmpty()
				m.SetName(createMetricName(event.EventType, sampleMetricName))
				dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
				for attrName, attrValue := range event.Attributes {
					dp.Attributes().PutStr(attrName, attrValue.(string))
				}
				dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(int64(event.Timestamp), 0)))
				dp.SetDoubleValue(sampleMetricValue)
			}
		}
	}
	return metrics
}

func createMetricName(eventType string, sampleMetricName string) string {
	prefix := strings.ToLower(strings.TrimSuffix(eventType, "Sample"))
	return fmt.Sprintf("%s.%s", prefix, sampleMetricName)
}

func extractBatch(line []byte) ([]Entities, error) {
	var rawEntities []RawEntities
	marshErr := json.Unmarshal(line, &rawEntities)
	if marshErr != nil {
		return nil, marshErr
	}
	return extractEntities(rawEntities)
}

func extractEntities(rawEntities []RawEntities) ([]Entities, error) {
	entities := make([]Entities, 0)
	for _, rawEntity := range rawEntities {
		events := make([]Event, 0)
		for _, rawEvent := range rawEntity.RawEvents {
			eventType, eventTypePresent := getString(rawEvent, "eventType")
			if !eventTypePresent {
				continue
			}
			entityKey, entityKeyPresent := getString(rawEvent, "entityKey")
			if !entityKeyPresent {
				continue
			}
			timestamp, timestampPresent := getFloat(rawEvent, "timestamp")
			if !timestampPresent {
				continue
			}
			attributes, metrics := getAttributesAndMetrics(rawEvent)
			event := Event{
				EventType:  eventType,
				EntityKey:  entityKey,
				Timestamp:  int(timestamp),
				Attributes: attributes,
				Metrics:    metrics,
			}
			events = append(events, event)
		}
		if len(events) > 0 {
			entities = append(entities, Entities{
				EntityID: rawEntity.EntityID,
				IsAgent:  rawEntity.IsAgent,
				Events:   events,
			})
		}
	}
	return entities, nil
}

func getString(rawEvent RawEvent, keyName string) (string, bool) {
	rawValue, present := rawEvent[keyName]
	if !present {
		return "", false
	}
	return rawValue.(string), true
}

func getFloat(rawEvent RawEvent, keyName string) (float64, bool) {
	rawValue, present := rawEvent[keyName]
	if !present {
		return 0, false
	}
	return rawValue.(float64), true
}

func getAttributesAndMetrics(rawEvent RawEvent) (map[string]interface{}, map[string]float64) {
	attributes := make(map[string]interface{})
	metrics := make(map[string]float64)
	for key, element := range rawEvent {
		if key == "eventType" || key == "timestamp" || key == "entityKey" {
			continue
		}
		switch v := element.(type) {
		case float64:
			metrics[key] = v
		case int:
			metrics[key] = float64(v)
		case string:
			attributes[key] = v
		default:
			continue
		}
	}
	return attributes, metrics
}
