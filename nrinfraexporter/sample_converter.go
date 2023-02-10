package nrinfraexporter

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"sort"
	"strconv"
	"strings"
)

type AllSamples struct {
	// entityId -> Samples
	EntitySamples map[int64]EntitySamples
}

// Samples for a given entity
type EntitySamples struct {
	EntityId int64
	// identifying attributes -> [samples]
	// identifying attributes are:
	//  - event type
	//  - timestamp
	//  - dimensions
	Samples map[string]Sample
}

type Sample map[string]interface{}

func (entitySamples *EntitySamples) AddDataPoint(resource pcommon.Resource, dp pmetric.NumberDataPoint) {
	eventType, hasEventType := dp.Attributes().Get("newrelic.infraEventType")
	if !hasEventType {
		return
	}
	eventMetricName, hasEventMetricName := dp.Attributes().Get("newrelic.infraMetricName")
	if !hasEventMetricName {
		return
	}
	allAttributes := mergeAttributes(resource, dp)
	identifyingHash := makeHash(eventType.Str(), dp.Timestamp(), allAttributes)
	sample, hasSample := entitySamples.Samples[identifyingHash]
	if !hasSample {
		sample = make(Sample, 0)
		entitySamples.Samples[identifyingHash] = sample
	}
	sample["timestamp"] = dp.Timestamp().AsTime().Unix()
	sample["eventType"] = eventType.Str()
	sample[eventMetricName.Str()] = dp.DoubleValue()

	allAttributes.Range(func(key string, value pcommon.Value) bool {
		if key == "newrelic.infraEventType" || key == "newrelic.infraMetricName" {
			return true
		}
		switch value.Type() {
		case pcommon.ValueTypeStr:
			sample[key] = value.Str()
		case pcommon.ValueTypeInt:
			sample[key] = value.Int()
		case pcommon.ValueTypeDouble:
			sample[key] = value.Double()
		case pcommon.ValueTypeBool:
			sample[key] = value.Bool()
		}
		return true
	})
}

func mergeAttributes(resource pcommon.Resource, dp pmetric.NumberDataPoint) pcommon.Map {
	rawAttrs := dp.Attributes().AsRaw()
	for key, val := range resource.Attributes().AsRaw() {
		rawAttrs[key] = val
	}
	allAttributes := pcommon.NewMap()
	allAttributes.FromRaw(rawAttrs)
	return allAttributes
}

func makeHash(eventType string, timestamp pcommon.Timestamp, attributes pcommon.Map) string {
	identifiers := make([]string, 0)
	identifiers = append(identifiers, eventType)
	identifiers = append(identifiers, timestamp.String())

	var keys []string
	attributes.Range(func(key string, value pcommon.Value) bool {
		if key == "newrelic.infraEventType" || key == "newrelic.infraMetricName" {
			return true
		}
		keys = append(keys, key)
		return true
	})

	// sort the keys to make the identifier reproducible
	sort.Strings(keys)
	for _, key := range keys {
		value, _ := attributes.Get(key)
		var stringValue string
		switch value.Type() {
		case pcommon.ValueTypeStr:
			stringValue = value.Str()
		case pcommon.ValueTypeInt:
			stringValue = strconv.FormatInt(value.Int(), 10)
		case pcommon.ValueTypeBool:
			stringValue = strconv.FormatBool(value.Bool())
		}
		if stringValue != "" {
			identifiers = append(identifiers, key)
			identifiers = append(identifiers, stringValue)
		}

	}
	return strings.Join(identifiers, "|")
}

func newSamples() AllSamples {
	return AllSamples{EntitySamples: make(map[int64]EntitySamples, 0)}
}

func (allSamples *AllSamples) AddMetric(entityId int64, resource pcommon.Resource, metric pmetric.Metric) {
	if metric.Type() != pmetric.MetricTypeGauge {
		return
	}
	entitySamples := allSamples.GetOrCreateSamples(entityId)
	for i := 0; i < metric.Gauge().DataPoints().Len(); i++ {
		dp := metric.Gauge().DataPoints().At(i)
		entitySamples.AddDataPoint(resource, dp)
	}
}

func (allSamples *AllSamples) GetOrCreateSamples(entityId int64) EntitySamples {
	samples, found := allSamples.EntitySamples[entityId]
	if !found {
		samples = EntitySamples{EntityId: entityId, Samples: make(map[string]Sample, 0)}
		allSamples.EntitySamples[entityId] = samples
	}
	return samples
}

func ConvertMetrics(metrics pmetric.Metrics) AllSamples {
	allSamples := newSamples()
	if metrics.MetricCount() == 0 {
		return allSamples
	}
	if metrics.ResourceMetrics().Len() == 0 {
		return allSamples
	}
	for i := 0; i < metrics.ResourceMetrics().Len(); i++ {
		rm := metrics.ResourceMetrics().At(i)
		rawEntityId, found := rm.Resource().Attributes().Get("EntityId")
		if !found {
			continue
		}
		entityId := rawEntityId.Int()
		if rm.ScopeMetrics().Len() == 0 {
			continue
		}
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			for k := 0; k < rm.ScopeMetrics().At(j).Metrics().Len(); k++ {
				allSamples.AddMetric(entityId, rm.Resource(), rm.ScopeMetrics().At(j).Metrics().At(k))
			}
		}

	}

	return allSamples
}
