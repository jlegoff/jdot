package nrinfraexporter

import (
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"testing"
	"time"
)

func TestConvertSystemSampleOneDataPoint(t *testing.T) {
	entityId := int64(123)
	now := time.Now()

	metrics := pmetric.NewMetrics()
	resourceMetrics := metrics.ResourceMetrics().AppendEmpty()
	resourceMetrics.Resource().Attributes().PutInt("EntityId", entityId)
	ms := resourceMetrics.ScopeMetrics().AppendEmpty().Metrics()

	attrs := map[string]string{
		"attrKey": "attrValue",
	}
	addMetric(ms, "system.cpuPercent", "SystemSample", "cpuPercent",
		attrs, []TestDataPoint{{Timestamp: now, Value: 1}})
	addMetric(ms, "system.memoryUsedPercent", "SystemSample", "memoryUsedPercent",
		attrs, []TestDataPoint{{Timestamp: now, Value: 2}})

	AllSamples := ConvertMetrics(metrics)
	assert.Equal(t, 1, len(AllSamples.EntitySamples))

	entitySamples, entityFound := AllSamples.EntitySamples[entityId]
	assert.True(t, entityFound)
	assert.Equal(t, 1, len(entitySamples.Samples))

	for _, sample := range entitySamples.Samples {
		eventType, foundEventType := sample["eventType"]
		assert.True(t, foundEventType)
		assert.Equal(t, "SystemSample", eventType)

		timestamp, foundTimestamp := sample["timestamp"]
		assert.True(t, foundTimestamp)
		assert.Equal(t, timestamp.(int64), now.Unix())

		cpuSample, foundCpuSample := sample["cpuPercent"]
		assert.True(t, foundCpuSample)
		assert.Equal(t, float64(1), cpuSample)

		memorySample, foundMemorySample := sample["memoryUsedPercent"]
		assert.True(t, foundMemorySample)
		assert.Equal(t, float64(2), memorySample)

		attrSample, foundAttrSample := sample["attrKey"]
		assert.True(t, foundAttrSample)
		assert.Equal(t, "attrValue", attrSample)

		_, foundEventTypeAttr := sample["newrelic.infraEventType"]
		assert.False(t, foundEventTypeAttr)

		_, foundMetricNameAttr := sample["newrelic.infraMetricName"]
		assert.False(t, foundMetricNameAttr)
	}
}

func TestConvertSystemSampleTwoDataPoints(t *testing.T) {
	entityId := int64(123)
	timestamp2 := time.Now()
	timestamp1 := timestamp2.Add(-time.Minute)

	metrics := pmetric.NewMetrics()
	resourceMetrics := metrics.ResourceMetrics().AppendEmpty()
	resourceMetrics.Resource().Attributes().PutInt("EntityId", entityId)
	ms := resourceMetrics.ScopeMetrics().AppendEmpty().Metrics()

	attrs := map[string]string{
		"attrKey": "attrValue",
	}
	addMetric(ms, "system.cpuPercent", "SystemSample", "cpuPercent",
		attrs, []TestDataPoint{{Timestamp: timestamp1, Value: 1}, {Timestamp: timestamp2, Value: 10}})
	addMetric(ms, "system.memoryUsedPercent", "SystemSample", "memoryUsedPercent",
		attrs, []TestDataPoint{{Timestamp: timestamp1, Value: 2}, {Timestamp: timestamp2, Value: 20}})

	AllSamples := ConvertMetrics(metrics)
	assert.Equal(t, 1, len(AllSamples.EntitySamples))

	entitySamples, entityFound := AllSamples.EntitySamples[entityId]
	assert.True(t, entityFound)
	assert.Equal(t, 2, len(entitySamples.Samples))

	for _, sample := range entitySamples.Samples {
		eventType, foundEventType := sample["eventType"]
		assert.True(t, foundEventType)
		assert.Equal(t, "SystemSample", eventType)

		timestamp, foundTimestamp := sample["timestamp"]
		assert.True(t, foundTimestamp)
		assert.True(t, timestamp.(int64) == timestamp1.Unix() || timestamp.(int64) == timestamp2.Unix())

		cpuSample, foundCpuSample := sample["cpuPercent"]
		assert.True(t, foundCpuSample)

		memorySample, foundMemorySample := sample["memoryUsedPercent"]
		assert.True(t, foundMemorySample)

		if timestamp.(int64) == timestamp1.Unix() {
			assert.Equal(t, float64(2), memorySample)
			assert.Equal(t, float64(1), cpuSample)
		} else {
			assert.Equal(t, float64(20), memorySample)
			assert.Equal(t, float64(10), cpuSample)
		}

		attrSample, foundAttrSample := sample["attrKey"]
		assert.True(t, foundAttrSample)
		assert.Equal(t, "attrValue", attrSample)

		_, foundEventTypeAttr := sample["newrelic.infraEventType"]
		assert.False(t, foundEventTypeAttr)

		_, foundMetricNameAttr := sample["newrelic.infraMetricName"]
		assert.False(t, foundMetricNameAttr)
	}
}

func TestConvertTwoSamples(t *testing.T) {
	entityId := int64(123)
	now := time.Now()

	metrics := pmetric.NewMetrics()
	resourceMetrics := metrics.ResourceMetrics().AppendEmpty()
	resourceMetrics.Resource().Attributes().PutInt("EntityId", entityId)
	ms := resourceMetrics.ScopeMetrics().AppendEmpty().Metrics()

	attrs := map[string]string{
		"attrKey": "attrValue",
	}
	addMetric(ms, "system.cpuPercent", "SystemSample", "cpuPercent",
		attrs, []TestDataPoint{{Timestamp: now, Value: 1}})
	addMetric(ms, "system.memoryUsedPercent", "SystemSample", "memoryUsedPercent",
		attrs, []TestDataPoint{{Timestamp: now, Value: 2}})

	addMetric(ms, "network.receiveBytesPerSecond", "NetworkSample", "receiveBytesPerSecond",
		attrs, []TestDataPoint{{Timestamp: now, Value: 3}})
	addMetric(ms, "network.receivePacketsPerSecond", "NetworkSample", "receivePacketsPerSecond",
		attrs, []TestDataPoint{{Timestamp: now, Value: 4}})

	allSamples := ConvertMetrics(metrics)
	assert.Equal(t, 1, len(allSamples.EntitySamples))

	entitySamples, entityFound := allSamples.EntitySamples[entityId]
	assert.True(t, entityFound)
	assert.Equal(t, 2, len(entitySamples.Samples))

	foundSystemSample, foundNetworkSample := false, false

	for _, sample := range entitySamples.Samples {
		eventType, foundEventType := sample["eventType"]
		assert.True(t, foundEventType)
		assert.True(t, eventType == "SystemSample" || eventType == "NetworkSample")

		timestamp, foundTimestamp := sample["timestamp"]
		assert.True(t, foundTimestamp)
		assert.Equal(t, timestamp.(int64), now.Unix())

		if eventType == "SystemSample" {
			foundSystemSample = true
			cpuSample, foundCpuSample := sample["cpuPercent"]
			assert.True(t, foundCpuSample)
			assert.Equal(t, float64(1), cpuSample)

			memorySample, foundMemorySample := sample["memoryUsedPercent"]
			assert.True(t, foundMemorySample)
			assert.Equal(t, float64(2), memorySample)

			attrSample, foundAttrSample := sample["attrKey"]
			assert.True(t, foundAttrSample)
			assert.Equal(t, "attrValue", attrSample)
		}
		if eventType == "NetworkSample" {
			foundNetworkSample = true
			receiveBytesSample, foundReceiveBytesSample := sample["receiveBytesPerSecond"]
			assert.True(t, foundReceiveBytesSample)
			assert.Equal(t, float64(3), receiveBytesSample)

			receivePacketsSample, foundReceivePacketsSample := sample["receivePacketsPerSecond"]
			assert.True(t, foundReceivePacketsSample)
			assert.Equal(t, float64(4), receivePacketsSample)

			attrSample, foundAttrSample := sample["attrKey"]
			assert.True(t, foundAttrSample)
			assert.Equal(t, "attrValue", attrSample)
		}

		_, foundEventTypeAttr := sample["newrelic.infraEventType"]
		assert.False(t, foundEventTypeAttr)

		_, foundMetricNameAttr := sample["newrelic.infraMetricName"]
		assert.False(t, foundMetricNameAttr)
	}

	assert.True(t, foundSystemSample)
	assert.True(t, foundNetworkSample)
}

func addMetric(metricSlice pmetric.MetricSlice, metricName string, infraEventType string, infraMetricName string,
	attributes map[string]string, datapoints []TestDataPoint) {
	metric := metricSlice.AppendEmpty()
	metric.SetName(metricName)
	mdp := metric.SetEmptyGauge().DataPoints()
	for _, testDp := range datapoints {
		dp := mdp.AppendEmpty()
		dp.Attributes().PutStr("newrelic.infraEventType", infraEventType)
		dp.Attributes().PutStr("newrelic.infraMetricName", infraMetricName)

		for key, value := range attributes {
			dp.Attributes().PutStr(key, value)
		}
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(testDp.Timestamp.Unix(), 0)))
		dp.SetDoubleValue(testDp.Value)
	}
}

type TestDataPoint struct {
	Timestamp time.Time
	Value     float64
}
