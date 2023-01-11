package nrinfrareceiver

import (
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"
)

func TestConvertRealBatch(t *testing.T) {
	filePath := filepath.Join("testdata", "agent_host_metrics_batch.json")
	bytes, fileErr := ioutil.ReadFile(filePath)
	if fileErr != nil {
		panic(fileErr)
	}
	content := strings.ReplaceAll(string(bytes), "\n", "")
	metrics := ConvertLine([]byte(content))
	assert.Equal(t, 240, metrics.MetricCount())
	assert.Equal(t, 1, metrics.ResourceMetrics().Len())
}

func TestConvertOneEvent(t *testing.T) {
	event := make(RawEvent)
	event["eventType"] = "SystemSample"
	event["timestamp"] = float64(1673362310)
	event["entityKey"] = "key"
	event["metric1"] = 1
	event["metric2"] = 2
	event["attribute"] = "value"

	allEvents := []RawEvent{event}
	rawEntities := RawEntities{
		EntityID:  123,
		IsAgent:   true,
		RawEvents: allEvents,
	}
	metrics := ConvertFromRawEntity([]RawEntities{rawEntities})
	assert.Equal(t, metrics.MetricCount(), 2)

	// Check resource
	// one event -> one resource
	assert.Equal(t, 1, metrics.ResourceMetrics().Len())
	resource := metrics.ResourceMetrics().At(0).Resource()
	assert.Equal(t, 1, resource.Attributes().Len())
	resourceEntityId, entityIdPresent := resource.Attributes().Get("EntityId")
	assert.Equal(t, true, entityIdPresent)
	assert.Equal(t, int64(123), resourceEntityId.Int())

	// Check metrics
	assert.Equal(t, 1, metrics.ResourceMetrics().At(0).ScopeMetrics().Len())
	assert.Equal(t, 2, metrics.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().Len())
	individualMetrics := metrics.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
	individualMetrics.Sort(sortMetrics)
	assert.Equal(t, 2, individualMetrics.Len())

	// metric 1
	assert.Equal(t, "system.metric1", individualMetrics.At(0).Name())
	assert.Equal(t, 1, individualMetrics.At(0).Gauge().DataPoints().Len())
	dp1 := individualMetrics.At(0).Gauge().DataPoints().At(0)
	assert.Equal(t, float64(1), dp1.DoubleValue())
	assert.Equal(t, int64(1673362310000000000), dp1.Timestamp().AsTime().UnixNano())
	assert.Equal(t, 1, dp1.Attributes().Len())
	attr, attrPresent := dp1.Attributes().Get("attribute")
	assert.Equal(t, true, attrPresent)
	assert.Equal(t, "value", attr.Str())

	// metric 2
	assert.Equal(t, "system.metric2", individualMetrics.At(1).Name())
	assert.Equal(t, 1, individualMetrics.At(1).Gauge().DataPoints().Len())
	dp2 := individualMetrics.At(1).Gauge().DataPoints().At(0)
	assert.Equal(t, float64(2), dp2.DoubleValue())
	assert.Equal(t, int64(1673362310000000000), dp2.Timestamp().AsTime().UnixNano())
	assert.Equal(t, 1, dp2.Attributes().Len())
	attr2, attr2Present := dp2.Attributes().Get("attribute")
	assert.Equal(t, true, attr2Present)
	assert.Equal(t, "value", attr2.Str())
}

func sortMetrics(a, b pmetric.Metric) bool {
	return a.Name() <= b.Name()
}
