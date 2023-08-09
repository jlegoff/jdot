package apmconnector

import (
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"testing"
)

func TestGetOrCreateResourceMetrics(t *testing.T) {
	meter := NewMeterProvider()
	attributes := pcommon.NewMap()
	attributes.PutStr("name", "test")
	attributes.PutInt("id", 5)
	metrics := meter.getOrCreateResourceMetrics(attributes)

	attributes = pcommon.NewMap()
	attributes.PutInt("id", 5)
	attributes.PutStr("name", "test")
	metrics2 := meter.getOrCreateResourceMetrics(attributes)
	assert.Equal(t, metrics, metrics2)
}
