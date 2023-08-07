package apmconnector

import (
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"testing"
)

func TestFilterAttributes(t *testing.T) {
	m := pcommon.NewMap()
	m.PutStr("service.name", "MyApp")
	m.PutStr("os.type", "linux")
	m.PutStr("host.name", "loki")
	m.PutStr("stuff", "meh")
	m.PutDouble("process.pid", 1)
	filtered := NewAttributeFilter().FilterAttributes(m)

	assert.Equal(t, 5, len(filtered.AsRaw()))
	{
		name, exists := filtered.Get("service.name")
		assert.Equal(t, true, exists)
		assert.Equal(t, "MyApp", name.AsString())
	}
	{
		instanceId, exists := filtered.Get("service.instance.id")
		assert.Equal(t, true, exists)
		assert.Equal(t, "loki", instanceId.AsString())
	}
	{
		host, exists := filtered.Get("host")
		assert.Equal(t, true, exists)
		assert.Equal(t, "loki", host.AsString())
	}
}

func TestFilterAttributesInstancePresent(t *testing.T) {
	m := pcommon.NewMap()
	m.PutStr("service.name", "MyApp")
	m.PutStr("host.name", "loki")
	m.PutStr("service.instance.id", "839944")
	m.PutDouble("process.pid", 1)
	filtered := NewAttributeFilter().FilterAttributes(m)

	assert.Equal(t, 4, len(filtered.AsRaw()))
	{
		instanceId, exists := filtered.Get("service.instance.id")
		assert.Equal(t, true, exists)
		assert.Equal(t, "839944", instanceId.AsString())
	}
}
