package apmconnector

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
)

type AttributeFilter struct {
	attributesToKeep []string
}

func NewAttributeFilter() *AttributeFilter {
	return &AttributeFilter{attributesToKeep: []string{"os.description", "telemetry.auto.version", "telemetry.sdk.language", "host.name",
		"os.type", "telemetry.sdk.name", "process.runtime.description", "process.runtime.version", "telemetry.sdk.version",
		"host.arch", "service.name", "service.instance.id"}}
}

func (attributeFilter *AttributeFilter) FilterAttributes(from pcommon.Map) pcommon.Map {
	f := from.AsRaw()
	m := make(map[string]any)
	for _, k := range attributeFilter.attributesToKeep {
		if v, exists := f[k]; exists {
			m[k] = v
		}
	}
	newMap := pcommon.NewMap()
	newMap.FromRaw(m)
	if hostName, exists := from.Get("host.name"); exists {
		newMap.PutStr("host", hostName.AsString())

		if _, e := newMap.Get("service.instance.id"); !e {
			newMap.PutStr("service.instance.id", hostName.AsString())
		}
	}
	return newMap
}
