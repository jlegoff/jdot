package apmconnector

import (
	"github.com/stretchr/testify/assert"
	//	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"testing"
)

func TestFilterAttributes(t *testing.T) {
	m := pcommon.NewMap()
	m.PutStr("service.name", "MyApp")
	m.PutStr("os.type", "linux")
	m.PutStr("host.name", "loki")
	m.PutStr("stuff", "meh")
	m.PutDouble("process.pid", 1)
	filtered := FilterAttributes(m)

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
	filtered := FilterAttributes(m)

	assert.Equal(t, 4, len(filtered.AsRaw()))
	{
		instanceId, exists := filtered.Get("service.instance.id")
		assert.Equal(t, true, exists)
		assert.Equal(t, "839944", instanceId.AsString())
	}
}

func TestGetTransactionMetricNameUnknown(t *testing.T) {
	span := ptrace.NewSpan()

	name, txType := GetTransactionMetricName(span)
	assert.Equal(t, "WebTransaction/Other/unknown", name)
	assert.Equal(t, WebTransactionType, txType)
	assert.Equal(t, "Web", txType.AsString())
}

func TestGetTransactionMetricNamRoute(t *testing.T) {
	span := ptrace.NewSpan()
	span.Attributes().PutStr("http.route", "/users")

	name, txType := GetTransactionMetricName(span)
	assert.Equal(t, "WebTransaction/http.route/users", name)
	assert.Equal(t, WebTransactionType, txType)
}

func TestGetTransactionMetricNamUrlPath(t *testing.T) {
	span := ptrace.NewSpan()
	span.Attributes().PutStr("url.path", "/owners/5")

	name, txType := GetTransactionMetricName(span)
	assert.Equal(t, "WebTransaction/Uri/owners/5", name)
	assert.Equal(t, WebTransactionType, txType)
}

func TestGetOrCreateTransaction(t *testing.T) {
	transactions := NewTransactionsMap()
	span := ptrace.NewSpan()
	var metrics pmetric.MetricSlice = pmetric.NewMetricSlice()
	transaction, _ := transactions.GetOrCreateTransaction("java", span, metrics)

	transaction.SetRootSpan(span)
	assert.Equal(t, true, transaction.IsRootSet())
	transactions.ProcessTransactions()

	existingTransaction, _ := transactions.GetOrCreateTransaction("java", span, metrics)
	assert.Equal(t, transaction, existingTransaction)
	assert.Equal(t, true, existingTransaction.IsRootSet())
}
