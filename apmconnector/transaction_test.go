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
	m.PutStr("stuff", "meh")
	m.PutDouble("process.pid", 1)
	filtered := FilterAttributes(m)

	assert.Equal(t, 2, len(filtered.AsRaw()))
	name, exists := filtered.Get("service.name")
	assert.Equal(t, true, exists)
	assert.Equal(t, "MyApp", name.AsString())
}

func TestGetTransactionMetricNamUnknown(t *testing.T) {
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
