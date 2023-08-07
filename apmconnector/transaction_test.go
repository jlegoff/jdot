package apmconnector

import (
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"testing"
)

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
	scopeMetrics := ScopeMetrics{metrics: make(map[string]*Metric)}

	transaction, _ := transactions.GetOrCreateTransaction("java", span, &scopeMetrics)

	transaction.SetRootSpan(span)
	assert.Equal(t, true, transaction.IsRootSet())
	transactions.ProcessTransactions()

	existingTransaction, _ := transactions.GetOrCreateTransaction("java", span, &scopeMetrics)
	assert.Equal(t, transaction, existingTransaction)
	assert.Equal(t, true, existingTransaction.IsRootSet())
}