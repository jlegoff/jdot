package apmconnector

import (
	"github.com/stretchr/testify/assert"
	//	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"testing"
)

func TestGetTransactionMetricNamUnknown(t *testing.T) {
	span := ptrace.NewSpan()

	name := GetTransactionMetricName(span)
	assert.Equal(t, "WebTransaction/Other/unknown", name.Name)
	assert.Equal(t, WebTransactionType, name.TransactionType)
}

func TestGetTransactionMetricNamRoute(t *testing.T) {
	span := ptrace.NewSpan()
	span.Attributes().PutStr("http.route", "/users")

	name := GetTransactionMetricName(span)
	assert.Equal(t, "WebTransaction/http.route/users", name.Name)
	assert.Equal(t, WebTransactionType, name.TransactionType)
}

func TestGetTransactionMetricNamUrlPath(t *testing.T) {
	span := ptrace.NewSpan()
	span.Attributes().PutStr("url.path", "/owners/5")

	name := GetTransactionMetricName(span)
	assert.Equal(t, "WebTransaction/Uri/owners/5", name.Name)
	assert.Equal(t, WebTransactionType, name.TransactionType)
}
