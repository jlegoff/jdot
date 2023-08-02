package apmconnector

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type ApmConnector struct {
	config *Config
	logger *zap.Logger

	metricsConsumer consumer.Metrics
	logsConsumer    consumer.Logs
}

func (c *ApmConnector) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (c *ApmConnector) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	if c.metricsConsumer != nil {
		metrics := ConvertTraces(c.logger, td)
		err := c.metricsConsumer.ConsumeMetrics(ctx, metrics)
		if err != nil {
			return err
		}
	}
	if c.logsConsumer != nil {
		logs := BuildTransactions(td)
		return c.logsConsumer.ConsumeLogs(ctx, logs)
	}
	return nil
}

func (c *ApmConnector) Start(_ context.Context, host component.Host) error {
	c.logger.Info("Starting the APM Connector")
	return nil
}

func (c *ApmConnector) Shutdown(context.Context) error {
	c.logger.Info("Stopping the APM Connector")
	return nil
}

func GetSdkLanguage(attributes pcommon.Map) string {
	sdkLanguage, sdkLanguagePresent := attributes.Get("telemetry.sdk.language")
	if sdkLanguagePresent {
		return sdkLanguage.AsString()
	}
	return "unknown"
}

type Transaction struct {
	SdkLanguage         string
	SpanToChildDuration map[string]int64
	ScopeMetric         pmetric.ScopeMetrics
	Measurements        []Measurement
}

type SegmentNameProvider interface {
	SegmentName(transactionType string)
}

type Measurement struct {
	metricName          string
	durationNanos       int64
	attributes          pcommon.Map
	segmentNameProvider SegmentNameProvider
	metricTimesliceName string
}

func ConvertTraces(logger *zap.Logger, td ptrace.Traces) pmetric.Metrics {
	fmt.Printf("BATCH\n")
	transactions := make(map[string]Transaction)
	metrics := pmetric.NewMetrics()
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		resourceMetrics := metrics.ResourceMetrics().AppendEmpty()
		rs := td.ResourceSpans().At(i)
		instrumentationProvider, instrumentationProviderPresent := rs.Resource().Attributes().Get("instrumentation.provider")
		if instrumentationProviderPresent && instrumentationProvider.AsString() != "opentelemetry" {
			logger.Debug("Skipping resource spans", zap.String("instrumentation.provider", instrumentationProvider.AsString()))
			continue
		}
		rs.Resource().CopyTo(resourceMetrics.Resource())
		sdkLanguage := GetSdkLanguage(rs.Resource().Attributes())
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			scopeSpan := rs.ScopeSpans().At(j)
			scopeMetric := resourceMetrics.ScopeMetrics().AppendEmpty()
			for k := 0; k < scopeSpan.Spans().Len(); k++ {
				span := scopeSpan.Spans().At(k)
				transaction := GetOrCreateTransaction(transactions, sdkLanguage, span, scopeMetric)
				AddSpan(transaction, span)

				fmt.Printf("Span kind: %s Name: %s Trace Id: %s Span id: %s\n", span.Kind(), span.Name(), span.TraceID().String(), span.SpanID().String())
				/*
					if span.Kind() == ptrace.SpanKindServer {
						ProcessServerSpan(span, scopeMetric, sdkLanguage)
					} else if span.Kind() == ptrace.SpanKindClient {
						// filter out db calls that have no parent (so no transaction)
						if !span.ParentSpanID().IsEmpty() {
							ProcessClientSpan(span, scopeMetric, sdkLanguage)
						}
					}*/
			}
		}

	}
	return metrics
}

func GetOrCreateTransaction(transactions map[string]Transaction, sdkLanguage string, span ptrace.Span, scopeMetric pmetric.ScopeMetrics) Transaction {
	transaction, txExists := transactions[span.TraceID().String()]
	if !txExists {
		transaction = Transaction{SdkLanguage: sdkLanguage, SpanToChildDuration: make(map[string]int64), ScopeMetric: scopeMetric, Measurements: []Measurement{}}
		transactions[span.TraceID().String()] = transaction
		fmt.Printf("Created transaction for: %s\n", span.TraceID().String())
	}
	return transaction
}

func AddSpan(transaction Transaction, span ptrace.Span) {
	if span.Kind() == ptrace.SpanKindServer {
		ProcessServerSpan(transaction, span)
	} else {
		duration := span.EndTimestamp() - span.StartTimestamp()
		childDuration, exists := transaction.SpanToChildDuration[span.ParentSpanID().String()]
		if !exists {
			childDuration = 0
		}
		childDuration += duration.AsTime().UnixNano()
		transaction.SpanToChildDuration[span.ParentSpanID().String()] = childDuration

		if span.Kind() == ptrace.SpanKindClient {
			// filter out db calls that have no parent (so no transaction)
			if !span.ParentSpanID().IsEmpty() {
				//ProcessClientSpan(span, scopeMetric, sdkLanguage)
			}
		}
	}
}

func ProcessDatabaseSpan(span ptrace.Span, scopeMetric pmetric.ScopeMetrics, sdkLanguage string) bool {
	dbSystem, dbSystemPresent := span.Attributes().Get("db.system")
	if dbSystemPresent {
		dbOperation, dbOperationPresent := span.Attributes().Get("db.operation")
		if dbOperationPresent {
			dbTable, dbTablePresent := span.Attributes().Get("db.sql.table")
			if dbTablePresent {
				metric := AddMetric(scopeMetric.Metrics(), "apm.service.datastore.operation.duration")
				dp := SetHistogramFromSpan(span, metric)
				span.Attributes().CopyTo(dp.Attributes())
				dp.Attributes().PutStr("db.operation", dbOperation.AsString())
				dp.Attributes().PutStr("db.system", dbSystem.AsString())
				dp.Attributes().PutStr("db.sql.table", dbTable.AsString())
				return true
			}
		}
	}
	return false
}

func ProcessExternalSpan(span ptrace.Span, scopeMetric pmetric.ScopeMetrics, sdkLanguage string) bool {
	serverAddress, serverAddressPresent := span.Attributes().Get("server.address")
	if serverAddressPresent {
		metric := AddMetric(scopeMetric.Metrics(), "apm.service.transaction.external.duration")
		dp := SetHistogramFromSpan(span, metric)
		span.Attributes().CopyTo(dp.Attributes())
		dp.Attributes().PutStr("external.host", serverAddress.AsString())

		// FIXME
		dp.Attributes().PutStr("transactionType", "Web")
		return true
	}
	return false
}

func ProcessClientSpan(span ptrace.Span, scopeMetric pmetric.ScopeMetrics, sdkLanguage string) {
	if !ProcessDatabaseSpan(span, scopeMetric, sdkLanguage) {
		ProcessExternalSpan(span, scopeMetric, sdkLanguage)
	}
}

func ProcessServerSpan(transaction Transaction, span ptrace.Span) {

	metric := AddMetric(transaction.ScopeMetric.Metrics(), "apm.service.transaction.duration")
	dp := SetHistogramFromSpan(span, metric)
	span.Attributes().CopyTo(dp.Attributes())
	dp.Attributes().PutStr("transactionType", "Web")
	transactionName := GetTransactionMetricName(span)
	dp.Attributes().PutStr("transactionName", transactionName)

	overviewWeb := AddMetric(transaction.ScopeMetric.Metrics(), "apm.service.overview.web")
	overviewDp := SetHistogramFromSpan(span, overviewWeb)
	span.Attributes().CopyTo(overviewDp.Attributes())

	overviewDp.Attributes().PutStr("segmentName", transaction.SdkLanguage)

	txBreakdownMetric := AddMetric(transaction.ScopeMetric.Metrics(), "apm.service.transaction.overview")
	txBreakdownDp := SetHistogramFromSpan(span, txBreakdownMetric)
	span.Attributes().CopyTo(txBreakdownDp.Attributes())
	txBreakdownDp.Attributes().PutStr("metricTimesliceName", transaction.SdkLanguage)
	txBreakdownDp.Attributes().PutStr("transactionName", transactionName)
}

func AddMetric(metrics pmetric.MetricSlice, metricName string) pmetric.Metric {
	metric := metrics.AppendEmpty()
	metric.SetName(metricName)
	metric.SetUnit("s")
	return metric
}

func SetHistogramFromSpan(span ptrace.Span, metric pmetric.Metric) pmetric.HistogramDataPoint {
	histogram := metric.SetEmptyHistogram()
	histogram.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	dp := histogram.DataPoints().AppendEmpty()
	dp.SetStartTimestamp(span.StartTimestamp())
	dp.SetTimestamp(span.EndTimestamp())

	duration := float64((span.EndTimestamp() - span.StartTimestamp()).AsTime().UnixNano()) / 1e9
	dp.SetSum(duration)
	dp.SetCount(1)
	dp.SetMin(duration)
	dp.SetMax(duration)
	return dp
}

func GetTransactionMetricName(span ptrace.Span) string {
	httpRoute, routePresent := span.Attributes().Get("http.route")
	if routePresent {
		// http.request.method
		method, methodPresent := span.Attributes().Get("http.method")
		// http.route starts with a /
		if methodPresent {
			return fmt.Sprintf("WebTransaction/http.route%s (%s)", httpRoute.Str(), method.Str())
		} else {
			return fmt.Sprintf("WebTransaction/http.route%s", httpRoute.Str())
		}
	}
	return "WebTransaction/Other/unknown"
}
