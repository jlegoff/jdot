package apmconnector

import (
	"fmt"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type Transaction struct {
	sdkLanguage         string
	spanToChildDuration map[string]int64
	metricSlice         pmetric.MetricSlice
	measurements        map[string]Measurement
	rootSpan            ptrace.Span
}

type Measurement struct {
	spanId                 string
	metricName             string
	durationNanos          int64
	exclusiveDurationNanos int64
	attributes             pcommon.Map
	segmentNameProvider    func(string) string
	metricTimesliceName    string
	// FIXME
	span ptrace.Span
}

func GetOrCreateTransaction(transactions map[string]Transaction, sdkLanguage string, span ptrace.Span, metricSlice pmetric.MetricSlice) *Transaction {
	traceID := span.TraceID().String()
	transaction, txExists := transactions[traceID]
	if !txExists {
		transaction = Transaction{sdkLanguage: sdkLanguage, spanToChildDuration: make(map[string]int64),
			metricSlice: metricSlice, measurements: make(map[string]Measurement)}
		transactions[traceID] = transaction
		//fmt.Printf("Created transaction for: %s   %s\n", traceID, transaction.sdkLanguage)
	}

	return &transaction
}

func (transaction *Transaction) SetRootSpan(span ptrace.Span) {
	transaction.rootSpan = span
}

func (transaction *Transaction) AddSpan(span ptrace.Span) {
	if span.Kind() == ptrace.SpanKindServer {
		transaction.SetRootSpan(span)
	} else {
		parentSpanID := span.ParentSpanID().String()
		childDuration, exists := transaction.spanToChildDuration[parentSpanID]
		newDuration := DurationInNanos(span)
		if exists {
			transaction.spanToChildDuration[parentSpanID] = (childDuration + newDuration)
		} else {
			transaction.spanToChildDuration[parentSpanID] = newDuration
		}

		if span.Kind() == ptrace.SpanKindClient {
			// filter out db calls that have no parent (so no transaction)
			if !span.ParentSpanID().IsEmpty() {
				transaction.ProcessClientSpan(span)
			}
		} else {
			transaction.ProcessGenericSpan(span)
		}
	}
}

func (transaction *Transaction) ProcessDatabaseSpan(span ptrace.Span) bool {
	dbSystem, dbSystemPresent := span.Attributes().Get("db.system")
	if dbSystemPresent {
		dbOperation, dbOperationPresent := span.Attributes().Get("db.operation")
		if dbOperationPresent {
			dbTable, dbTablePresent := span.Attributes().Get("db.sql.table")
			if dbTablePresent {
				attributes := pcommon.NewMap()
				//span.Attributes().CopyTo(attributes)
				attributes.PutStr("db.operation", dbOperation.AsString())
				attributes.PutStr("db.system", dbSystem.AsString())
				attributes.PutStr("db.sql.table", dbTable.AsString())

				segmentNameProvider := func(t string) string { return dbSystem.AsString() }
				timesliceName := fmt.Sprintf("Datastore/statement/%s/%s/%s", dbSystem.AsString(), dbTable.AsString(), dbOperation.AsString())
				measurement := Measurement{spanId: span.SpanID().String(), metricName: "apm.service.datastore.operation.duration", span: span,
					durationNanos: DurationInNanos(span), attributes: attributes, segmentNameProvider: segmentNameProvider, metricTimesliceName: timesliceName}

				transaction.measurements[measurement.spanId] = measurement

				return true
			}
		}
	}
	return false
}

func (transaction *Transaction) ProcessExternalSpan(span ptrace.Span) bool {
	serverAddress, serverAddressPresent := span.Attributes().Get("server.address")
	if serverAddressPresent {
		metric := AddMetric(transaction.metricSlice, "apm.service.transaction.external.duration")
		dp := SetHistogramFromSpan(metric, span)
		span.Attributes().CopyTo(dp.Attributes())
		dp.Attributes().PutStr("external.host", serverAddress.AsString())

		// FIXME
		//dp.Attributes().PutStr("transactionType", "Web")

		return true
	}
	return false
}

func (transaction *Transaction) ProcessGenericSpan(span ptrace.Span) bool {
	segmentNameProvider := func(t string) string { return transaction.sdkLanguage }
	attributes := pcommon.NewMap()
	timesliceName := fmt.Sprintf("Custom/%s", span.Name())
	measurement := Measurement{spanId: span.SpanID().String(), metricName: "newrelic.timeslice.value", span: span,
		durationNanos: DurationInNanos(span), attributes: attributes, segmentNameProvider: segmentNameProvider, metricTimesliceName: timesliceName}

	transaction.measurements[measurement.spanId] = measurement

	return true
}

func (transaction *Transaction) ProcessClientSpan(span ptrace.Span) bool {
	if !transaction.ProcessDatabaseSpan(span) {
		return transaction.ProcessExternalSpan(span)
	}
	return false
}

func (transaction *Transaction) ProcessServerSpan(span ptrace.Span) {

	metric := AddMetric(transaction.metricSlice, "apm.service.transaction.duration")
	dp := SetHistogramFromSpan(metric, span)
	span.Attributes().CopyTo(dp.Attributes())

	// FIXME
	transactionType := "Web"

	dp.Attributes().PutStr("transactionType", transactionType)
	transactionName := GetTransactionMetricName(span)
	dp.Attributes().PutStr("transactionName", transactionName)

	breakdownBySegment := make(map[string]int64)
	totalBreakdownNanos := int64(0)
	for _, measurement := range transaction.measurements {
		transaction.ProcessMeasurement(&measurement, transactionType, transactionName)
		segmentName := measurement.segmentNameProvider(transactionType)
		segmentSum, exists := breakdownBySegment[segmentName]
		if exists {
			breakdownBySegment[segmentName] = segmentSum + measurement.exclusiveDurationNanos
		} else {
			breakdownBySegment[segmentName] = measurement.exclusiveDurationNanos
		}
		totalBreakdownNanos += measurement.exclusiveDurationNanos
	}

	remainingNanos := DurationInNanos(span) - totalBreakdownNanos
	if remainingNanos > 0 {
		vmTime, vmTimeExists := breakdownBySegment[transaction.sdkLanguage]
		if vmTimeExists {
			breakdownBySegment[transaction.sdkLanguage] = vmTime + remainingNanos
		} else {
			breakdownBySegment[transaction.sdkLanguage] = vmTime
		}
	}

	// FIXME
	overviewMetricName := "apm.service.overview.web"

	for segment, sum := range breakdownBySegment {
		overviewMetric := AddMetric(transaction.metricSlice, overviewMetricName)
		overviewDp := SetHistogram(overviewMetric, span.StartTimestamp(), span.EndTimestamp(), sum)
		span.Attributes().CopyTo(overviewDp.Attributes())

		overviewDp.Attributes().PutStr("segmentName", segment)
	}
}

func (transaction *Transaction) ProcessMeasurement(measurement *Measurement, transactionType string, transactionName string) {
	exclusiveDuration := transaction.ExclusiveTime(*measurement)
	measurement.exclusiveDurationNanos = exclusiveDuration
	measurement.attributes.PutStr("metricTimesliceName", measurement.metricTimesliceName)
	//	fmt.Printf("Name: %s total: %d exclusive: %d    id:%s\n", measurement.metricName, measurement.durationNanos, exclusiveDuration, measurement.spanId)

	metric := AddMetric(transaction.metricSlice, measurement.metricName)
	metricDp := SetHistogramFromSpan(metric, measurement.span)
	measurement.attributes.CopyTo(metricDp.Attributes())

	overviewMetric := AddMetric(transaction.metricSlice, "apm.service.transaction.overview")
	overviewMetricDp := SetHistogram(overviewMetric, measurement.span.StartTimestamp(), measurement.span.EndTimestamp(), exclusiveDuration)
	measurement.attributes.PutStr("transactionName", transactionName)
	measurement.attributes.PutStr("scope", transactionName)
	measurement.attributes.PutStr("transactionType", transactionType)

	measurement.attributes.CopyTo(overviewMetricDp.Attributes())
}

func DurationInNanos(span ptrace.Span) int64 {
	return (span.EndTimestamp() - span.StartTimestamp()).AsTime().UnixNano()
}

func (transaction *Transaction) ExclusiveTime(measurement Measurement) int64 {
	duration, exists := transaction.spanToChildDuration[measurement.spanId]
	if !exists {
		return measurement.durationNanos
	}
	return measurement.durationNanos - duration
}

func AddMetric(metrics pmetric.MetricSlice, metricName string) pmetric.Metric {
	metric := metrics.AppendEmpty()
	metric.SetName(metricName)
	metric.SetUnit("s")
	return metric
}

func NanosToSeconds(nanos int64) float64 {
	return float64(nanos) / 1e9
}

func SetHistogramFromSpan(metric pmetric.Metric, span ptrace.Span) pmetric.HistogramDataPoint {
	return SetHistogram(metric, span.StartTimestamp(), span.EndTimestamp(), (span.EndTimestamp() - span.StartTimestamp()).AsTime().UnixNano())
}

func SetHistogram(metric pmetric.Metric, startTimestamp pcommon.Timestamp, endTimestamp pcommon.Timestamp, durationNanos int64) pmetric.HistogramDataPoint {
	histogram := metric.SetEmptyHistogram()
	histogram.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	dp := histogram.DataPoints().AppendEmpty()
	dp.SetStartTimestamp(startTimestamp)
	dp.SetTimestamp(endTimestamp)

	duration := NanosToSeconds(durationNanos)
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
