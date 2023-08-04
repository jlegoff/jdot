package apmconnector

import (
	"fmt"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type TransactionType string

const (
	WebTransactionType   TransactionType = "Web"
	OtherTransactionType TransactionType = "Other"
)

func (t TransactionType) AsString() string {
	return fmt.Sprintf("%s", t)
}

type TransactionName struct {
	Name            string
	TransactionType TransactionType
}

type Transaction struct {
	SdkLanguage         string
	SpanToChildDuration map[string]int64
	MetricSlice         pmetric.MetricSlice
	Measurements        map[string]Measurement
	RootSpan            ptrace.Span
}

type Measurement struct {
	SpanId                 string
	MetricName             string
	DurationNanos          int64
	ExclusiveDurationNanos int64
	Attributes             pcommon.Map
	SegmentNameProvider    func(TransactionType) string
	MetricTimesliceName    string
	// FIXME
	Span ptrace.Span
}

func GetOrCreateTransaction(transactions map[string]Transaction, sdkLanguage string, span ptrace.Span, metricSlice pmetric.MetricSlice) *Transaction {
	traceID := span.TraceID().String()
	transaction, txExists := transactions[traceID]
	if !txExists {
		transaction = Transaction{SdkLanguage: sdkLanguage, SpanToChildDuration: make(map[string]int64),
			MetricSlice: metricSlice, Measurements: make(map[string]Measurement)}
		transactions[traceID] = transaction
		//fmt.Printf("Created transaction for: %s   %s\n", traceID, transaction.sdkLanguage)
	}

	return &transaction
}

func (transaction *Transaction) SetRootSpan(span ptrace.Span) {
	transaction.RootSpan = span
}

func (transaction *Transaction) AddSpan(span ptrace.Span) {
	if span.Kind() == ptrace.SpanKindServer {
		transaction.SetRootSpan(span)
	} else {
		parentSpanID := span.ParentSpanID().String()
		newDuration := DurationInNanos(span)
		transaction.SpanToChildDuration[parentSpanID] += newDuration
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

				segmentNameProvider := func(t TransactionType) string { return dbSystem.AsString() }
				timesliceName := fmt.Sprintf("Datastore/statement/%s/%s/%s", dbSystem.AsString(), dbTable.AsString(), dbOperation.AsString())
				measurement := Measurement{SpanId: span.SpanID().String(), MetricName: "apm.service.datastore.operation.duration", Span: span,
					DurationNanos: DurationInNanos(span), Attributes: attributes, SegmentNameProvider: segmentNameProvider, MetricTimesliceName: timesliceName}

				transaction.Measurements[measurement.SpanId] = measurement

				return true
			}
		}
	}
	return false
}

func (transaction *Transaction) ProcessExternalSpan(span ptrace.Span) bool {
	serverAddress, serverAddressPresent := span.Attributes().Get("server.address")
	if serverAddressPresent {
		metric := AddMetric(transaction.MetricSlice, "apm.service.transaction.external.duration")
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
	segmentNameProvider := func(t TransactionType) string { return transaction.SdkLanguage }
	attributes := pcommon.NewMap()
	timesliceName := fmt.Sprintf("Custom/%s", span.Name())
	measurement := Measurement{SpanId: span.SpanID().String(), MetricName: "newrelic.timeslice.value", Span: span,
		DurationNanos: DurationInNanos(span), Attributes: attributes, SegmentNameProvider: segmentNameProvider, MetricTimesliceName: timesliceName}

	transaction.Measurements[measurement.SpanId] = measurement

	return true
}

func (transaction *Transaction) ProcessClientSpan(span ptrace.Span) bool {
	if !transaction.ProcessDatabaseSpan(span) {
		return transaction.ProcessExternalSpan(span)
	}
	return false
}

func (transaction *Transaction) ProcessServerSpan() {
	if (ptrace.Span{}) == transaction.RootSpan {
		// root span is not set
		return
	}
	span := transaction.RootSpan
	metric := AddMetric(transaction.MetricSlice, "apm.service.transaction.duration")
	dp := SetHistogramFromSpan(metric, span)
	span.Attributes().CopyTo(dp.Attributes())

	fullTransactionName := GetTransactionMetricName(span)
	transactionName := fullTransactionName.Name
	transactionType := fullTransactionName.TransactionType
	dp.Attributes().PutStr("transactionType", transactionType.AsString())

	dp.Attributes().PutStr("transactionName", transactionName)

	breakdownBySegment := make(map[string]int64)
	totalBreakdownNanos := int64(0)
	for _, measurement := range transaction.Measurements {
		transaction.ProcessMeasurement(&measurement, transactionType, transactionName)
		segmentName := measurement.SegmentNameProvider(transactionType)
		breakdownBySegment[segmentName] += measurement.ExclusiveDurationNanos
		totalBreakdownNanos += measurement.ExclusiveDurationNanos
	}

	remainingNanos := DurationInNanos(span) - totalBreakdownNanos
	if remainingNanos > 0 {
		breakdownBySegment[transaction.SdkLanguage] += remainingNanos
	}

	// FIXME
	overviewMetricName := "apm.service.overview.web"

	for segment, sum := range breakdownBySegment {
		overviewMetric := AddMetric(transaction.MetricSlice, overviewMetricName)
		overviewDp := SetHistogram(overviewMetric, span.StartTimestamp(), span.EndTimestamp(), sum)
		span.Attributes().CopyTo(overviewDp.Attributes())

		overviewDp.Attributes().PutStr("segmentName", segment)
	}
}

func (transaction *Transaction) ProcessMeasurement(measurement *Measurement, transactionType TransactionType, transactionName string) {
	exclusiveDuration := transaction.ExclusiveTime(*measurement)
	measurement.ExclusiveDurationNanos = exclusiveDuration
	measurement.Attributes.PutStr("metricTimesliceName", measurement.MetricTimesliceName)
	//	fmt.Printf("Name: %s total: %d exclusive: %d    id:%s\n", measurement.metricName, measurement.durationNanos, exclusiveDuration, measurement.spanId)

	metric := AddMetric(transaction.MetricSlice, measurement.MetricName)
	metricDp := SetHistogramFromSpan(metric, measurement.Span)
	measurement.Attributes.CopyTo(metricDp.Attributes())

	overviewMetric := AddMetric(transaction.MetricSlice, "apm.service.transaction.overview")
	overviewMetricDp := SetHistogram(overviewMetric, measurement.Span.StartTimestamp(), measurement.Span.EndTimestamp(), exclusiveDuration)
	measurement.Attributes.PutStr("transactionName", transactionName)
	measurement.Attributes.PutStr("scope", transactionName)
	measurement.Attributes.PutStr("transactionType", transactionType.AsString())

	measurement.Attributes.CopyTo(overviewMetricDp.Attributes())
}

func DurationInNanos(span ptrace.Span) int64 {
	return (span.EndTimestamp() - span.StartTimestamp()).AsTime().UnixNano()
}

func (transaction *Transaction) ExclusiveTime(measurement Measurement) int64 {
	return measurement.DurationNanos - transaction.SpanToChildDuration[measurement.SpanId]
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

func GetTransactionMetricName(span ptrace.Span) TransactionName {
	httpRoute, routePresent := span.Attributes().Get("http.route")
	if routePresent {
		// http.request.method
		method, methodPresent := span.Attributes().Get("http.method")
		// http.route starts with a /
		if methodPresent {
			return TransactionName{Name: fmt.Sprintf("WebTransaction/http.route%s (%s)", httpRoute.Str(), method.Str()), TransactionType: WebTransactionType}
		} else {
			return TransactionName{Name: fmt.Sprintf("WebTransaction/http.route%s", httpRoute.Str()), TransactionType: WebTransactionType}
		}
	}
	return TransactionName{Name: "WebTransaction/Other/unknown", TransactionType: WebTransactionType}
}
