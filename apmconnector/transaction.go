package apmconnector

import (
	"fmt"

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

func (t TransactionType) GetOverviewMetricName() string {
	switch t {
	case WebTransactionType:
		return "apm.service.overview.web"
	default:
		return "apm.service.overview.other"
	}
}

type Transaction struct {
	SdkLanguage         string
	SpanToChildDuration map[string]int64
	ScopeMetrics        *ScopeMetrics

	Measurements map[string]Measurement
	RootSpan     ptrace.Span
}

type Measurement struct {
	SpanId                 string
	MetricName             string
	DurationNanos          int64
	ExclusiveDurationNanos int64
	Attributes             map[string]string
	SegmentNameProvider    func(TransactionType) string
	MetricTimesliceName    string
	// FIXME
	Span ptrace.Span
}

type TransactionsMap struct {
	Transactions map[string]*Transaction
}

func NewTransactionsMap() *TransactionsMap {
	return &TransactionsMap{Transactions: make(map[string]*Transaction)}
}

func (transactions *TransactionsMap) ProcessTransactions() {
	for _, transaction := range transactions.Transactions {
		transaction.ProcessServerSpan()
	}
}

func (transactions *TransactionsMap) GetOrCreateTransaction(sdkLanguage string, span ptrace.Span, metrics *ScopeMetrics) (*Transaction, string) {
	traceID := span.TraceID().String()
	transaction, txExists := transactions.Transactions[traceID]
	if !txExists {
		transaction = &Transaction{SdkLanguage: sdkLanguage, SpanToChildDuration: make(map[string]int64),
			ScopeMetrics: metrics, Measurements: make(map[string]Measurement)}
		transactions.Transactions[traceID] = transaction
		//fmt.Printf("Created transaction for: %s   %s\n", traceID, transaction.sdkLanguage)
	}

	return transaction, traceID
}

func (transaction *Transaction) IsRootSet() bool {
	return (ptrace.Span{}) != transaction.RootSpan
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

func NewSimpleNameProvider(name string) func(TransactionType) string {
	return func(t TransactionType) string { return name }
}

func (transaction *Transaction) ProcessDatabaseSpan(span ptrace.Span) bool {
	if dbSystem, dbSystemPresent := span.Attributes().Get("db.system"); dbSystemPresent {
		if dbOperation, dbOperationPresent := span.Attributes().Get("db.operation"); dbOperationPresent {
			if dbTable, dbTablePresent := span.Attributes().Get("db.sql.table"); dbTablePresent {
				attributes := make(map[string]string)
				//span.Attributes().CopyTo(attributes)
				attributes["db.operation"] = dbOperation.AsString()
				attributes["db.system"] = dbSystem.AsString()
				attributes["db.sql.table"] = dbTable.AsString()

				timesliceName := fmt.Sprintf("Datastore/statement/%s/%s/%s", dbSystem.AsString(), dbTable.AsString(), dbOperation.AsString())
				measurement := Measurement{SpanId: span.SpanID().String(), MetricName: "apm.service.datastore.operation.duration", Span: span,
					DurationNanos: DurationInNanos(span), Attributes: attributes, SegmentNameProvider: NewSimpleNameProvider(dbSystem.AsString()), MetricTimesliceName: timesliceName}

				transaction.Measurements[measurement.SpanId] = measurement

				return true
			}
		}
	}
	return false
}

func (transaction *Transaction) ProcessExternalSpan(span ptrace.Span) bool {
	if serverAddress, serverAddressPresent := span.Attributes().Get("server.address"); serverAddressPresent {
		attributes := make(map[string]string)
		//span.Attributes().CopyTo(attributes)
		attributes["external.host"] = serverAddress.AsString()

		segmentNameProvider := func(t TransactionType) string {
			switch t {
			case WebTransactionType:
				return "Web external"
			default:
				return "Background external"
			}
		}
		timesliceName := fmt.Sprintf("External/%s/all", serverAddress.AsString())
		measurement := Measurement{SpanId: span.SpanID().String(), MetricName: "apm.service.external.host.duration", Span: span,
			DurationNanos: DurationInNanos(span), Attributes: attributes, SegmentNameProvider: segmentNameProvider, MetricTimesliceName: timesliceName}

		transaction.Measurements[measurement.SpanId] = measurement
		/*
			metric := AddMetric(transaction.MetricSlice, "apm.service.transaction.external.duration")
			dp := SetHistogramFromSpan(metric, span)
			span.Attributes().CopyTo(dp.Attributes())
			dp.Attributes().PutStr("external.host", serverAddress.AsString())
		*/
		// FIXME
		//dp.Attributes().PutStr("transactionType", "Web")

		return true
	}
	return false
}

func (transaction *Transaction) ProcessGenericSpan(span ptrace.Span) bool {
	attributes := make(map[string]string)
	timesliceName := fmt.Sprintf("Custom/%s", span.Name())
	measurement := Measurement{SpanId: span.SpanID().String(), MetricName: "newrelic.timeslice.value", Span: span,
		DurationNanos: DurationInNanos(span), Attributes: attributes, SegmentNameProvider: NewSimpleNameProvider(transaction.SdkLanguage), MetricTimesliceName: timesliceName}

	transaction.Measurements[measurement.SpanId] = measurement

	return true
}

func (transaction *Transaction) ProcessClientSpan(span ptrace.Span) bool {
	return transaction.ProcessDatabaseSpan(span) || transaction.ProcessExternalSpan(span)
}

func (transaction *Transaction) ProcessServerSpan() {
	if !transaction.IsRootSet() {
		return
	}
	span := transaction.RootSpan
	metric := transaction.AddMetric("apm.service.transaction.duration")
	metric.AddDatapoint(span, make(map[string]string))

	transactionName, transactionType := GetTransactionMetricName(span)
	attributes := map[string]string{
		"transactionType": transactionType.AsString(),
		"transactionName": transactionName,
	}

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

	overviewMetricName := transactionType.GetOverviewMetricName()

	for segment, sum := range breakdownBySegment {
		attributes["segmentName"] = segment
		overviewMetric := transaction.AddMetric(overviewMetricName)
		overviewMetric.AddDatapointWithValue(span, attributes, NanosToSeconds(sum))
	}
}

func (transaction *Transaction) ProcessMeasurement(measurement *Measurement, transactionType TransactionType, transactionName string) {
	exclusiveDuration := transaction.ExclusiveTime(*measurement)
	measurement.ExclusiveDurationNanos = exclusiveDuration
	measurement.Attributes["metricTimesliceName"] = measurement.MetricTimesliceName
	//	fmt.Printf("Name: %s total: %d exclusive: %d    id:%s\n", measurement.metricName, measurement.durationNanos, exclusiveDuration, measurement.spanId)

	metric := transaction.AddMetric(measurement.MetricName)
	metric.AddDatapoint(measurement.Span, measurement.Attributes)

	measurement.Attributes["scope"] = transactionName
	// we might not need transactionName here..
	measurement.Attributes["transactionName"] = transactionName
	measurement.Attributes["transactionType"] = transactionType.AsString()
	overviewMetric := transaction.AddMetric("apm.service.transaction.overview")
	overviewMetric.AddDatapoint(measurement.Span, measurement.Attributes)
}

func DurationInNanos(span ptrace.Span) int64 {
	return (span.EndTimestamp() - span.StartTimestamp()).AsTime().UnixNano()
}

func (transaction *Transaction) ExclusiveTime(measurement Measurement) int64 {
	return measurement.DurationNanos - transaction.SpanToChildDuration[measurement.SpanId]
}

func (transaction *Transaction) AddMetric(metricName string) *Metric {
	metric := transaction.ScopeMetrics.GetOrCreateMetric(metricName)
	return metric
}

func GetTransactionMetricName(span ptrace.Span) (string, TransactionType) {
	if httpRoute, routePresent := span.Attributes().Get("http.route"); routePresent {
		return GetWebTransactionMetricName(span, httpRoute.Str(), "http.route")
	}
	if urlPath, urlPathPresent := span.Attributes().Get("url.path"); urlPathPresent {
		return GetWebTransactionMetricName(span, urlPath.Str(), "Uri")
	}
	return "WebTransaction/Other/unknown", WebTransactionType
}

func GetWebTransactionMetricName(span ptrace.Span, name string, nameType string) (string, TransactionType) {
	if method, methodPresent := span.Attributes().Get("http.method"); methodPresent {
		return fmt.Sprintf("WebTransaction/%s%s (%s)", nameType, name, method.Str()), WebTransactionType
	} else {
		return fmt.Sprintf("WebTransaction/%s%s", nameType, name), WebTransactionType
	}
}

func NanosToSeconds(nanos int64) float64 {
	return float64(nanos) / 1e9
}
