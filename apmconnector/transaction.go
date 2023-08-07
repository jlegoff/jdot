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

func (transactions *TransactionsMap) GetOrCreateTransaction(sdkLanguage string, span ptrace.Span, metricSlice pmetric.MetricSlice) (*Transaction, string) {
	traceID := span.TraceID().String()
	transaction, txExists := transactions.Transactions[traceID]
	if !txExists {
		transaction = &Transaction{SdkLanguage: sdkLanguage, SpanToChildDuration: make(map[string]int64),
			MetricSlice: metricSlice, Measurements: make(map[string]Measurement)}
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
				attributes := pcommon.NewMap()
				//span.Attributes().CopyTo(attributes)
				attributes.PutStr("db.operation", dbOperation.AsString())
				attributes.PutStr("db.system", dbSystem.AsString())
				attributes.PutStr("db.sql.table", dbTable.AsString())

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
		attributes := pcommon.NewMap()
		//span.Attributes().CopyTo(attributes)
		attributes.PutStr("external.host", serverAddress.AsString())

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
	attributes := pcommon.NewMap()
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
	metric := AddMetric(transaction.MetricSlice, "apm.service.transaction.duration")
	dp := SetHistogramFromSpan(metric, span)
	//span.Attributes().CopyTo(dp.Attributes())

	transactionName, transactionType := GetTransactionMetricName(span)

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

	overviewMetricName := transactionType.GetOverviewMetricName()

	for segment, sum := range breakdownBySegment {
		overviewMetric := AddMetric(transaction.MetricSlice, overviewMetricName)
		overviewDp := SetHistogram(overviewMetric, span.StartTimestamp(), span.EndTimestamp(), sum)
		//span.Attributes().CopyTo(overviewDp.Attributes())

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
	measurement.Attributes.PutStr("scope", transactionName)
	// we might not need transactionName here..
	measurement.Attributes.PutStr("transactionName", transactionName)
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

func SetHistogram(metric pmetric.Metric, startTimestamp, endTimestamp pcommon.Timestamp, durationNanos int64) pmetric.HistogramDataPoint {
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

func FilterAttributes(from pcommon.Map) pcommon.Map {
	attributes := []string{"os.description", "telemetry.auto.version", "telemetry.sdk.language", "host.name",
		"os.type", "telemetry.sdk.name", "process.runtime.description", "process.runtime.version", "telemetry.sdk.version",
		"host.arch", "service.name", "service.instance.id"}

	f := from.AsRaw()
	m := make(map[string]any)
	for _, k := range attributes {
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

func GetSdkLanguage(attributes pcommon.Map) string {
	sdkLanguage, sdkLanguagePresent := attributes.Get("telemetry.sdk.language")
	if sdkLanguagePresent {
		return sdkLanguage.AsString()
	}
	return "unknown"
}

// Generate the metrc used for the host instances drop down
func GenerateInstanceMetric(resourceMetrics pmetric.ScopeMetrics, hostName string, timestamp pcommon.Timestamp) {
	metric := resourceMetrics.Metrics().AppendEmpty()
	metric.SetName("apm.service.instance.count")
	sum := metric.SetEmptySum()
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	sum.SetIsMonotonic(false)
	dp := sum.DataPoints().AppendEmpty()

	dp.SetTimestamp(timestamp)

	dp.SetIntValue(1)

	dp.Attributes().PutStr("instanceName", hostName)
	dp.Attributes().PutStr("host.displayName", hostName)
}
