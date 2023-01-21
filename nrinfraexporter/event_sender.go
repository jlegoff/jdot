package nrinfraexporter

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"time"
)

// copied from https://github.com/newrelic/infrastructure-agent/blob/d9c6f5f/internal/agent/event_sender.go#L277
type MetricPost struct {
	ExternalKeys     []string          `json:"ExternalKeys,omitempty"`
	EntityID         int64             `json:"EntityID,omitempty"`
	IsAgent          bool              `json:"IsAgent"`
	Events           []json.RawMessage `json:"Events"`
	ReportingAgentID int64             `json:"ReportingAgentID,omitempty"`
}

func SendEvents(metricIngestURL string, licenseKey string, samples AllSamples) {
	client := getHttpClient()
	metricPosts := PreparePost(samples)
	if len(metricPosts) == 0 {
		return
	}
	agentId := metricPosts[0].ReportingAgentID

	postBytes, marshallErr := json.Marshal(metricPosts)
	if marshallErr != nil {
		fmt.Println("Error marshalling data", marshallErr)
		return
	}
	reqBuf := bytes.NewBuffer(postBytes)
	req, reqErr := http.NewRequest("POST", fmt.Sprintf("%s/infra/v2/metrics/events/bulk", metricIngestURL), reqBuf)
	if reqErr != nil {
		fmt.Println("Error creating request", reqErr)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "New Relic Infrastructure Agent version 2.0")
	req.Header.Set("X-License-Key", licenseKey)
	req.Header.Set("X-NRI-Entity-Key", strconv.FormatInt(agentId, 10))
	req.Header.Set("X-NRI-Agent-Entity-Id", strconv.FormatInt(agentId, 10))
	resp, clientErr := client.Do(req)
	if clientErr != nil {
		fmt.Println("Error sending events", clientErr)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		fmt.Println("Request failed", resp.StatusCode)
		buf, _ := ioutil.ReadAll(resp.Body)
		fmt.Println(string(buf))
		fmt.Println(licenseKey)
		fmt.Println(strconv.FormatInt(agentId, 10))
	}
}

func getHttpClient() *http.Client {
	return &http.Client{
		Timeout:   time.Minute,
		Transport: getTransport(),
	}
}

func getTransport() *http.Transport {
	cfg := &tls.Config{RootCAs: systemCertPool()}
	return &http.Transport{
		DialContext:           (&net.Dialer{Timeout: time.Minute, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   time.Minute,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig:       cfg,
	}
}

func systemCertPool() *x509.CertPool {
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	return pool
}

func PreparePost(samples AllSamples) []MetricPost {
	var metricPosts []MetricPost
	for _, entitySamples := range samples.EntitySamples {
		metricPosts = append(metricPosts, ConvertEntitySample(entitySamples))
	}
	return metricPosts
}

func ConvertEntitySample(entitySamples EntitySamples) MetricPost {
	var rawEvents []json.RawMessage
	for _, sample := range entitySamples.Samples {
		raw, errMarsh := json.Marshal(sample)
		if errMarsh != nil {
			fmt.Println("Error serializing event", errMarsh)
			continue
		}
		rawEvents = append(rawEvents, raw)
	}
	mp := MetricPost{
		EntityID:         entitySamples.EntityId,
		ReportingAgentID: entitySamples.EntityId, // TODO
		IsAgent:          true,                   // TODO
		Events:           rawEvents,
	}
	return mp
}
