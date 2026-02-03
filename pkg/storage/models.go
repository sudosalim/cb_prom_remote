package storage

import (
	"crypto/md5"
	"fmt"
	"sort"
	"strings"
	"time"

	pb "github.com/sudosalim/cb_prom_remote/proto"
)

// TimeSeriesDocument represents a Couchbase time series document
type TimeSeriesDocument struct {
	// Required fields for Couchbase time series
	TsStart    int64       `json:"ts_start"`              // Start timestamp in milliseconds
	TsEnd      int64       `json:"ts_end"`                // End timestamp in milliseconds
	TsInterval *int64      `json:"ts_interval,omitempty"` // Interval for regular series (milliseconds)
	TsData     interface{} `json:"ts_data"`               // Array of values or [timestamp, value] pairs

	// Metadata fields
	MetricName string            `json:"metric_name"`
	Labels     map[string]string `json:"labels"`
	SeriesHash string            `json:"series_hash"`

	// Document management
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Version   int       `json:"version"`
}

// Sample represents a single metric sample
type Sample struct {
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
}

// TimeSeries represents a complete time series with metadata
type TimeSeries struct {
	MetricName string            `json:"metric_name"`
	Labels     map[string]string `json:"labels"`
	Samples    []Sample          `json:"samples"`
	SeriesHash string            `json:"series_hash"`
}

// DocumentKey generates a Couchbase document key for a time series
func (ts *TimeSeries) DocumentKey(timeWindow time.Time, interval time.Duration) string {
	// Create a time-bucketed key: metrics:{metric_name}:{series_hash}:{time_bucket}
	bucket := timeWindow.Truncate(interval).Unix()
	return fmt.Sprintf("metrics:%s:%s:%d", ts.MetricName, ts.SeriesHash, bucket)
}

// CreateSeriesHash creates a deterministic hash for a time series based on labels
func CreateSeriesHash(labels map[string]string) string {
	// Sort labels to ensure consistent hash
	var labelPairs []string
	for k, v := range labels {
		labelPairs = append(labelPairs, fmt.Sprintf("%s=%s", k, v))
	}
	sort.Strings(labelPairs)

	labelString := strings.Join(labelPairs, ",")
	hash := md5.Sum([]byte(labelString))
	return fmt.Sprintf("%x", hash)[:16] // Use first 16 characters
}

// NewTimeSeriesFromProto creates a TimeSeries from protobuf data
func NewTimeSeriesFromProto(pbSeries *pb.TimeSeries) *TimeSeries {
	ts := &TimeSeries{
		Labels:  make(map[string]string),
		Samples: make([]Sample, 0, len(pbSeries.Samples)),
	}

	// Extract labels
	for _, label := range pbSeries.Labels {
		ts.Labels[label.Name] = label.Value
		if label.Name == "__name__" {
			ts.MetricName = label.Value
		}
	}

	// If no __name__ label, use empty metric name
	if ts.MetricName == "" {
		ts.MetricName = "unknown"
	}

	// Extract samples
	for _, sample := range pbSeries.Samples {
		ts.Samples = append(ts.Samples, Sample{
			Timestamp: sample.Timestamp,
			Value:     sample.Value,
		})
	}

	// Generate series hash
	ts.SeriesHash = CreateSeriesHash(ts.Labels)

	return ts
}

// For regular format: just return a copy of the values slice
func float64SliceCopy(values []float64) []float64 {
	out := make([]float64, len(values))
	copy(out, values)
	return out
}

// ToDocument converts TimeSeries to a Couchbase time series document
func (ts *TimeSeries) ToDocument(isRegular bool, interval time.Duration) *TimeSeriesDocument {
	if len(ts.Samples) == 0 {
		return nil
	}

	doc := &TimeSeriesDocument{
		MetricName: ts.MetricName,
		Labels:     ts.Labels,
		SeriesHash: ts.SeriesHash,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
		Version:    1,
	}

	// Set start and end timestamps
	doc.TsStart = ts.Samples[0].Timestamp
	doc.TsEnd = ts.Samples[len(ts.Samples)-1].Timestamp

	if isRegular && interval > 0 {
		// Regular time series - append-only, no nulls
		intervalMs := interval.Milliseconds()
		doc.TsInterval = &intervalMs
		values := make([]float64, len(ts.Samples))
		for i, sample := range ts.Samples {
			values[i] = sample.Value
		}
		doc.TsData = float64SliceCopy(values)
	} else {
		// Irregular time series - store [timestamp, value] pairs
		data := make([][]interface{}, len(ts.Samples))
		for i, sample := range ts.Samples {
			data[i] = []interface{}{sample.Timestamp, sample.Value}
		}
		doc.TsData = data
	}

	return doc
}

// BuildRegularDocument creates a Couchbase regular time series document for a fixed window.
// windowStartMs and windowEndMs define the document window; intervalMs is the time between consecutive values.
// Samples are bucketed into slots; unfilled slots get math.NaN(). Multiple samples in the same slot use the last value.
func BuildRegularDocument(metricName string, labels map[string]string, seriesHash string, windowStartMs, windowEndMs, intervalMs int64, samples []Sample) *TimeSeriesDocument {
	if intervalMs <= 0 || windowEndMs <= windowStartMs {
		return nil
	}
	numSlots := (windowEndMs - windowStartMs) / intervalMs
	if numSlots <= 0 {
		return nil
	}
	if len(samples) == 0 {
		return nil
	}
	values := make([]float64, len(samples))
	for i, s := range samples {
		values[i] = s.Value
	}
	now := time.Now()
	return &TimeSeriesDocument{
		TsStart:    samples[0].Timestamp,
		TsEnd:      samples[len(samples)-1].Timestamp,
		TsInterval: &intervalMs,
		TsData:     float64SliceCopy(values),
		MetricName: metricName,
		Labels:     labels,
		SeriesHash: seriesHash,
		CreatedAt:  now,
		UpdatedAt:  now,
		Version:    1,
	}
}

// IsStaleMarker checks if a value is a Prometheus stale marker
func IsStaleMarker(value float64) bool {
	// Prometheus stale marker is NaN with specific bit pattern: 0x7ff0000000000002
	return value != value && fmt.Sprintf("%016x", uint64(value)) == "7ff8000000000002"
}

// ValidateLabels validates that labels conform to Prometheus standards
func ValidateLabels(labels map[string]string) error {
	for name, value := range labels {
		if name == "" {
			return fmt.Errorf("label name cannot be empty")
		}
		if value == "" {
			return fmt.Errorf("label value cannot be empty")
		}
		// Label names starting with __ are reserved
		if strings.HasPrefix(name, "__") && name != "__name__" {
			return fmt.Errorf("label name %s is reserved", name)
		}
	}
	return nil
}
