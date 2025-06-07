package storage

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"github.com/couchbase/gocb/v2"

	"github.com/sudosalim/cb_prom_remote/pkg/config"
	pb "github.com/sudosalim/cb_prom_remote/proto"
)

// CouchbaseWriter implements Writer interface for Couchbase storage
type CouchbaseWriter struct {
	cluster    *gocb.Cluster
	bucket     *gocb.Bucket
	collection *gocb.Collection
	config     *config.Config
	
	// Batching
	batchMutex sync.Mutex
	batch      []*TimeSeries
	batchTimer *time.Timer
	
	// Metrics
	metrics *StorageMetrics
	
	// Shutdown
	done chan struct{}
	wg   sync.WaitGroup
}

// NewCouchbaseWriter creates a new Couchbase storage writer
func NewCouchbaseWriter(cfg *config.Config) (*CouchbaseWriter, error) {
	// Connection options
	opts := gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: cfg.Couchbase.Username,
			Password: cfg.Couchbase.Password,
		},
		TimeoutsConfig: gocb.TimeoutsConfig{
			ConnectTimeout: cfg.Couchbase.ConnectTimeout,
			KVTimeout:      cfg.Couchbase.KVTimeout,
		},
	}

	// Handle Capella connections
	if cfg.Couchbase.Username != "" && cfg.Couchbase.Password != "" {
		opts.SecurityConfig = gocb.SecurityConfig{
			TLSSkipVerify: false, // Use proper TLS for Capella
		}
	}

	// Connect to cluster
	cluster, err := gocb.Connect(cfg.Couchbase.ConnectionString, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Couchbase cluster: %w", err)
	}

	// Wait for cluster to be ready
	err = cluster.WaitUntilReady(cfg.Couchbase.ConnectTimeout, nil)
	if err != nil {
		cluster.Close(nil)
		return nil, fmt.Errorf("cluster not ready: %w", err)
	}

	// Get bucket
	bucket := cluster.Bucket(cfg.Couchbase.Bucket)
	err = bucket.WaitUntilReady(cfg.Couchbase.ConnectTimeout, nil)
	if err != nil {
		cluster.Close(nil)
		return nil, fmt.Errorf("bucket not ready: %w", err)
	}

	// Get collection
	collection := bucket.Scope(cfg.Couchbase.Scope).Collection(cfg.Couchbase.Collection)

	writer := &CouchbaseWriter{
		cluster:    cluster,
		bucket:     bucket,
		collection: collection,
		config:     cfg,
		batch:      make([]*TimeSeries, 0, cfg.Storage.BatchSize),
		metrics: &StorageMetrics{
			ConnectionStatus: "connected",
		},
		done: make(chan struct{}),
	}

	// Start batch flusher
	writer.startBatchFlusher()

	log.Printf("Connected to Couchbase: %s", cfg.Couchbase.ConnectionString)
	return writer, nil
}

// Write implements the Writer interface
func (w *CouchbaseWriter) Write(ctx context.Context, req *pb.WriteRequest) error {
	if req == nil || len(req.Timeseries) == 0 {
		return fmt.Errorf("empty write request")
	}

	// Convert protobuf to internal format
	timeSeries := make([]*TimeSeries, 0, len(req.Timeseries))
	for _, pbSeries := range req.Timeseries {
		ts := NewTimeSeriesFromProto(pbSeries)
		
		// Validate labels
		if err := ValidateLabels(ts.Labels); err != nil {
			return fmt.Errorf("invalid labels: %w", err)
		}
		
		// Filter out stale markers and any NaN values
		filteredSamples := make([]Sample, 0, len(ts.Samples))
		for _, sample := range ts.Samples {
			// Filter out NaN values (including stale markers) and infinite values
			if !IsStaleMarker(sample.Value) && !isNaNOrInf(sample.Value) {
				filteredSamples = append(filteredSamples, sample)
			}
		}
		ts.Samples = filteredSamples
		
		if len(ts.Samples) > 0 {
			timeSeries = append(timeSeries, ts)
		}
	}

	if len(timeSeries) == 0 {
		return nil // Nothing to write after filtering
	}

	// Add to batch
	w.batchMutex.Lock()
	w.batch = append(w.batch, timeSeries...)
	shouldFlush := len(w.batch) >= w.config.Storage.BatchSize
	w.batchMutex.Unlock()

	// Immediate flush if batch is full
	if shouldFlush {
		return w.flushBatch(ctx)
	}

	// Update metrics
	w.metrics.SamplesTotal += int64(len(req.Timeseries))
	w.metrics.TimeSeriesTotal += int64(len(timeSeries))
	w.metrics.LastWriteTime = time.Now()

	return nil
}

// flushBatch writes the current batch to Couchbase
func (w *CouchbaseWriter) flushBatch(ctx context.Context) error {
	w.batchMutex.Lock()
	if len(w.batch) == 0 {
		w.batchMutex.Unlock()
		return nil
	}
	
	// Take current batch and reset
	currentBatch := w.batch
	w.batch = make([]*TimeSeries, 0, w.config.Storage.BatchSize)
	w.batchMutex.Unlock()

	start := time.Now()
	defer func() {
		w.metrics.FlushLatency = time.Since(start)
	}()

	// Group new time series by document key
	newSeriesByDocKey := make(map[string][]*TimeSeries)
	
	for _, ts := range currentBatch {
		timeWindow := time.UnixMilli(ts.Samples[0].Timestamp)
		docKey := ts.DocumentKey(timeWindow, w.config.Storage.TimeSeriesInterval)
		newSeriesByDocKey[docKey] = append(newSeriesByDocKey[docKey], ts)
	}

	// Process each document
	for docKey, newSeries := range newSeriesByDocKey {
		// First, try to get existing document
		var existingDoc *TimeSeriesDocument
		getResult, err := w.collection.Get(docKey, &gocb.GetOptions{
			Timeout: w.config.Couchbase.KVTimeout,
		})
		
		if err == nil {
			// Document exists, decode it
			err = getResult.Content(&existingDoc)
			if err != nil {
				log.Printf("Failed to decode existing document %s: %v", docKey, err)
				continue
			}
		} else {
			// Document doesn't exist or error occurred
			// Check if it's a "not found" error
			if !isNotFoundError(err) {
				log.Printf("Failed to get document %s: %v", docKey, err)
				w.metrics.WriteErrorsTotal++
				continue
			}
		}

		var finalDoc *TimeSeriesDocument

		if existingDoc != nil {
			// Append new samples to existing document
			finalDoc = existingDoc
			for _, ts := range newSeries {
				if err := w.appendTimeSeriestoDocument(finalDoc, ts); err != nil {
					log.Printf("Failed to append time series to document %s: %v", docKey, err)
					continue
				}
			}
		} else {
			// Create new document from first time series
			finalDoc = newSeries[0].ToDocument(false, 0)
			if finalDoc == nil {
				continue
			}

			// Merge remaining time series into the new document
			for _, ts := range newSeries[1:] {
				if err := w.mergeTimeSeriesIntoDocument(finalDoc, ts); err != nil {
					log.Printf("Failed to merge time series into new document: %v", err)
					continue
				}
			}
		}

		// Upsert the final document
		_, err = w.collection.Upsert(docKey, finalDoc, &gocb.UpsertOptions{
			Expiry:  w.config.Storage.RetentionPeriod,
			Timeout: w.config.Couchbase.KVTimeout,
		})
		
		if err != nil {
			w.metrics.WriteErrorsTotal++
			log.Printf("Failed to write document %s: %v", docKey, err)
			return fmt.Errorf("failed to write to Couchbase: %w", err)
		}
	}

	w.metrics.WritesTotal++
	log.Printf("Flushed batch of %d time series into %d documents", len(currentBatch), len(newSeriesByDocKey))
	
	return nil
}

// mergeTimeSeriesIntoDocument merges a time series into an existing document
func (w *CouchbaseWriter) mergeTimeSeriesIntoDocument(doc *TimeSeriesDocument, ts *TimeSeries) error {
	// For simplicity, we'll append the new samples to the existing data
	// In a production system, you might want more sophisticated merging logic
	
	if len(ts.Samples) == 0 {
		return nil
	}

	// Update timestamps
	if ts.Samples[0].Timestamp < doc.TsStart {
		doc.TsStart = ts.Samples[0].Timestamp
	}
	if ts.Samples[len(ts.Samples)-1].Timestamp > doc.TsEnd {
		doc.TsEnd = ts.Samples[len(ts.Samples)-1].Timestamp
	}

	// Append samples (assuming irregular format)
	if existingData, ok := doc.TsData.([][]interface{}); ok {
		for _, sample := range ts.Samples {
			existingData = append(existingData, []interface{}{sample.Timestamp, sample.Value})
		}
		doc.TsData = existingData
	} else {
		// Convert to irregular format and add new samples
		newData := make([][]interface{}, 0)
		for _, sample := range ts.Samples {
			newData = append(newData, []interface{}{sample.Timestamp, sample.Value})
		}
		doc.TsData = newData
	}

	doc.UpdatedAt = time.Now()
	doc.Version++

	return nil
}

// appendTimeSeriestoDocument appends new samples to an existing document, preserving existing data
func (w *CouchbaseWriter) appendTimeSeriestoDocument(doc *TimeSeriesDocument, ts *TimeSeries) error {
	if len(ts.Samples) == 0 {
		return nil
	}

	// Update ts_end if new samples are later
	if ts.Samples[len(ts.Samples)-1].Timestamp > doc.TsEnd {
		doc.TsEnd = ts.Samples[len(ts.Samples)-1].Timestamp
	}

	// Update ts_start if new samples are earlier
	if ts.Samples[0].Timestamp < doc.TsStart {
		doc.TsStart = ts.Samples[0].Timestamp
	}

	// Append new samples to existing data (handling both formats from JSON unmarshaling)
	if existingData, ok := doc.TsData.([][]interface{}); ok {
		// Direct format match - append new samples
		updatedData := existingData
		for _, sample := range ts.Samples {
			updatedData = append(updatedData, []interface{}{sample.Timestamp, sample.Value})
		}
		doc.TsData = updatedData
	} else if existingSlice, ok := doc.TsData.([]interface{}); ok {
		// JSON unmarshaling converted [][]interface{} to []interface{} - convert back
		updatedData := make([][]interface{}, 0, len(existingSlice)+len(ts.Samples))

		// Convert existing data
		for _, item := range existingSlice {
			if itemSlice, ok := item.([]interface{}); ok && len(itemSlice) == 2 {
				updatedData = append(updatedData, itemSlice)
			}
		}

		// Add new samples
		for _, sample := range ts.Samples {
			updatedData = append(updatedData, []interface{}{sample.Timestamp, sample.Value})
		}

		doc.TsData = updatedData
	} else {
		// If existing data is not in any expected format, start fresh with new samples
		newData := make([][]interface{}, 0, len(ts.Samples))
		for _, sample := range ts.Samples {
			newData = append(newData, []interface{}{sample.Timestamp, sample.Value})
		}
		doc.TsData = newData
	}

	// Update metadata
	doc.UpdatedAt = time.Now()
	doc.Version++

	return nil
}

// isNotFoundError checks if the error is a "document not found" error
func isNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	// Check for Couchbase "document not found" error
	return errors.Is(err, gocb.ErrDocumentNotFound)
}

// isNaNOrInf checks if a float64 value is NaN or infinite
func isNaNOrInf(value float64) bool {
	return math.IsNaN(value) || math.IsInf(value, 0)
}

// startBatchFlusher starts a background goroutine to flush batches periodically
func (w *CouchbaseWriter) startBatchFlusher() {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		
		ticker := time.NewTicker(w.config.Storage.FlushInterval)
		defer ticker.Stop()
		
		for {
			select {
			case <-ticker.C:
				if err := w.flushBatch(context.Background()); err != nil {
					log.Printf("Failed to flush batch: %v", err)
				}
			case <-w.done:
				return
			}
		}
	}()
}

// Health implements the Writer interface
func (w *CouchbaseWriter) Health(ctx context.Context) error {
	// Perform a simple ping to check connectivity
	pingResult, err := w.cluster.Ping(&gocb.PingOptions{
		ServiceTypes: []gocb.ServiceType{gocb.ServiceTypeKeyValue},
		Timeout:      w.config.Couchbase.KVTimeout,
	})
	
	if err != nil {
		w.metrics.ConnectionStatus = "unhealthy"
		return fmt.Errorf("health check failed: %w", err)
	}

	// Simple health check - if ping succeeded, consider it healthy
	if len(pingResult.Services) == 0 {
		w.metrics.ConnectionStatus = "unhealthy"
		return fmt.Errorf("no services found in ping result")
	}

	w.metrics.ConnectionStatus = "healthy"
	return nil
}

// Close implements the Writer interface
func (w *CouchbaseWriter) Close() error {
	log.Println("Closing Couchbase writer...")
	
	// Signal shutdown
	close(w.done)
	
	// Wait for background goroutines
	w.wg.Wait()
	
	// Flush any remaining data
	if err := w.flushBatch(context.Background()); err != nil {
		log.Printf("Error flushing final batch: %v", err)
	}
	
	// Close Couchbase connection
	if err := w.cluster.Close(nil); err != nil {
		return fmt.Errorf("failed to close Couchbase cluster: %w", err)
	}
	
	log.Println("Couchbase writer closed")
	return nil
}

// GetMetrics returns storage metrics
func (w *CouchbaseWriter) GetMetrics() *StorageMetrics {
	w.batchMutex.Lock()
	w.metrics.QueueDepth = len(w.batch)
	w.batchMutex.Unlock()
	
	return w.metrics
} 