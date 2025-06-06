package storage

import (
	"context"
	"fmt"
	"log"
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
		
		// Filter out stale markers if configured
		filteredSamples := make([]Sample, 0, len(ts.Samples))
		for _, sample := range ts.Samples {
			if !IsStaleMarker(sample.Value) {
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

	// Group by document key
	documents := make(map[string]*TimeSeriesDocument)
	
	for _, ts := range currentBatch {
		timeWindow := time.UnixMilli(ts.Samples[0].Timestamp)
		docKey := ts.DocumentKey(timeWindow, w.config.Storage.TimeSeriesInterval)
		
		if existingDoc, exists := documents[docKey]; exists {
			// Merge samples into existing document
			if err := w.mergeTimeSeriesIntoDocument(existingDoc, ts); err != nil {
				log.Printf("Failed to merge time series: %v", err)
				continue
			}
		} else {
			// Create new document
			doc := ts.ToDocument(false, 0) // Use irregular format for simplicity
			if doc != nil {
				documents[docKey] = doc
			}
		}
	}

	// Write documents to Couchbase
	for docKey, doc := range documents {
		// Upsert document
		_, err := w.collection.Upsert(docKey, doc, &gocb.UpsertOptions{
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
	log.Printf("Flushed batch of %d time series into %d documents", len(currentBatch), len(documents))
	
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