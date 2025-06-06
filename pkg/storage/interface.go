package storage

import (
	"context"
	"time"

	pb "github.com/couchbase/cb_prom_remote/proto"
)

// Writer defines the interface for storing time series data
type Writer interface {
	// Write stores time series data
	Write(ctx context.Context, req *pb.WriteRequest) error
	
	// Close closes the storage connection and flushes any pending data
	Close() error
	
	// Health checks if the storage backend is healthy
	Health(ctx context.Context) error
}

// WriteOptions configures write behavior
type WriteOptions struct {
	// Timeout for write operations
	Timeout time.Duration
	
	// Whether to force immediate flush
	Flush bool
	
	// Whether to validate data before writing
	Validate bool
}

// StorageMetrics provides metrics about storage operations
type StorageMetrics struct {
	WritesTotal       int64
	WriteErrorsTotal  int64
	SamplesTotal      int64
	TimeSeriesTotal   int64
	LastWriteTime     time.Time
	ConnectionStatus  string
	QueueDepth        int
	FlushLatency      time.Duration
} 