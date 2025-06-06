package test

import (
	"bytes"
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/golang/snappy"
	"google.golang.org/protobuf/proto"

	"github.com/couchbase/cb_prom_remote/internal/server"
	"github.com/couchbase/cb_prom_remote/pkg/config"
	pb "github.com/couchbase/cb_prom_remote/proto"
)

func TestRemoteWriteHTTPLayer(t *testing.T) {
	// Create test configuration
	cfg := &config.Config{
		Server: config.ServerConfig{
			ListenAddress:  ":0", // Use random available port
			ReadTimeout:    5 * time.Second,
			WriteTimeout:   5 * time.Second,
			IdleTimeout:    10 * time.Second,
			MaxRequestSize: 1024 * 1024, // 1MB
		},
		Couchbase: config.CouchbaseConfig{
			ConnectionString: "couchbase://localhost",
			Bucket:          "test",
			Scope:           "timeseries",
			Collection:      "samples",
		},
		Storage: config.StorageConfig{
			BatchSize:     1000,
			FlushInterval: time.Second,
		},
	}

	// Create server
	srv, err := server.NewServer(cfg)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// Start server in background
	go func() {
		if err := srv.Start(); err != nil && err != http.ErrServerClosed {
			t.Errorf("Server failed: %v", err)
		}
	}()

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Create test write request
	writeReq := &pb.WriteRequest{
		Timeseries: []*pb.TimeSeries{
			{
				Labels: []*pb.Label{
					{Name: "__name__", Value: "test_metric"},
					{Name: "job", Value: "test_job"},
					{Name: "instance", Value: "localhost:8080"},
				},
				Samples: []*pb.Sample{
					{
						Value:     42.0,
						Timestamp: time.Now().UnixMilli(),
					},
				},
			},
		},
	}

	// Marshal and compress
	data, err := proto.Marshal(writeReq)
	if err != nil {
		t.Fatalf("Failed to marshal protobuf: %v", err)
	}

	compressed := snappy.Encode(nil, data)

	// Create HTTP request
	req, err := http.NewRequest("POST", "http://localhost:8080/api/v1/write", bytes.NewReader(compressed))
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}

	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "snappy")
	req.Header.Set("User-Agent", "test-client/1.0")
	req.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")

	// Send request
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	defer resp.Body.Close()

	// Check response
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	// Check response headers
	if version := resp.Header.Get("X-Prometheus-Remote-Write-Version"); version != "0.1.0" {
		t.Errorf("Expected version 0.1.0, got %s", version)
	}

	// Stop server
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Stop(ctx); err != nil {
		t.Errorf("Failed to stop server: %v", err)
	}
} 