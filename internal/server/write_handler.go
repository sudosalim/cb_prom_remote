package server

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/couchbase/cb_prom_remote/pkg/config"
	"github.com/couchbase/cb_prom_remote/pkg/protocol"
	"github.com/couchbase/cb_prom_remote/pkg/storage"
	pb "github.com/couchbase/cb_prom_remote/proto"
)

const (
	// Required headers for Prometheus remote write
	HeaderContentType     = "Content-Type"
	HeaderContentEncoding = "Content-Encoding"
	HeaderUserAgent       = "User-Agent"
	HeaderRemoteWriteVersion = "X-Prometheus-Remote-Write-Version"
	
	// Expected values
	ExpectedContentType = "application/x-protobuf"
	SupportedVersion    = "0.1.0"
)

// WriteHandler handles Prometheus remote write requests
type WriteHandler struct {
	decoder *protocol.Decoder
	config  *config.Config
	storage storage.Writer
}

// GetStorage returns the storage writer (for health checks)
func (h *WriteHandler) GetStorage() storage.Writer {
	return h.storage
}

// NewWriteHandler creates a new write handler
func NewWriteHandler(cfg *config.Config) (*WriteHandler, error) {
	decoder, err := protocol.NewDecoder()
	if err != nil {
		return nil, fmt.Errorf("failed to create decoder: %w", err)
	}

	// Create storage writer
	storageWriter, err := storage.NewCouchbaseWriter(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage writer: %w", err)
	}

	return &WriteHandler{
		decoder: decoder,
		config:  cfg,
		storage: storageWriter,
	}, nil
}

// ServeHTTP implements http.Handler interface
func (h *WriteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Only allow POST requests
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Validate required headers
	if err := h.validateHeaders(r); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Enforce request size limit
	if r.ContentLength > h.config.Server.MaxRequestSize {
		http.Error(w, "Request too large", http.StatusRequestEntityTooLarge)
		return
	}

	// Read request body
	body, err := io.ReadAll(io.LimitReader(r.Body, h.config.Server.MaxRequestSize))
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	// Detect compression type
	compressionType, err := protocol.DetectCompressionType(r.Header.Get(HeaderContentEncoding))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Decode the request
	writeRequest, err := h.decoder.DecodeWriteRequest(body, compressionType)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to decode request: %v", err), http.StatusBadRequest)
		return
	}

	// Process the write request (placeholder for now)
	if err := h.processWriteRequest(writeRequest); err != nil {
		http.Error(w, fmt.Sprintf("Failed to process request: %v", err), http.StatusInternalServerError)
		return
	}

	// Send successful response
	w.Header().Set(HeaderRemoteWriteVersion, SupportedVersion)
	w.WriteHeader(http.StatusOK)
}

// validateHeaders validates required Prometheus remote write headers
func (h *WriteHandler) validateHeaders(r *http.Request) error {
	// Validate Content-Type
	contentType := r.Header.Get(HeaderContentType)
	if contentType != ExpectedContentType {
		return fmt.Errorf("invalid content type: expected %s, got %s", ExpectedContentType, contentType)
	}

	// Validate Content-Encoding is present
	contentEncoding := r.Header.Get(HeaderContentEncoding)
	if contentEncoding == "" {
		return fmt.Errorf("missing Content-Encoding header")
	}

	// Validate User-Agent is present
	userAgent := r.Header.Get(HeaderUserAgent)
	if userAgent == "" {
		return fmt.Errorf("missing User-Agent header")
	}

	return nil
}

// processWriteRequest processes the decoded write request
func (h *WriteHandler) processWriteRequest(req interface{}) error {
	writeRequest, ok := req.(*pb.WriteRequest)
	if !ok {
		return fmt.Errorf("invalid request type")
	}

	if writeRequest == nil {
		return fmt.Errorf("received nil write request")
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Write to storage
	return h.storage.Write(ctx, writeRequest)
}

// Close cleans up resources
func (h *WriteHandler) Close() error {
	if err := h.storage.Close(); err != nil {
		return fmt.Errorf("failed to close storage: %w", err)
	}
	return h.decoder.Close()
} 