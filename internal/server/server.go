package server

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/couchbase/cb_prom_remote/pkg/config"
)

// Server represents the HTTP server for remote write operations
type Server struct {
	httpServer   *http.Server
	writeHandler *WriteHandler
	config       *config.Config
}

// NewServer creates a new HTTP server
func NewServer(cfg *config.Config) (*Server, error) {
	writeHandler, err := NewWriteHandler(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create write handler: %w", err)
	}

	mux := http.NewServeMux()

	// Remote write endpoint
	mux.Handle("/api/v1/write", writeHandler)

	// Health check endpoint (includes storage health)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		if err := writeHandler.GetStorage().Health(ctx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(fmt.Sprintf("Storage unhealthy: %v", err)))
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// Ready check endpoint
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Ready"))
	})

	httpServer := &http.Server{
		Addr:         cfg.Server.ListenAddress,
		Handler:      mux,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
		IdleTimeout:  cfg.Server.IdleTimeout,
	}

	return &Server{
		httpServer:   httpServer,
		writeHandler: writeHandler,
		config:       cfg,
	}, nil
}

// Start starts the HTTP server
func (s *Server) Start() error {
	if s.config.Server.TLS.Enabled {
		return s.httpServer.ListenAndServeTLS(
			s.config.Server.TLS.CertFile,
			s.config.Server.TLS.KeyFile,
		)
	}
	return s.httpServer.ListenAndServe()
}

// Stop gracefully stops the HTTP server
func (s *Server) Stop(ctx context.Context) error {
	// Close the write handler first
	if err := s.writeHandler.Close(); err != nil {
		return fmt.Errorf("failed to close write handler: %w", err)
	}

	// Shutdown the HTTP server
	return s.httpServer.Shutdown(ctx)
}
