package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sudosalim/cb_prom_remote/internal/server"
	"github.com/sudosalim/cb_prom_remote/pkg/config"
)

func main() {
	var (
		configFile = flag.String("config", "", "Path to configuration file")
		listenAddr = flag.String("listen-address", ":8080", "Address to listen on")
	)
	flag.Parse()

	// Load configuration (supports environment variables)
	cfg, err := config.LoadConfig(*configFile)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Override listen address if provided via flag
	if *listenAddr != ":8080" {
		cfg.Server.ListenAddress = *listenAddr
	}

	// Log key startup settings
	log.Println("--- Remote Write Startup Configuration ---")
	log.Printf("Server listen address: %s", cfg.Server.ListenAddress)
	log.Printf("Couchbase connection string: %s", cfg.Couchbase.ConnectionString)
	log.Printf("Couchbase bucket: %s, scope: %s, collection: %s", cfg.Couchbase.Bucket, cfg.Couchbase.Scope, cfg.Couchbase.Collection)
	log.Printf("Storage batch size: %d, flush interval: %s, time series type: %s", cfg.Storage.BatchSize, cfg.Storage.FlushInterval, cfg.Storage.TimeSeriesType)

	// Create and start server
	srv, err := server.NewServer(cfg)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// Start server in a goroutine
	go func() {
		log.Printf("Starting remote write server on %s", cfg.Server.ListenAddress)
		if err := srv.Start(); err != nil {
			log.Fatalf("Server failed to start: %v", err)
		}
	}()

	// Wait for interrupt signal to gracefully shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")

	// Give the server 30 seconds to shutdown gracefully
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Stop(ctx); err != nil {
		log.Printf("Server forced to shutdown: %v", err)
	} else {
		log.Println("Server exited gracefully")
	}
}
