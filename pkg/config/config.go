package config

import (
	"fmt"
	"time"
)

// Config holds the complete configuration for the services
type Config struct {
	Server    ServerConfig    `yaml:"server"`
	Couchbase CouchbaseConfig `yaml:"couchbase"`
	Storage   StorageConfig   `yaml:"storage"`
	Metrics   MetricsConfig   `yaml:"metrics"`
}

// ServerConfig holds HTTP server configuration
type ServerConfig struct {
	ListenAddress    string        `yaml:"listen_address" default:":8080"`
	ReadTimeout      time.Duration `yaml:"read_timeout" default:"30s"`
	WriteTimeout     time.Duration `yaml:"write_timeout" default:"30s"`
	IdleTimeout      time.Duration `yaml:"idle_timeout" default:"120s"`
	MaxRequestSize   int64         `yaml:"max_request_size" default:"33554432"` // 32MB
	EnableProfiling  bool          `yaml:"enable_profiling" default:"false"`
	TLS              TLSConfig     `yaml:"tls"`
}

// TLSConfig holds TLS configuration
type TLSConfig struct {
	Enabled  bool   `yaml:"enabled" default:"false"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

// CouchbaseConfig holds Couchbase connection configuration
type CouchbaseConfig struct {
	ConnectionString string        `yaml:"connection_string" default:"couchbase://localhost"`
	Username         string        `yaml:"username"`
	Password         string        `yaml:"password"`
	Bucket           string        `yaml:"bucket" default:"metrics"`
	Scope            string        `yaml:"scope" default:"timeseries"`
	Collection       string        `yaml:"collection" default:"samples"`
	ConnectTimeout   time.Duration `yaml:"connect_timeout" default:"10s"`
	KVTimeout        time.Duration `yaml:"kv_timeout" default:"5s"`
}

// StorageConfig holds storage-specific configuration
type StorageConfig struct {
	BatchSize           int           `yaml:"batch_size" default:"1000"`
	FlushInterval       time.Duration `yaml:"flush_interval" default:"5s"`
	DocumentSizeLimit   int           `yaml:"document_size_limit" default:"20971520"` // 20MB
	TimeSeriesInterval  time.Duration `yaml:"timeseries_interval" default:"1h"`       // Group samples by hour
	RetentionPeriod     time.Duration `yaml:"retention_period" default:"720h"`        // 30 days
	CompressionEnabled  bool          `yaml:"compression_enabled" default:"true"`
}

// MetricsConfig holds internal metrics configuration
type MetricsConfig struct {
	Enabled bool   `yaml:"enabled" default:"true"`
	Path    string `yaml:"path" default:"/metrics"`
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.Couchbase.ConnectionString == "" {
		return fmt.Errorf("couchbase connection string is required")
	}
	
	if c.Couchbase.Bucket == "" {
		return fmt.Errorf("couchbase bucket is required")
	}
	
	if c.Storage.BatchSize <= 0 {
		return fmt.Errorf("storage batch size must be positive")
	}
	
	if c.Storage.FlushInterval <= 0 {
		return fmt.Errorf("storage flush interval must be positive")
	}
	
	if c.Server.TLS.Enabled {
		if c.Server.TLS.CertFile == "" || c.Server.TLS.KeyFile == "" {
			return fmt.Errorf("TLS cert and key files are required when TLS is enabled")
		}
	}
	
	return nil
} 