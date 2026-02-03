package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// LoadConfig loads configuration from environment variables and optionally from a YAML file
func LoadConfig(configFile string) (*Config, error) {
	cfg := &Config{
		Server: ServerConfig{
			ListenAddress:   getEnvString("SERVER_LISTEN_ADDRESS", ":8080"),
			ReadTimeout:     getEnvDuration("SERVER_READ_TIMEOUT", 30*time.Second),
			WriteTimeout:    getEnvDuration("SERVER_WRITE_TIMEOUT", 30*time.Second),
			IdleTimeout:     getEnvDuration("SERVER_IDLE_TIMEOUT", 120*time.Second),
			MaxRequestSize:  getEnvInt64("SERVER_MAX_REQUEST_SIZE", 32*1024*1024),
			EnableProfiling: getEnvBool("SERVER_ENABLE_PROFILING", false),
			TLS: TLSConfig{
				Enabled:  getEnvBool("SERVER_TLS_ENABLED", false),
				CertFile: getEnvString("SERVER_TLS_CERT_FILE", ""),
				KeyFile:  getEnvString("SERVER_TLS_KEY_FILE", ""),
			},
		},
		Couchbase: CouchbaseConfig{
			ConnectionString: getEnvString("COUCHBASE_CONNECTION_STRING", "couchbase://localhost"),
			Username:         getEnvString("COUCHBASE_USERNAME", ""),
			Password:         getEnvString("COUCHBASE_PASSWORD", ""),
			Bucket:           getEnvString("COUCHBASE_BUCKET", "metrics"),
			Scope:            getEnvString("COUCHBASE_SCOPE", "timeseries"),
			Collection:       getEnvString("COUCHBASE_COLLECTION", "samples"),
			ConnectTimeout:   getEnvDuration("COUCHBASE_CONNECT_TIMEOUT", 10*time.Second),
			KVTimeout:        getEnvDuration("COUCHBASE_KV_TIMEOUT", 5*time.Second),
		},
		Storage: StorageConfig{
			BatchSize:             getEnvInt("STORAGE_BATCH_SIZE", 1000),
			FlushInterval:         getEnvDuration("STORAGE_FLUSH_INTERVAL", 5*time.Second),
			DocumentSizeLimit:     getEnvInt("STORAGE_DOCUMENT_SIZE_LIMIT", 20*1024*1024),
			TimeSeriesInterval:    getEnvDuration("STORAGE_TIMESERIES_INTERVAL", time.Hour),
			RetentionPeriod:       getEnvDuration("STORAGE_RETENTION_PERIOD", 720*time.Hour),
			CompressionEnabled:    getEnvBool("STORAGE_COMPRESSION_ENABLED", true),
			TimeSeriesType:        strings.ToLower(getEnvString("STORAGE_TIMESERIES_TYPE", "irregular")),
			RegularSampleInterval: getEnvDuration("STORAGE_REGULAR_SAMPLE_INTERVAL", time.Minute),
		},
		Metrics: MetricsConfig{
			Enabled: getEnvBool("METRICS_ENABLED", true),
			Path:    getEnvString("METRICS_PATH", "/metrics"),
		},
	}

	// TODO: Load from YAML file if provided and merge with env vars
	if configFile != "" {
		// YAML loading would go here
		fmt.Printf("YAML config loading not yet implemented for file: %s\n", configFile)
	}

	return cfg, cfg.Validate()
}

// Environment variable helper functions
func getEnvString(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			return parsed
		}
	}
	return defaultValue
}

func getEnvInt64(key string, defaultValue int64) int64 {
	if value := os.Getenv(key); value != "" {
		if parsed, err := strconv.ParseInt(value, 10, 64); err == nil {
			return parsed
		}
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if parsed, err := strconv.ParseBool(value); err == nil {
			return parsed
		}
	}
	return defaultValue
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if parsed, err := time.ParseDuration(value); err == nil {
			return parsed
		}
	}
	return defaultValue
}
