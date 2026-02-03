# Couchbase Prometheus Remote Storage

A basic Prometheus-compatible remote storage implementation writng to a Couchbase cluster.

## Architecture

This project provides separate services for remote write and remote read, which can be deployed and scaled separately:

- **Remote Write Service** (`cmd/remote-write/`) - Receives metrics from Prometheus/vmagent
- **Remote Read Service** (`cmd/remote-read/`) - Serves queries from Prometheus/Grafana
- **Shared Libraries** (`pkg/`) - Common functionality used by both services

## Project Structure

```
├── cmd/
│   ├── remote-write/          # Remote write service
│   └── remote-read/           # Remote read service
├── pkg/
│   ├── config/               # Configuration management
│   ├── protocol/             # Prometheus protocol handling
│   ├── storage/              # Couchbase storage interface using the Go SDK
├── internal/server/          # HTTP server implementations
├── proto/                    # Protobuf definitions
├── deploy/                   # Docker deployment configurations
└── test/                     # Integration tests
```


## Time Series Storage: Regular vs Irregular

This project supports two storage modes for time series data:

### Irregular Series (default)
- Each sample is stored as a `[timestamp, value]` pair.
- Documents can contain samples with arbitrary, non-uniform intervals.
- Best for metrics with missing data points or variable scrape intervals.
- Data is stored as an array of pairs: `[[ts1, v1], [ts2, v2], ...]`.

### Regular Series
- Samples are stored as a dense array of values, with a fixed interval between them.
- Each document covers a fixed time window (e.g., 1 hour), and each slot represents a regular interval (e.g., 1 minute).
- More efficient for high-cardinality, regular metrics.
- Data is stored as an array of values: `[v1, v2, v3, ...]` with metadata for start, end, and interval.

The storage mode is controlled by the `STORAGE_TIMESERIES_TYPE` setting (`irregular` or `regular`).

---

## Services

### Remote Write Service
- Receives Prometheus remote write requests
- Processes and stores time series data to a Couchbase cluster
- Supports both Snappy and zstd compression.

### Remote Read Service (Not yet implemented)
- Handles Prometheus remote read queries
- Retrieves time series data from Couchbase

---

## Key Storage Settings

The following environment variables (or YAML config keys) control storage behavior:

- `STORAGE_TIMESERIES_TYPE`: `irregular` (default) or `regular`. Controls storage format.
- `STORAGE_TIMESERIES_INTERVAL`: Time window for each document (e.g., `1h`). All values that happens within the interval will reside in the same JSON document.
- `STORAGE_REGULAR_SAMPLE_INTERVAL`: Interval between samples in regular mode (e.g., `1m`).
- `STORAGE_BATCH_SIZE`: Number of time series to buffer before flushing to the server for storage.
- `STORAGE_FLUSH_INTERVAL`: Max time to wait before flushing a batch.
- `STORAGE_DOCUMENT_SIZE_LIMIT`: Max document size in bytes.
- `STORAGE_RETENTION_PERIOD`: How long documents are retained. This is set as a Time-to-live (TTL) for the data.


See `pkg/config/config.go` for all available options and defaults.

---

## Configuration

The service supports configuration via environment variables or YAML files:

### Environment Variables

```bash
# Couchbase Connection
export COUCHBASE_CONNECTION_STRING="couchbase://localhost"
export COUCHBASE_USERNAME="your-username"
export COUCHBASE_PASSWORD="your-password"
export COUCHBASE_BUCKET="metrics"
```

See `config/env.example` for a complete example.

## Deployment

### Local Development with Docker Compose
1. In deploy directory, copy both compose.yml and vmagent-config.yml

```bash
# Create your local deployment configs
cd deploy
cp compose.yml compose.local.yml
cp config.yml config.local.yml
```
They update your local configs as you wish. You can then start the services with docker compose.
```bash
# Start Remote Write + vmagent
cd deploy && docker compose -f compose.local.yml up -d
# Or using make (on top directory):
make compose-up COMPOSE_FILE=compose.local

# Check health
curl http://localhost:8080/health
curl http://localhost:8080/ready
```

### Production Deployment

Services can be deployed independently:
- Scale remote write based on ingestion load
- Scale remote read based on query load
- Different resource allocations per service type

### Prometheus Agent Configuration

Configure Prometheus (or other compartible senders) to send metrics to the remote write service:

```yaml
global:
  scrape_interval: 60s

scrape_configs:
  - job_name: 'my-app'
    static_configs:
      - targets: ['app:8080']

# Point to your remote write service
remoteWrite:
  url: "http://cb-remote-write:8080/api/v1/write"
```
