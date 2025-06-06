# Couchbase Prometheus Remote Storage (Personal)

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
│   ├── processor/            # Data processing logic
│   └── metrics/              # Internal metrics
├── internal/server/          # HTTP server implementations
├── proto/                    # Protobuf definitions
├── deploy/                   # Deployment configurations
└── test/                     # Integration tests
```

## Services

### Remote Write Service
- Receives Prometheus remote write requests
- Processes and stores time series data to a Couchbase cluster
- Supports both Prometheus (Snappy) and VictoriaMetrics (zstd) protocols

### Remote Read Service (Not yet implemented)
- Handles Prometheus remote read queries
- Retrieves time series data from Couchbase

## Configuration

The service supports configuration via environment variables or YAML files:

### Environment Variables (recommended for production)

```bash
# Couchbase Connection (for Capella)
export COUCHBASE_CONNECTION_STRING="couchbase://localhost"
export COUCHBASE_USERNAME="your-username"
export COUCHBASE_PASSWORD="your-password"
export COUCHBASE_BUCKET="metrics"
```

See `config/env.example` for a complete example.

## Deployment

### Local Development with Docker Compose

```bash
# Start Remote Write + vmagent
cd deploy
docker-compose up -d

# Check health
curl http://localhost:8080/health
curl http://localhost:8080/ready
```

### Production Deployment

Services can be deployed independently:
- Scale remote write based on ingestion load
- Scale remote read based on query load
- Different resource allocations per service type

### vmagent Configuration

Configure vmagent to send metrics to the remote write service:

```yaml
# vmagent.yml
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
