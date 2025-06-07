.PHONY: build clean clean-all run-write run-read test proto docker-build docker-run compose-up compose-down

# Build both services
build: build-write build-read

# Build remote write service
build-write:
	go build -o bin/remote-write ./cmd/remote-write

# Build remote read service
build-read:
	go build -o bin/remote-read ./cmd/remote-read

# Run remote write service
run-write: build-write
	./bin/remote-write

# Run remote read service
run-read: build-read
	./bin/remote-read

# Generate protobuf files (requires protoc and protoc-gen-go)
proto:
	protoc --go_out=. --go_opt=paths=source_relative proto/remote.proto

# Run tests
test:
	go test -v ./...

# Clean build artifacts
clean:
	rm -rf bin/
	rm -rf dist/
	rm -f *.log
	rm -f *.prof
	rm -f *.test
	rm -f coverage.out
	rm -rf tmp/

# Clean everything including Docker
clean-all: clean compose-down
	docker system prune -f
	docker volume prune -f

# Install dependencies
deps:
	go mod tidy
	go mod download

# Development setup
dev-setup: deps
	@echo "Setting up development environment..."
	@echo "Make sure you have protoc and protoc-gen-go installed for proto generation"

# Docker targets
docker-build:
	docker build -t cb-remote-write:latest .

# Docker Compose targets
COMPOSE_FILE := compose
compose-up:
	cd deploy && docker compose -f ${COMPOSE_FILE}.yml up -d

compose-down:
	cd deploy && docker compose -f ${COMPOSE_FILE}.yml down

compose-logs:
	cd deploy && docker compose -f ${COMPOSE_FILE}.yml logs -f

# Test with local Couchbase
test-local: compose-up
	@echo "Waiting for services to be ready..."
	@sleep 30
	@echo "Testing health endpoint..."
	curl -f http://localhost:8080/health
	@echo "Testing ready endpoint..."
	curl -f http://localhost:8080/ready
