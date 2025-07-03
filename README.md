# Native Submit Plugin for Spark Operator

A high-performance alternative to `spark-submit` for launching Spark applications via the Spark Operator in Kubernetes clusters. This plugin eliminates the JVM spin-up overhead associated with traditional `spark-submit` commands, providing faster application startup times.

## Features

- 🚀 Native implementation bypassing JVM overhead
- ⚡ Faster Spark application startup
- 🔧 Flexible configuration options
- 🔒 Secure execution environment
- 📊 Resource management and optimization
- 🔄 Support for various Spark application types (Java, Scala, Python, R)
- 🌐 gRPC service with health checks and metrics
- 🐳 Docker containerization with sidecar deployment

## Prerequisites

- Kubernetes cluster
- Spark Operator installed in the cluster
- kubectl configured to access the cluster
- Docker (for containerized deployment)

## Quick Start

### 1. Build and Run with Docker

```bash
# Build the Docker image
docker build -t native-submit:latest .

# Run the gRPC service
docker run -p 50051:50051 -p 8080:8080 native-submit:latest
```

### 2. Test the gRPC Service

```bash
# Run the test client
go run test_grpc_client.go
```

### 3. Deploy as Sidecar with Spark Operator

```bash
# Apply the deployment
kubectl apply -f deploy.yaml
```

## Docker Compose (Development)

For local development and testing:

```bash
# Start the gRPC service
docker-compose up native-submit-grpc

# Run tests (optional)
docker-compose --profile test up grpc-test-client
```

## Architecture

The plugin consists of several components:

- `common/`: Shared utilities and constants
- `driver/`: Driver pod management
- `service/`: Core service implementation
- `configmap/`: Configuration management
- `main/`: gRPC server entry point

### gRPC Service

The gRPC service provides:
- **Port 50051**: gRPC API endpoint
- **Port 8080**: Health check endpoints (`/healthz`, `/readyz`)
- **Metrics**: Prometheus metrics for monitoring
- **Health Checks**: Kubernetes-ready health and readiness probes

### API Endpoints

- **gRPC**: `RunAltSparkSubmit` method for submitting Spark applications
- **HTTP Health**: `GET /healthz` - Service health check
- **HTTP Ready**: `GET /readyz` - Service readiness check

## Usage

### As a Sidecar Container

The gRPC service is designed to run as a sidecar container alongside the Spark Operator controller:

```yaml
containers:
- name: spark-operator-controller
  # Main Spark Operator container
- name: native-submit-grpc
  # gRPC service sidecar
  image: ghcr.io/kubeflow/spark-operator/native-submit:latest
  ports:
  - containerPort: 50051  # gRPC
  - containerPort: 8080   # Health checks
```

### gRPC Client Example

```go
package main

import (
    "context"
    "log"
    pb "nativesubmit/proto/spark"
    "google.golang.org/grpc"
)

func main() {
    conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
    if err != nil {
        log.Fatalf("Failed to connect: %v", err)
    }
    defer conn.Close()

    client := pb.NewSparkSubmitServiceClient(conn)
    
    req := &pb.RunAltSparkSubmitRequest{
        SparkApplication: &pb.SparkApplication{
            Metadata: &pb.ObjectMeta{
                Name:      "test-app",
                Namespace: "default",
            },
            Spec: &pb.SparkApplicationSpec{
                Type: "Python",
                Mode: "cluster",
            },
        },
        SubmissionId: "test-submission-id",
    }

    resp, err := client.RunAltSparkSubmit(context.Background(), req)
    if err != nil {
        log.Fatalf("Call failed: %v", err)
    }

    log.Printf("Success: %v", resp.GetSuccess())
}
```

## Building

```bash
# Build the plugin
go build -o native-submit ./main

# Build Docker image
docker build -t native-submit:latest .

# Run tests
go test -v ./...
```

## Testing

```bash
# Run unit tests
go test -v ./...

# Run tests with coverage
go test -cover ./...

# Test gRPC service
go run test_grpc_client.go

# Test with Docker Compose
docker-compose --profile test up grpc-test-client
```

## Deployment

### Kubernetes Deployment

```bash
# Deploy as sidecar with Spark Operator
kubectl apply -f deploy.yaml

# Check deployment status
kubectl get pods -n spark-operator

# Check service endpoints
kubectl get svc -n spark-operator
```

### Health Checks

The service includes:
- **Liveness Probe**: `GET /healthz` on port 8080
- **Readiness Probe**: `GET /readyz` on port 8080
- **Docker Health Check**: Built into the container

### Monitoring

Prometheus metrics are available at `/metrics` on port 8080:
- `grpc_requests_total`: Total gRPC requests
- `grpc_request_duration_seconds`: Request duration
- `spark_applications_total`: Spark application submissions
- `grpc_active_connections`: Active connections

## Configuration

### Environment Variables

- `GRPC_PORT`: gRPC server port (default: 50051)
- `HEALTH_PORT`: Health check port (default: 8080)

### Kubernetes Configuration

The service is configured to run with:
- Non-root user (UID: 185, GID: 185)
- Resource limits and requests
- Security context with minimal privileges
- Health checks and readiness probes
