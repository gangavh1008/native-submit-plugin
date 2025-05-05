# Native Submit Plugin for Spark Operator

A high-performance alternative to `spark-submit` for launching Spark applications via the Spark Operator in Kubernetes clusters. This plugin eliminates the JVM spin-up overhead associated with traditional `spark-submit` commands, providing faster application startup times.

## Features

- 🚀 Native implementation bypassing JVM overhead
- ⚡ Faster Spark application startup
- 🔧 Flexible configuration options
- 🔒 Secure execution environment
- 📊 Resource management and optimization
- 🔄 Support for various Spark application types (Java, Scala, Python, R)

## Prerequisites


- Kubernetes cluster
- Spark Operator installed in the cluster
- kubectl configured to access the cluster

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/your-org/native-submit-plugin.git
   cd native-submit-plugin
   ```

2. Build the plugin:
   ```bash
   go build -buildmode=plugin -o plugin.so ./main
   ```

3. Deploy the plugin to your cluster:
   ```bash
   kubectl apply -f deploy/
   ```

## Usage
native-submit will be  plugin to spark operator.

## Docker/RPC Mode

You can also run Native Submit as a standalone RPC server in a Docker container. This exposes the `runAltSparkSubmit` method via Go's net/rpc over TCP.

### Build Docker Image

Create a Dockerfile in the project root with the following content:

```Dockerfile
FROM golang:1.21-alpine as builder
WORKDIR /app
COPY . .
RUN go build -o native-submit ./main

FROM alpine:3.18
WORKDIR /app
COPY --from=builder /app/native-submit .
ENTRYPOINT ["/app/native-submit"]
```

Build the image:

```bash
docker build -t native-submit:latest .
```

### Run the RPC Server

```bash
docker run -e NATIVE_SUBMIT_MODE=rpc -p 12345:12345 native-submit:latest
```

### RPC API

- Method: `RunAltSparkSubmit`
- Request: `{ App: <JSON-encoded v1beta2.SparkApplication>, SubmissionID: <string> }`
- Response: `{ Success: <bool>, Error: <string> }`

You can use any Go net/rpc client to call this method. The server listens on TCP port 12345.

## Architecture

The plugin consists of several components:

- `common/`: Shared utilities and constants
- `driver/`: Driver pod management
- `service/`: Core service implementation
- `configmap/`: Configuration management
- `main/`: Plugin entry point


### Building

```bash
# Build the plugin
go build -buildmode=plugin -o plugin.so ./main

# Run tests
go test -v ./...
```

### Testing

```bash
# Run unit tests
go test -v ./...

# Run tests with coverage
go test -cover ./...

# Run specific package tests
go test -v ./pkg/...
```
