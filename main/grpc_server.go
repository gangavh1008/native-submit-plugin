package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "nativesubmit/proto/spark"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"google.golang.org/grpc"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Helper: Convert proto SparkApplication to v1beta2.SparkApplication
func convertProtoToSparkApplication(protoApp *pb.SparkApplication) *v1beta2.SparkApplication {
	if protoApp == nil {
		return nil
	}
	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:        protoApp.GetMetadata().GetName(),
			Namespace:   protoApp.GetMetadata().GetNamespace(),
			Labels:      protoApp.GetMetadata().GetLabels(),
			Annotations: protoApp.GetMetadata().GetAnnotations(),
		},
		Spec: v1beta2.SparkApplicationSpec{
			Arguments: protoApp.GetSpec().GetArguments(),
		},
	}

	return app
}

type server struct {
	pb.UnimplementedSparkSubmitServiceServer
}

func (s *server) RunAltSparkSubmit(ctx context.Context, req *pb.RunAltSparkSubmitRequest) (*pb.RunAltSparkSubmitResponse, error) {
	start := time.Now()

	app := convertProtoToSparkApplication(req.GetSparkApplication())
	success, err := runAltSparkSubmit(app, req.GetSubmissionId())

	// Record metrics
	appType := "unknown"
	if app != nil && app.Spec.Type != "" {
		appType = string(app.Spec.Type)
	}
	RecordSparkApplicationMetrics(appType, success, time.Since(start))

	if err != nil {
		return &pb.RunAltSparkSubmitResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}
	return &pb.RunAltSparkSubmitResponse{
		Success:      success,
		ErrorMessage: "",
	}, nil
}

// HTTP health check handler
func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// readinessCheckHandler checks if the service is ready to serve requests
func readinessCheckHandler(w http.ResponseWriter, r *http.Request) {
	// Add any readiness checks here (e.g., database connectivity, dependencies)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Ready"))
}

func main() {
	// Get port from environment or use default
	port := os.Getenv("GRPC_PORT")
	if port == "" {
		port = "50051"
	}

	// Get health check port from environment or use default
	healthPort := os.Getenv("HEALTH_PORT")
	if healthPort == "" {
		healthPort = "8080"
	}

	// Start HTTP health check server
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/healthz", healthCheckHandler)
		mux.HandleFunc("/readyz", readinessCheckHandler)

		log.Printf("Health check server listening on :%s", healthPort)
		if err := http.ListenAndServe(":"+healthPort, mux); err != nil {
			log.Fatalf("failed to start health check server: %v", err)
		}
	}()

	// Start gRPC server
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(metricsUnaryInterceptor()),
	)
	pb.RegisterSparkSubmitServiceServer(grpcServer, &server{})

	log.Printf("gRPC server listening on :%s", port)

	// Graceful shutdown
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down gRPC server...")
	grpcServer.GracefulStop()
	log.Println("Server stopped")
}
