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

	"github.com/google/uuid"
	"github.com/kubeflow/spark-operator/api/v1beta2"
	"google.golang.org/grpc"
	wrapperspb "google.golang.org/protobuf/types/known/wrapperspb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Helper: Convert proto SparkApplication to v1beta2.SparkApplication
func convertProtoToSparkApplication(protoApp *pb.SparkApplication) *v1beta2.SparkApplication {
	if protoApp == nil {
		return nil
	}

	// Convert SparkApplicationType enum to string
	var appType v1beta2.SparkApplicationType
	switch protoApp.GetSpec().GetType() {
	case pb.SparkApplicationType_SPARK_APPLICATION_TYPE_JAVA:
		appType = v1beta2.SparkApplicationTypeJava
	case pb.SparkApplicationType_SPARK_APPLICATION_TYPE_SCALA:
		appType = v1beta2.SparkApplicationTypeScala
	case pb.SparkApplicationType_SPARK_APPLICATION_TYPE_PYTHON:
		appType = v1beta2.SparkApplicationTypePython
	case pb.SparkApplicationType_SPARK_APPLICATION_TYPE_R:
		appType = v1beta2.SparkApplicationTypeR
	default:
		appType = v1beta2.SparkApplicationTypeJava // default
	}

	// Convert DeployMode enum to string
	var deployMode v1beta2.DeployMode
	switch protoApp.GetSpec().GetMode() {
	case pb.DeployMode_DEPLOY_MODE_CLUSTER:
		deployMode = v1beta2.DeployModeCluster
	case pb.DeployMode_DEPLOY_MODE_CLIENT:
		deployMode = v1beta2.DeployModeClient
	case pb.DeployMode_DEPLOY_MODE_IN_CLUSTER_CLIENT:
		deployMode = v1beta2.DeployModeInClusterClient
	default:
		deployMode = v1beta2.DeployModeCluster // default
	}

	// Helper function to convert wrapper to string pointer
	getStringPtr := func(wrapper *wrapperspb.StringValue) *string {
		if wrapper != nil && wrapper.GetValue() != "" {
			val := wrapper.GetValue()
			return &val
		}
		return nil
	}

	// Helper function to convert wrapper to int32 pointer
	getInt32Ptr := func(wrapper *wrapperspb.Int32Value) *int32 {
		if wrapper != nil {
			val := wrapper.GetValue()
			return &val
		}
		return nil
	}

	// Helper function to convert wrapper to int64 pointer
	getInt64Ptr := func(wrapper *wrapperspb.Int64Value) *int64 {
		if wrapper != nil {
			val := wrapper.GetValue()
			return &val
		}
		return nil
	}

	// Ensure UID is not empty - generate one if needed
	uid := protoApp.GetMetadata().GetUid()
	if uid == "" {
		uid = uuid.New().String()
	}

	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:        protoApp.GetMetadata().GetName(),
			Namespace:   protoApp.GetMetadata().GetNamespace(),
			UID:         types.UID(uid),
			Labels:      protoApp.GetMetadata().GetLabels(),
			Annotations: protoApp.GetMetadata().GetAnnotations(),
		},
		Spec: v1beta2.SparkApplicationSpec{
			Type:                 appType,
			Mode:                 deployMode,
			Image:                getStringPtr(protoApp.GetSpec().GetImage()),
			ImagePullPolicy:      getStringPtr(protoApp.GetSpec().GetImagePullPolicy()),
			ImagePullSecrets:     protoApp.GetSpec().GetImagePullSecrets(),
			SparkConf:            protoApp.GetSpec().GetSparkConf(),
			HadoopConf:           protoApp.GetSpec().GetHadoopConf(),
			SparkConfigMap:       getStringPtr(protoApp.GetSpec().GetSparkConfigMap()),
			HadoopConfigMap:      getStringPtr(protoApp.GetSpec().GetHadoopConfigMap()),
			Arguments:            protoApp.GetSpec().GetArguments(),
			MainClass:            getStringPtr(protoApp.GetSpec().GetMainClass()),
			MainApplicationFile:  getStringPtr(protoApp.GetSpec().GetMainApplicationFile()),
			ProxyUser:            getStringPtr(protoApp.GetSpec().GetProxyUser()),
			FailureRetries:       getInt32Ptr(protoApp.GetSpec().GetFailureRetries()),
			RetryInterval:        getInt64Ptr(protoApp.GetSpec().GetRetryInterval()),
			MemoryOverheadFactor: getStringPtr(protoApp.GetSpec().GetMemoryOverheadFactor()),
			BatchScheduler:       getStringPtr(protoApp.GetSpec().GetBatchScheduler()),
			TimeToLiveSeconds:    getInt64Ptr(protoApp.GetSpec().GetTimeToLiveSeconds()),
			PythonVersion:        getStringPtr(&wrapperspb.StringValue{Value: protoApp.GetSpec().GetPythonVersion()}),
			SparkVersion:         protoApp.GetSpec().GetSparkVersion(),
		},
	}

	// Handle Dependencies
	if deps := protoApp.GetSpec().GetDeps(); deps != nil {
		app.Spec.Deps = v1beta2.Dependencies{
			Jars:            deps.GetJars(),
			Files:           deps.GetFiles(),
			PyFiles:         deps.GetPyFiles(),
			Packages:        deps.GetPackages(),
			ExcludePackages: deps.GetExcludePackages(),
			Repositories:    deps.GetRepositories(),
			Archives:        deps.GetArchives(),
		}
	}

	// Handle DynamicAllocation
	if dynAlloc := protoApp.GetSpec().GetDynamicAllocation(); dynAlloc != nil {
		initialExecutors := dynAlloc.GetInitialExecutors()
		minExecutors := dynAlloc.GetMinExecutors()
		maxExecutors := dynAlloc.GetMaxExecutors()
		shuffleTimeout := dynAlloc.GetShuffleTrackingTimeout()

		app.Spec.DynamicAllocation = &v1beta2.DynamicAllocation{
			Enabled:                dynAlloc.GetEnabled(),
			InitialExecutors:       &initialExecutors,
			MinExecutors:           &minExecutors,
			MaxExecutors:           &maxExecutors,
			ShuffleTrackingTimeout: &shuffleTimeout,
		}
	}

	// Handle RestartPolicy
	if restartPolicy := protoApp.GetSpec().GetRestartPolicy(); restartPolicy != nil {
		app.Spec.RestartPolicy = v1beta2.RestartPolicy{
			Type: v1beta2.RestartPolicyType(restartPolicy.GetType()),
		}
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
	log.Println("Starting native-submit gRPC service...")

	// Get port from environment or use default
	port := os.Getenv("GRPC_PORT")
	if port == "" {
		port = "50051"
	}

	// Get health check port from environment or use default
	healthPort := os.Getenv("HEALTH_PORT")
	if healthPort == "" {
		healthPort = "9090"
	}

	log.Printf("Using gRPC port: %s, Health port: %s", port, healthPort)

	// Create error channels for goroutines
	healthErr := make(chan error, 1)
	grpcErr := make(chan error, 1)

	// Start HTTP health check server
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/healthz", healthCheckHandler)
		mux.HandleFunc("/readyz", readinessCheckHandler)

		log.Printf("Health check server listening on :%s", healthPort)
		if err := http.ListenAndServe(":"+healthPort, mux); err != nil {
			log.Printf("Health check server error: %v", err)
			healthErr <- err
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

	// Start gRPC server in goroutine
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("gRPC server error: %v", err)
			grpcErr <- err
		}
	}()

	// Wait for interrupt signal or server errors
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	log.Println("Service started successfully. Waiting for signals or errors...")

	// Keep the main process running
	select {
	case <-quit:
		log.Println("Received shutdown signal, shutting down gRPC server...")
		grpcServer.GracefulStop()
		log.Println("Server stopped")
	case err := <-healthErr:
		log.Printf("Health server failed: %v", err)
		grpcServer.GracefulStop()
		log.Fatalf("Health server error: %v", err)
	case err := <-grpcErr:
		log.Printf("gRPC server failed: %v", err)
		log.Fatalf("gRPC server error: %v", err)
	}

	// This should never be reached, but just in case
	log.Println("Main process completed unexpectedly")
}
