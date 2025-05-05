package main

import (
	"context"
	"log"
	"net"

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
			Deps:      protoApp.GetSpec().GetDeps(),
		},
	}

	return app
}

type server struct {
	pb.UnimplementedSparkSubmitServiceServer
}

func (s *server) RunAltSparkSubmit(ctx context.Context, req *pb.RunAltSparkSubmitRequest) (*pb.RunAltSparkSubmitResponse, error) {
	app := convertProtoToSparkApplication(req.GetSparkApplication())
	success, err := runAltSparkSubmit(app, req.GetSubmissionId())
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

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterSparkSubmitServiceServer(grpcServer, &server{})
	log.Println("gRPC server listening on :50051")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
