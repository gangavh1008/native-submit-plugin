package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/rpc"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type RunAltSparkSubmitRequest struct {
	App          json.RawMessage // JSON-encoded v1beta2.SparkApplication
	SubmissionID string
}

type RunAltSparkSubmitResponse struct {
	Success bool
	Error   string
}

func main() {
	// Minimal SparkApplication for testing
	app := v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-app",
			Namespace: "default",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Type: "Python",
			Mode: "cluster",
		},
	}
	appBytes, err := json.Marshal(app)
	if err != nil {
		log.Fatalf("failed to marshal app: %v", err)
	}

	req := RunAltSparkSubmitRequest{
		App:          appBytes,
		SubmissionID: "test-submission-id",
	}
	var resp RunAltSparkSubmitResponse

	client, err := rpc.Dial("tcp", "localhost:12345")
	if err != nil {
		log.Fatalf("failed to connect to RPC server: %v", err)
	}
	defer client.Close()

	err = client.Call("RPCServer.RunAltSparkSubmit", req, &resp)
	if err != nil {
		log.Fatalf("RPC call failed: %v", err)
	}

	fmt.Printf("Success: %v\nError: %s\n", resp.Success, resp.Error)
}
