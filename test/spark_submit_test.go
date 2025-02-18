package test

import (
	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"nativesubmit"

	//"k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"testing"
)

func TestRunAltSparkSubmit(t *testing.T) {
	submissionID := uuid.New().String()
	//fakeClient := fake.NewSimpleClientset()
	scheme := runtime.NewScheme()
	utilruntime.Must(corev1.AddToScheme(scheme))
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		Build()

	test.TestApp.Spec.SparkConf["spark.kubernetes.driver.label.test"] = "test-label"
	test.TestApp.Spec.SparkConf["spark.kubernetes.driver.label.testa"] = "test-label-a"
	requestSucceeded, errRunAltSparkSubmit := nativesubmit.RunAltSparkSubmit(test.TestApp, submissionID, fakeClient)
	if errRunAltSparkSubmit != nil || !requestSucceeded {
		t.Fatalf("failed to run spark-submit: %v", errRunAltSparkSubmit)
	}
}
