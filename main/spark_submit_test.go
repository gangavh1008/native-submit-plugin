package main

import (
	"nativesubmit/common"

	"github.com/google/uuid"
	"github.com/kubeflow/spark-operator/api/v1beta2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"

	//"k8s.io/client-go/kubernetes/fake"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestRunAltSparkSubmit(t *testing.T) {
	submissionID := uuid.New().String()
	//fakeClient := fake.NewSimpleClientset()
	scheme := runtime.NewScheme()
	utilruntime.Must(corev1.AddToScheme(scheme))
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		Build()

	common.TestApp.Spec.SparkConf["spark.kubernetes.driver.label.test"] = "test-label"
	common.TestApp.Spec.SparkConf["spark.kubernetes.driver.label.testa"] = "test-label-a"
	requestSucceeded, errRunAltSparkSubmit := runAltSparkSubmit(common.TestApp, submissionID, fakeClient)
	if errRunAltSparkSubmit != nil || !requestSucceeded {
		t.Fatalf("failed to run spark-submit: %v", errRunAltSparkSubmit)
	}
}
func TestGetServiceName(t *testing.T) {

	type testcase struct {
		app *v1beta2.SparkApplication
	}

	testFn := func(test testcase, t *testing.T) {
		serviceName := getServiceName(test.app)
		if serviceName == "" {
			t.Fatalf(`Unit test for getServiceName() function failed`)
		}

	}
	testcases := []testcase{
		{
			app: getServiceNameFunctionTestData1,
		},
		{
			app: getServiceNameFunctionTestData2,
		},
	}
	for _, test := range testcases {
		testFn(test, t)
	}

}
