package service

import (
	"nativesubmit/common"
	"nativesubmit/configmap"
	"nativesubmit/driver"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"

	//"k8s.io/client-go/kubernetes/fake"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestCreateDriverService(t *testing.T) {
	type testcase struct {
		app                  *v1beta2.SparkApplication
		driverConfigMapName  string
		serviceLabels        map[string]string
		submissionID         string
		createdApplicationId string
	}
	//fakeClient := fake.NewSimpleClientset()
	scheme := runtime.NewScheme()
	utilruntime.Must(corev1.AddToScheme(scheme))
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		Build()
	serviceLabels := map[string]string{SparkAppNameLabel: "test-app"}
	testFn := func(test testcase, t *testing.T) {
		errCreateSparkAppConfigMap := configmap.Create(test.app, test.submissionID, test.createdApplicationId, fakeClient, test.driverConfigMapName, "testservicename")
		if errCreateSparkAppConfigMap != nil {
			t.Errorf("failed to create configmap: %v", errCreateSparkAppConfigMap)
		}

		errCreateDriverPod := driver.Create(test.app, serviceLabels, test.driverConfigMapName, fakeClient, test.app.Spec.Driver.VolumeMounts, test.app.Spec.Volumes)
		if errCreateDriverPod != nil {
			t.Errorf("failed to create Driver pod: %v", errCreateDriverPod)
		}
		err := Create(test.app, serviceLabels, fakeClient, "abcdefg123231kkllkjjlkl", "testservicename")
		if err != nil {
			t.Errorf("failed to create driver service: %v", err)
		}
	}

	common.TestApp.Spec.SparkConf["spark.kubernetes.driver.service.label.test"] = "test-label"
	common.TestApp.Spec.SparkConf["spark.kubernetes.driver.service.label.testa"] = "test-label-a"
	testcases := []testcase{
		{
			app:                  common.TestApp,
			driverConfigMapName:  "test-app-driver-configmap",
			serviceLabels:        map[string]string{SparkAppNameLabel: "test-app"},
			submissionID:         "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			createdApplicationId: "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
		},
	}
	testcases = append(testcases, testcases[0], testcases[0])

	for index, test := range testcases {
		if index == 0 {
			test.app.Spec.SparkConf["spark.kubernetes.driver.service.ipFamilies"] = "IPv4"
		} else if index == 1 {
			test.app.Spec.SparkConf["spark.driver.port"] = "7080"
			test.app.Spec.SparkConf["spark.blockManager.port"] = "8080"
		} else if index == 2 {
			test.app.Spec.SparkConf["spark.driver.port"] = "7080"
			test.app.Spec.SparkConf["spark.driver.blockManager.port"] = "8080"
		}
		testFn(test, t)
	}
}
