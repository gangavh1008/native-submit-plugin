package driver

import (
	"nativesubmit/common"
	"nativesubmit/configmap"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	//"github.com/kubeflow/spark-operator/pkg/config"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"

	//"k8s.io/client-go/kubernetes/fake"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestCreateSparkAppDriverPod(t *testing.T) {
	type testcase struct {
		app                  *v1beta2.SparkApplication
		driverConfigMapName  string
		serviceLabels        map[string]string
		submissionID         string
		createdApplicationId string
	}
	//fakeClient := fake.NewSimpleClientset()
	// Create a fake client
	scheme := runtime.NewScheme()
	utilruntime.Must(v1.AddToScheme(scheme))
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		Build()
	testFn := func(test testcase, t *testing.T) {
		errCreateSparkAppConfigMap := configmap.Create(test.app, test.submissionID, test.createdApplicationId, fakeClient, test.driverConfigMapName, "test")
		if errCreateSparkAppConfigMap != nil {
			t.Errorf("failed to create configmap: %v", errCreateSparkAppConfigMap)
		}

		errCreateDriverPod := Create(test.app, test.serviceLabels, test.driverConfigMapName, fakeClient, test.app.Spec.Driver.VolumeMounts, test.app.Spec.Volumes)
		if errCreateDriverPod != nil {
			t.Errorf("failed to create Driver pod: %v", errCreateDriverPod)
		}

	}
	testcases := []testcase{
		{
			app:                  common.TestApp,
			driverConfigMapName:  "test-app-driver-configmap",
			serviceLabels:        map[string]string{SparkAppNameLabel: "test-app"},
			submissionID:         "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			createdApplicationId: "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
		},
	}
	testcases = append(testcases, testcases[0], testcases[0], testcases[0], testcases[0], testcases[0])
	for index, test := range testcases {
		if index == 0 {
			test.app.Spec.SparkConf["spark.kubernetes.driver.podTemplateFile"] = ""
			test.app.Spec.SparkConf["spark.kubernetes.driver.podTemplateContainerName"] = "spark-kubernetes-driver"
			test.app.Spec.Driver.Annotations = nil
			test.app.Spec.SparkConf["spark.kubernetes.node.selector.identifier"] = "testIdentifier"
			test.app.Spec.SparkConf["spark.kubernetes.driver.node.selector.identifier"] = "testIdentifier"
			test.app.Spec.SparkConf["spark.kubernetes.scheduler.name"] = "dummy-scheduler"
			test.app.Spec.Image = nil
			test.app.Spec.SparkConf["spark.kubernetes.container.image"] = "871501607754.dkr.ecr.us-west-2.amazonaws.com/sfci/dva-transformation/spark-on-k8s-sample-apps/flowsnake-basic-operator-integration-spark-3.3.2:jenkins-dva-transformation-spark-on-k8s-sample-apps-spark-3.3.2-sfdc-3-itest"
			test.app.Spec.SparkConf["spark.kubernetes.memoryOverhead"] = "10m"
		}
		if index == 1 {
			test.app.Spec.Driver.SecurityContext = nil
			test.app.Spec.ImagePullPolicy = nil
			test.app.Spec.Image = nil
			test.app.Spec.Driver.Image = common.StringPointer("871501607754.dkr.ecr.us-west-2.amazonaws.com/sfci/dva-transformation/spark-on-k8s-sample-apps/flowsnake-basic-operator-integration-spark-3.3.2:jenkins-dva-transformation-spark-on-k8s-sample-apps-spark-3.3.2-sfdc-3-itest")
			test.app.Spec.Driver.Memory = nil
			test.app.Spec.Driver.CoreLimit = nil
			test.app.Spec.Driver.Cores = nil
			test.app.Spec.MemoryOverheadFactor = common.StringPointer("0.15")
			test.app.Spec.NodeSelector = map[string]string{"identifier": "testIdentifier"}
		}
		if index == 2 {
			test.app.Spec.Driver.NodeSelector = map[string]string{"identifier": "testIdentifier"}
			test.app.Spec.ImagePullSecrets = nil
			test.app.Spec.SparkConf["spark.kubernetes.container.image.pullSecrets"] = "dummy-data"
			test.app.Spec.Driver.ServiceAccount = nil
			test.app.Spec.SparkConf["spark.kubernetes.authenticate.driver.serviceAccountName"] = "fit-driver-account"
			test.app.Spec.SparkConf["spark.kubernetes.driver.scheduler.name"] = "dummy-scheduler"
			test.app.Spec.Driver.TerminationGracePeriodSeconds = common.Int64Pointer(1)
			test.app.Spec.Driver.Tolerations = []v1.Toleration{{Key: "example-key", Operator: "Exists", Effect: "NoSchedule"}}
			test.app.Spec.SparkConf["spark.kubernetes.driver.secrets.test"] = "test-secret"
			test.app.Spec.Driver.InitContainers = []v1.Container{{Name: "test-init-container", Image: "test"}}
			test.app.Spec.Image = nil
			test.app.Spec.Driver.Image = nil
			test.app.Spec.SparkConf["spark.kubernetes.driver.container.image"] = "871501607754.dkr.ecr.us-west-2.amazonaws.com/sfci/dva-transformation/spark-on-k8s-sample-apps/flowsnake-basic-operator-integration-spark-3.3.2:jenkins-dva-transformation-spark-on-k8s-sample-apps-spark-3.3.2-sfdc-3-itest"
			test.app.Spec.ImagePullPolicy = nil
			test.app.Spec.SparkConf["spark.kubernetes.container.image.pullPolicy"] = "IfNotPresent"
			test.app.Spec.SparkConf["spark.driver.memoryOverhead"] = "10m"
		}
		if index == 3 {
			test.app.Spec.Driver.MemoryOverhead = common.StringPointer("10m")
		}

		testFn(test, t)
	}
}
