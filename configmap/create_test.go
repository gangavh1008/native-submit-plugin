package configmap

import (
	"context"
	"nativesubmit/common"
	"testing"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	apiv1 "k8s.io/api/core/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	ctrlClient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

var memoryQuantity resource.Quantity
var cpuQuantity resource.Quantity

func TestCreateSparkAppConfigMap(t *testing.T) {

	type testcase struct {
		app                  *v1beta2.SparkApplication
		submissionID         string
		createdApplicationId string
		driverConfigMapName  string
	}

	// Create a fake client
	scheme := runtime.NewScheme()
	utilruntime.Must(corev1.AddToScheme(scheme))
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		Build()

	testFn := func(test testcase, t *testing.T) {
		err := Create(test.app, test.submissionID, test.createdApplicationId, fakeClient, test.driverConfigMapName, "test")
		if err != nil {
			t.Errorf("failed to create configmap: %v", err)
		}
		configMap := &apiv1.ConfigMap{}
		configMap.Name = "test-app-driver-configmap"
		configMap.Namespace = "default"
		err = fakeClient.Get(context.TODO(), ctrlClient.ObjectKeyFromObject(configMap), configMap)
		if err != nil && configMap.Data != nil {
			t.Errorf("failed to get ConfigMap %s: %v", "test-app-driver-configmap", err)
		}
	}
	testcases := []testcase{
		{
			app:                  common.TestApp,
			submissionID:         "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			createdApplicationId: "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			driverConfigMapName:  "test-app-driver-configmap",
		},
	}
	for _, test := range testcases {
		testFn(test, t)
	}

}

func TestBuildAltSubmissionCommandArgs(t *testing.T) {

	type testcase struct {
		app                  *v1beta2.SparkApplication
		driverPodName        string
		submissionID         string
		createdApplicationId string
	}
	testFn := func(test testcase, t *testing.T) {
		_, err := buildAltSubmissionCommandArgs(test.app, test.driverPodName, test.submissionID, test.createdApplicationId, "testservicename")
		if err != nil {
			t.Errorf("failed to build Spark Application Submission Arguments: %v", err)
		}
	}
	testcases := []testcase{
		{
			app:                  common.TestApp,
			submissionID:         "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			createdApplicationId: "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			driverPodName:        "test-app-driver",
		},
		{
			app:                  common.TestApp,
			submissionID:         "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			createdApplicationId: "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			driverPodName:        "test-app-driver",
		},
		{
			app:                  common.TestApp,
			submissionID:         "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			createdApplicationId: "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			driverPodName:        "test-app-driver",
		},
		{
			app:                  common.TestApp,
			submissionID:         "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			createdApplicationId: "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			driverPodName:        "test-app-driver",
		},
	}
	for index, test := range testcases {
		if index == 0 {
			test.app.Spec.Driver.Cores = nil
			test.app.Spec.Driver.CoreRequest = nil
			test.app.Spec.Driver.CoreLimit = nil
			test.app.Spec.Executor.CoreRequest = nil
			test.app.Spec.Executor.Cores = nil
			test.app.Spec.Executor.CoreLimit = nil
			test.app.Spec.Driver.Secrets = []v1beta2.SecretInfo{
				{
					Name: "f8d3bdf3-a20b-448c-b2ae-bf14ba4bffc6",
					Path: "/etc/ccp-secrets",
					Type: "GCPServiceAccount",
				},
				{Name: "test", Path: "etc/ccp-secrets", Type: "HadoopDelegationToken"},
			}

			test.app.Spec.Executor.Image = common.StringPointer("dummy-placer.dkr.ecr.us-west-2.amazonaws.com/basic-spark-test-3.3.2:1")
			test.app.Spec.Driver.EnvSecretKeyRefs = map[string]v1beta2.NameKey{"test": {Key: "test", Name: "test"}}
			test.app.Spec.Executor.EnvSecretKeyRefs = map[string]v1beta2.NameKey{"test": {Key: "test", Name: "test"}}

		}
		if index == 1 {
			test.app.Spec.Executor.CoreRequest = common.StringPointer("1")
		}
		if index == 2 {
			test.app.Spec.SparkConf["spark.kubernetes.executor.request.cores"] = "1200"
			test.app.Spec.SparkConf["spark.driver.cores"] = "1200"
			test.app.Spec.SparkConf["spark.kubernetes.driver.request.cores"] = "1200"
			test.app.Spec.SparkConf["spark.kubernetes.driver.limit.cores"] = "1200"
			test.app.Spec.SparkConf["spark.executor.cores"] = "1200"
			test.app.Spec.SparkConf["spark.executor.limit.cores"] = "1200"
			test.app.Spec.Executor.MemoryOverhead = common.StringPointer("0.2")
			test.app.Spec.Type = v1beta2.SparkApplicationTypePython
			test.app.Spec.Executor.ServiceAccount = common.StringPointer("fit-driver-serviceaccount")
			test.app.Spec.Executor.DeleteOnTermination = common.BoolPointer(true)
		}
		if index == 3 {
			test.app.Spec.Executor.Memory = nil
			test.app.Spec.Type = v1beta2.SparkApplicationTypeR
			test.app.Spec.Executor.EnvVars = map[string]string{
				"Name":  "Dummy-env",
				"Value": "dummy-env-val",
			}
		}
		testFn(test, t)
	}

}
func TestCreateSparkAppConfigMapUtil(t *testing.T) {
	//CreateConfigMapUtil(configMapName string, app *v1beta2.SparkApplication, configMapData map[string]string, kubeClient kubernetes.Interface) error

	type testcase struct {
		app           *v1beta2.SparkApplication
		configMapName string
		configMapData map[string]string
	}

	scheme := runtime.NewScheme()
	utilruntime.Must(corev1.AddToScheme(scheme))
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		Build()
	//fakeClient := fake.NewSimpleClientset()
	testFn := func(test testcase, t *testing.T) {
		err := createConfigMapUtil(test.configMapName, test.app, test.configMapData, fakeClient)
		if err != nil {
			t.Errorf("Unit test for createConfigMapUtil() function failed with error : %v", err)
		}

	}
	testcases := []testcase{
		{
			app:           common.TestApp,
			configMapName: "test-app-driver-config-map",
			configMapData: map[string]string{"app-name": "test-app"},
		},
	}
	for _, test := range testcases {
		testFn(test, t)
	}

}

func int64Pointer(a int64) *int64 {
	return &a
}

func booleanPointer(a bool) *bool {
	return &a
}

func TestAddLocalDirConfOptions(t *testing.T) {
	testFn := func(test common.Testcase, t *testing.T) {
		_, err := addLocalDirConfOptions(test.App)
		if err != nil {
			t.Fatalf(`Unit test for addLocalDirConfOptions() failed`)
		}

	}
	testcases := common.TestCasesList
	for index, test := range testcases {
		if index == 0 {
			//Add spark-local-dir- prefix volume
			sparkLocalDirVolume := corev1.Volume{
				Name: "spark-local-dir-1",
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{
						Medium: "Memory",
					},
				},
			}
			test.App.Spec.Volumes = append(test.App.Spec.Volumes, sparkLocalDirVolume)
			sparkLocalDirVolumeMount := corev1.VolumeMount{
				MountPath: "/etc/ccp/lldc/applogs",
				Name:      "spark-local-dir-1",
			}
			test.App.Spec.Driver.VolumeMounts = append(test.App.Spec.Driver.VolumeMounts, sparkLocalDirVolumeMount)
			test.App.Spec.Executor.VolumeMounts = append(test.App.Spec.Executor.VolumeMounts, sparkLocalDirVolumeMount)
		}
		if index == 1 {
			test.App.Spec.SparkConf["spark.driver.blockManager.port"] = "7079"
		}
		if index == 2 {
			test.App.Spec.SparkConf["spark.blockManager.port"] = "7079"
		}
		if index == 1 {
			test.App.Spec.SparkConf["spark.driver.blockManager.port"] = "test-val"
		}
		if index == 2 {
			test.App.Spec.SparkConf["spark.blockManager.port"] = "test-val"
		}
		testFn(test, t)
	}

}
