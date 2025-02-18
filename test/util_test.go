package test

import (
	"github.com/kubeflow/spark-operator/api/v1beta2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"nativesubmit"

	//"k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"testing"
)

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
		err := nativesubmit.createConfigMapUtil(test.configMapName, test.app, test.configMapData, fakeClient)
		if err != nil {
			t.Errorf("Unit test for createConfigMapUtil() function failed with error : %v", err)
		}

	}
	testcases := []testcase{
		{
			app:           TestApp,
			configMapName: "test-app-driver-config-map",
			configMapData: map[string]string{"app-name": "test-app"},
		},
	}
	for _, test := range testcases {
		testFn(test, t)
	}

}

func TestGetAppNamespaceNTestGetResourceLabels(t *testing.T) {
	testcases := []testcase{
		{
			app: TestApp,
		},
		{
			app: TestApp,
		},
	}
	testFn := func(test testcase, t *testing.T) {
		namespace := nativesubmit.GetAppNamespace(test.app)
		if namespace == "" {
			t.Fatalf(`Unit test for getAppNamespace() function failed`)
		}
		appLabels := nativesubmit.getResourceLabels(test.app)
		if test.app.Status.SubmissionID == "test-submission-id-0001" && appLabels["sparkoperator.k8s.io/submission-id"] != "test-submission-id-0001" {
			t.Fatalf(`Unit test for getResourceLabels() function failed`)
		}
	}
	for index, test := range testcases {
		if index == 0 {
			test.app.Namespace = ""
			test.app.Spec.SparkConf["spark.kubernetes.namespace"] = "fs-flowsnake"
			test.app.Status.SubmissionID = "test-submission-id-0001"
		}
		testFn(test, t)
	}

}

func TestSelectSparkContainer(t *testing.T) {
	var pod corev1.Pod
	pod.Name = "driver"
	pod.Spec.Containers = []corev1.Container{
		{
			Name: "dummy-container-2",
		},
		{
			Name: "spark-kubernetes-driver",
		},
		{
			Name: "dummy-container",
		},
	}
	podWithSelectedContainer := nativesubmit.selectSparkContainer(pod, "spark-kubernetes-driver")
	if podWithSelectedContainer.Spec.Containers[0].Name != "spark-kubernetes-driver" {
		t.Fatalf(`Unit test for selectSparkContainer() function failed`)
	}

}

func TestRandomHex(t *testing.T) {
	_, error := nativesubmit.randomHex(10)
	if error != nil {
		t.Fatalf(`Unit test for randomHex() function failed with error: %v`, error)
	}
}

func TestDoFetchFile(t *testing.T) {
	filePath := "file//a.txt"
	ftpPath := "ftp://localhost/a.txt"
	httpFile := "http://test.com/a.txt"
	httpsFile := "https://test.com/a.txt"
	sparkFile := "spark://a.txt"
	localFilePath := "local//a.txt"
	fileName := "a.txt"

	sparkConf := make(map[string]string)
	_, err := nativesubmit.doFetchFile(filePath, nativesubmit.createTempDir(), fileName, sparkConf)
	if err != nil {
		t.Log("Error occcurred while testing doFetchFile function")
	}
	_, err = nativesubmit.downloadFile(filePath, nativesubmit.createTempDir(), sparkConf)
	if err != nil {
		t.Log("Error occcurred while testing downloadFile function")
	}
	_, err = nativesubmit.doFetchFile(ftpPath, nativesubmit.createTempDir(), fileName, sparkConf)
	if err != nil {
		t.Log("Error occcurred while testing doFetchFile function")
	}
	_, err = nativesubmit.downloadFile(ftpPath, nativesubmit.createTempDir(), sparkConf)
	if err != nil {
		t.Log("Error occcurred while testing downloadFile function")
	}
	_, err = nativesubmit.doFetchFile(httpFile, nativesubmit.createTempDir(), fileName, sparkConf)
	if err != nil {
		t.Log("Error occcurred while testing doFetchFile function")
	}
	_, err = nativesubmit.downloadFile(httpFile, nativesubmit.createTempDir(), sparkConf)
	if err != nil {
		t.Log("Error occcurred while testing downloadFile function")
	}
	_, err = nativesubmit.doFetchFile(httpsFile, nativesubmit.createTempDir(), fileName, sparkConf)
	if err != nil {
		t.Log("Error occcurred while testing doFetchFile function")
	}
	_, err = nativesubmit.downloadFile(httpsFile, nativesubmit.createTempDir(), sparkConf)
	if err != nil {
		t.Log("Error occcurred while testing downloadFile function")
	}
	_, err = nativesubmit.doFetchFile(sparkFile, nativesubmit.createTempDir(), fileName, sparkConf)
	if err != nil {
		t.Log("Error occcurred while testing doFetchFile function")
	}
	_, err = nativesubmit.downloadFile(sparkFile, nativesubmit.createTempDir(), sparkConf)
	if err != nil {
		t.Log("Error occcurred while testing downloadFile function")
	}
	_, err = nativesubmit.downloadFile(localFilePath, nativesubmit.createTempDir(), sparkConf)
	if err != nil {
		t.Log("Error occcurred while testing downloadFile function")
	}

}
func TestGetServiceName(t *testing.T) {

	type testcase struct {
		app *v1beta2.SparkApplication
	}

	testFn := func(test testcase, t *testing.T) {
		serviceName := nativesubmit.getServiceName(test.app)
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

var getServiceNameFunctionTestData1 = &v1beta2.SparkApplication{
	Spec: v1beta2.SparkApplicationSpec{
		Type: v1beta2.SparkApplicationTypeScala,
		Driver: v1beta2.DriverSpec{
			PodName: nativesubmit.StringPointer("abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"),
		},
	},
}

var getServiceNameFunctionTestData2 = &v1beta2.SparkApplication{
	Spec: v1beta2.SparkApplicationSpec{
		Type: v1beta2.SparkApplicationTypeScala,
		Driver: v1beta2.DriverSpec{
			PodName: nativesubmit.StringPointer("abcdefghijk"),
		},
	},
}

func TestGetDriverPodName(t *testing.T) {
	testcases := []testcase{
		{
			app: TestApp,
		},
		{
			app: TestApp,
		},
	}
	testFn := func(test testcase, t *testing.T) {
		podName := nativesubmit.getDriverPodName(test.app)
		if podName == "" {
			t.Fatalf(`Unit test for getDriverPodName() failed`)
		}
	}
	for index, test := range testcases {
		if index == 0 {
			test.app.Spec.Driver.PodName = stringPointer("")
			test.app.Spec.SparkConf["spark.kubernetes.driver.pod.name"] = "test-app-driver"
		}
		testFn(test, t)
	}

}

func TestAddLocalDirConfOptions(t *testing.T) {
	testFn := func(test testcase, t *testing.T) {
		_, err := nativesubmit.addLocalDirConfOptions(test.app)
		if err != nil {
			t.Fatalf(`Unit test for addLocalDirConfOptions() failed`)
		}

	}
	testcases := TestCasesList
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
			test.app.Spec.Volumes = append(test.app.Spec.Volumes, sparkLocalDirVolume)
			sparkLocalDirVolumeMount := corev1.VolumeMount{
				MountPath: "/etc/ccp/lldc/applogs",
				Name:      "spark-local-dir-1",
			}
			test.app.Spec.Driver.VolumeMounts = append(test.app.Spec.Driver.VolumeMounts, sparkLocalDirVolumeMount)
			test.app.Spec.Executor.VolumeMounts = append(test.app.Spec.Executor.VolumeMounts, sparkLocalDirVolumeMount)
		}
		if index == 1 {
			test.app.Spec.SparkConf["spark.driver.blockManager.port"] = "7079"
		}
		if index == 2 {
			test.app.Spec.SparkConf["spark.blockManager.port"] = "7079"
		}
		if index == 1 {
			test.app.Spec.SparkConf["spark.driver.blockManager.port"] = "test-val"
		}
		if index == 2 {
			test.app.Spec.SparkConf["spark.blockManager.port"] = "test-val"
		}
		testFn(test, t)
	}

}

type testcase struct {
	app *v1beta2.SparkApplication
}

var TestCasesList = []testcase{
	{
		app: TestApp,
	},
	{
		app: TestApp,
	},
	{
		app: TestApp,
	},
	{
		app: TestApp,
	},
	{
		app: TestApp,
	},
}

func indexedProcessing(index int, test testcase) {
	if index == 1 {
		test.app.Spec.SparkConf["spark.kubernetes.local.dirs.tmpfs"] = "false"
		test.app.Spec.SparkConf["spark.kubernetes.driver.SPARK_LOCAL_DIRS"] = "/tmp/spark-local-dir-2,/tmp/spark-local-dir-1,/tmp/spark-local-dir-100"
	}
	if index == 0 {
		test.app.Spec.SparkConf["spark.kubernetes.local.dirs.tmpfs"] = "true"
	}
	if index == 2 {
		test.app.Spec.Volumes = nil
		test.app.Spec.Driver.VolumeMounts = nil
		test.app.Spec.SparkConf["spark.kubernetes.local.dirs.tmpfs"] = "true"
	}
}
