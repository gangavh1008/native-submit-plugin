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
	tests := []struct {
		name                 string
		app                  *v1beta2.SparkApplication
		driverConfigMapName  string
		serviceLabels        map[string]string
		submissionID         string
		createdApplicationId string
		setupFunc            func(*v1beta2.SparkApplication)
	}{
		{
			name:                 "basic driver pod creation",
			app:                  common.BaseTestApp(),
			driverConfigMapName:  "test-app-driver-configmap",
			serviceLabels:        map[string]string{SparkAppNameLabel: "test-app"},
			submissionID:         "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			createdApplicationId: "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			setupFunc: func(app *v1beta2.SparkApplication) {
				app.Spec.SparkConf["spark.kubernetes.driver.podTemplateFile"] = ""
				app.Spec.SparkConf["spark.kubernetes.driver.podTemplateContainerName"] = "spark-kubernetes-driver"
				app.Spec.Driver.Annotations = nil
				app.Spec.SparkConf["spark.kubernetes.node.selector.identifier"] = "testIdentifier"
				app.Spec.SparkConf["spark.kubernetes.driver.node.selector.identifier"] = "testIdentifier"
				app.Spec.SparkConf["spark.kubernetes.scheduler.name"] = "dummy-scheduler"
				app.Spec.Image = nil
				app.Spec.SparkConf["spark.kubernetes.container.image"] = "dummy-placer.dkr.ecr.us-west-2.amazonaws.com/basic-spark-test-3.3.2:1"
				app.Spec.SparkConf["spark.kubernetes.memoryOverhead"] = "10m"
			},
		},
		{
			name:                 "driver pod with security context",
			app:                  common.BaseTestApp(),
			driverConfigMapName:  "test-app-driver-configmap",
			serviceLabels:        map[string]string{SparkAppNameLabel: "test-app"},
			submissionID:         "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			createdApplicationId: "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			setupFunc: func(app *v1beta2.SparkApplication) {
				app.Spec.Driver.SecurityContext = nil
				app.Spec.ImagePullPolicy = nil
				app.Spec.Image = nil
				app.Spec.Driver.Image = common.StringPointer("dummy-placer.dkr.ecr.us-west-2.amazonaws.com/basic-spark-test-3.3.2:1")
				app.Spec.Driver.Memory = nil
				app.Spec.Driver.CoreLimit = nil
				app.Spec.Driver.Cores = nil
				app.Spec.MemoryOverheadFactor = common.StringPointer("0.15")
				app.Spec.NodeSelector = map[string]string{"identifier": "testIdentifier"}
			},
		},
		{
			name:                 "driver pod with service account",
			app:                  common.BaseTestApp(),
			driverConfigMapName:  "test-app-driver-configmap",
			serviceLabels:        map[string]string{SparkAppNameLabel: "test-app"},
			submissionID:         "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			createdApplicationId: "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			setupFunc: func(app *v1beta2.SparkApplication) {
				app.Spec.Driver.NodeSelector = map[string]string{"identifier": "testIdentifier"}
				app.Spec.ImagePullSecrets = nil
				app.Spec.SparkConf["spark.kubernetes.container.image.pullSecrets"] = "dummy-data"
				app.Spec.Driver.ServiceAccount = nil
				app.Spec.SparkConf["spark.kubernetes.authenticate.driver.serviceAccountName"] = "driver-account"
				app.Spec.SparkConf["spark.kubernetes.driver.scheduler.name"] = "dummy-scheduler"
				app.Spec.Driver.TerminationGracePeriodSeconds = common.Int64Pointer(1)
				app.Spec.Driver.Tolerations = []v1.Toleration{{Key: "example-key", Operator: "Exists", Effect: "NoSchedule"}}
				app.Spec.SparkConf["spark.kubernetes.driver.secrets.test"] = "test-secret"
				app.Spec.Driver.InitContainers = []v1.Container{{Name: "test-init-container", Image: "test"}}
				app.Spec.Image = nil
				app.Spec.Driver.Image = nil
				app.Spec.SparkConf["spark.kubernetes.driver.container.image"] = "dummy-placer.dkr.ecr.us-west-2.amazonaws.com/basic-spark-test-3.3.2:1"
				app.Spec.ImagePullPolicy = nil
				app.Spec.SparkConf["spark.kubernetes.container.image.pullPolicy"] = "IfNotPresent"
				app.Spec.SparkConf["spark.driver.memoryOverhead"] = "10m"
			},
		},
		{
			name:                 "driver pod with memory overhead",
			app:                  common.BaseTestApp(),
			driverConfigMapName:  "test-app-driver-configmap",
			serviceLabels:        map[string]string{SparkAppNameLabel: "test-app"},
			submissionID:         "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			createdApplicationId: "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			setupFunc: func(app *v1beta2.SparkApplication) {
				app.Spec.Driver.MemoryOverhead = common.StringPointer("10m")
			},
		},
	}

	// Create a fake client
	scheme := runtime.NewScheme()
	utilruntime.Must(v1.AddToScheme(scheme))
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		Build()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup test case
			tt.setupFunc(tt.app)

			// Create configmap
			err := configmap.Create(tt.app, tt.submissionID, tt.createdApplicationId, fakeClient, tt.driverConfigMapName, "test")
			if err != nil {
				t.Fatalf("failed to create configmap: %v", err)
			}

			// Create driver pod
			err = Create(tt.app, tt.serviceLabels, tt.driverConfigMapName, fakeClient, tt.app.Spec.Driver.VolumeMounts, tt.app.Spec.Volumes)
			if err != nil {
				t.Fatalf("failed to create Driver pod: %v", err)
			}
		})
	}
}
