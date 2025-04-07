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
	tests := []struct {
		name                 string
		app                  *v1beta2.SparkApplication
		submissionID         string
		createdApplicationId string
		driverConfigMapName  string
		wantErr              bool
	}{
		{
			name:                 "basic configmap creation",
			app:                  common.BaseTestApp(),
			submissionID:         "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			createdApplicationId: "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			driverConfigMapName:  "test-app-driver-configmap",
			wantErr:              false,
		},
		{
			name:                 "configmap with empty submission ID",
			app:                  common.BaseTestApp(),
			submissionID:         "",
			createdApplicationId: "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			driverConfigMapName:  "test-app-driver-configmap",
			wantErr:              true,
		},
		{
			name:                 "configmap with empty created application ID",
			app:                  common.BaseTestApp(),
			submissionID:         "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			createdApplicationId: "",
			driverConfigMapName:  "test-app-driver-configmap",
			wantErr:              true,
		},
	}

	// Create a fake client
	scheme := runtime.NewScheme()
	utilruntime.Must(corev1.AddToScheme(scheme))
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		Build()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Create(tt.app, tt.submissionID, tt.createdApplicationId, fakeClient, tt.driverConfigMapName, "test")
			if (err != nil) != tt.wantErr {
				t.Errorf("Create() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				configMap := &apiv1.ConfigMap{}
				configMap.Name = tt.driverConfigMapName
				configMap.Namespace = "default"
				err = fakeClient.Get(context.TODO(), ctrlClient.ObjectKeyFromObject(configMap), configMap)
				if err != nil {
					t.Errorf("failed to get ConfigMap %s: %v", tt.driverConfigMapName, err)
				}
				if configMap.Data == nil {
					t.Errorf("ConfigMap %s data is nil", tt.driverConfigMapName)
				}
			}
		})
	}
}

func TestBuildAltSubmissionCommandArgs(t *testing.T) {
	tests := []struct {
		name                 string
		app                  *v1beta2.SparkApplication
		driverPodName        string
		submissionID         string
		createdApplicationId string
		wantErr              bool
	}{
		{
			name:                 "basic submission args",
			app:                  common.BaseTestApp(),
			driverPodName:        "test-app-driver",
			submissionID:         "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			createdApplicationId: "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			wantErr:              false,
		},
		{
			name:                 "submission args with empty driver pod name",
			app:                  common.BaseTestApp(),
			driverPodName:        "",
			submissionID:         "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			createdApplicationId: "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			wantErr:              true,
		},
		{
			name:                 "submission args with empty submission ID",
			app:                  common.BaseTestApp(),
			driverPodName:        "test-app-driver",
			submissionID:         "",
			createdApplicationId: "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			wantErr:              true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := buildAltSubmissionCommandArgs(tt.app, tt.driverPodName, tt.submissionID, tt.createdApplicationId, "testservicename")
			if (err != nil) != tt.wantErr {
				t.Errorf("buildAltSubmissionCommandArgs() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCreateSparkAppConfigMapUtil(t *testing.T) {
	tests := []struct {
		name          string
		app           *v1beta2.SparkApplication
		configMapName string
		configMapData map[string]string
		wantErr       bool
	}{
		{
			name:          "basic configmap util",
			app:           common.BaseTestApp(),
			configMapName: "test-app-driver-config-map",
			configMapData: map[string]string{"app-name": "test-app"},
			wantErr:       false,
		},
		{
			name:          "configmap util with empty configmap name",
			app:           common.BaseTestApp(),
			configMapName: "",
			configMapData: map[string]string{"app-name": "test-app"},
			wantErr:       true,
		},
		{
			name:          "configmap util with nil configmap data",
			app:           common.BaseTestApp(),
			configMapName: "test-app-driver-config-map",
			configMapData: nil,
			wantErr:       true,
		},
	}

	// Create a fake client
	scheme := runtime.NewScheme()
	utilruntime.Must(corev1.AddToScheme(scheme))
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		Build()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := createConfigMapUtil(tt.configMapName, tt.app, tt.configMapData, fakeClient)
			if (err != nil) != tt.wantErr {
				t.Errorf("createConfigMapUtil() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
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
