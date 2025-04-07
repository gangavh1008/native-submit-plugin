package configmap

import (
	"context"
	"fmt"
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
		validateConfig       func(*apiv1.ConfigMap) error
	}{
		{
			name:                 "basic configmap creation",
			app:                  common.BaseTestApp(),
			submissionID:         "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			createdApplicationId: "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			driverConfigMapName:  "test-app-driver-configmap",
			wantErr:              false,
			validateConfig: func(cm *apiv1.ConfigMap) error {
				if cm.Data == nil {
					return fmt.Errorf("configmap data is nil")
				}
				if cm.Data["spark.app.name"] != "test-app" {
					return fmt.Errorf("unexpected spark.app.name value: %s", cm.Data["spark.app.name"])
				}
				if cm.Data["spark.app.id"] != "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO" {
					return fmt.Errorf("unexpected spark.app.id value: %s", cm.Data["spark.app.id"])
				}
				return nil
			},
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
		{
			name: "configmap with custom spark config",
			app: func() *v1beta2.SparkApplication {
				app := common.BaseTestApp()
				app.Spec.SparkConf["spark.executor.memory"] = "2g"
				app.Spec.SparkConf["spark.executor.cores"] = "2"
				return app
			}(),
			submissionID:         "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			createdApplicationId: "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			driverConfigMapName:  "test-app-driver-configmap",
			wantErr:              false,
			validateConfig: func(cm *apiv1.ConfigMap) error {
				if cm.Data == nil {
					return fmt.Errorf("configmap data is nil")
				}
				if cm.Data["spark.executor.memory"] != "2g" {
					return fmt.Errorf("unexpected spark.executor.memory value: %s", cm.Data["spark.executor.memory"])
				}
				if cm.Data["spark.executor.cores"] != "2" {
					return fmt.Errorf("unexpected spark.executor.cores value: %s", cm.Data["spark.executor.cores"])
				}
				return nil
			},
		},
		{
			name: "configmap with custom driver config",
			app: func() *v1beta2.SparkApplication {
				app := common.BaseTestApp()
				memoryStr := memoryQuantity.String()
				app.Spec.Driver.Memory = &memoryStr
				cores := int32(2)
				app.Spec.Driver.Cores = &cores
				return app
			}(),
			submissionID:         "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			createdApplicationId: "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			driverConfigMapName:  "test-app-driver-configmap",
			wantErr:              false,
			validateConfig: func(cm *apiv1.ConfigMap) error {
				if cm.Data == nil {
					return fmt.Errorf("configmap data is nil")
				}
				if cm.Data["spark.driver.memory"] != memoryQuantity.String() {
					return fmt.Errorf("unexpected spark.driver.memory value: %s", cm.Data["spark.driver.memory"])
				}
				if cm.Data["spark.driver.cores"] != "2" {
					return fmt.Errorf("unexpected spark.driver.cores value: %s", cm.Data["spark.driver.cores"])
				}
				return nil
			},
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
			// Clean up any existing ConfigMap before the test
			configMap := &apiv1.ConfigMap{}
			configMap.Name = tt.driverConfigMapName
			configMap.Namespace = "default"
			_ = fakeClient.Delete(context.TODO(), configMap)

			err := Create(tt.app, tt.submissionID, tt.createdApplicationId, fakeClient, tt.driverConfigMapName, "test")
			if (err != nil) != tt.wantErr {
				t.Errorf("Create() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && tt.validateConfig != nil {
				configMap := &apiv1.ConfigMap{}
				configMap.Name = tt.driverConfigMapName
				configMap.Namespace = "default"
				err = fakeClient.Get(context.TODO(), ctrlClient.ObjectKeyFromObject(configMap), configMap)
				if err != nil {
					t.Errorf("failed to get ConfigMap %s: %v", tt.driverConfigMapName, err)
					return
				}
				if err := tt.validateConfig(configMap); err != nil {
					t.Errorf("configmap validation failed: %v", err)
				}
			}
		})
	}
}
