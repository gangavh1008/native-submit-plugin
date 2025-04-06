package driver

import (
	"nativesubmit/common"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestHandleLocalDirsFeatureStep(t *testing.T) {

	testFn := func(test common.Testcase, t *testing.T) {
		//Pod Volumes setup
		var driverPodVolumes []corev1.Volume

		driverPodContainerSpec, resolvedLocalDirs := CreateDriverPodContainerSpec(test.App)
		//(app *v1beta2.SparkApplication, driverPodVolumes *[]apiv1.Volume, volumeMounts *[]apiv1.VolumeMount, envVariables *[]apiv1.EnvVar)
		localDirFeatureSetupError := handleLocalDirsFeatureStep(test.App, resolvedLocalDirs, &driverPodVolumes, &driverPodContainerSpec.VolumeMounts, &driverPodContainerSpec.Env, test.App.Spec.Driver.VolumeMounts, test.App.Spec.Volumes)
		if localDirFeatureSetupError != nil {
			t.Errorf("failed to setup local directory for the driver pod: %v", localDirFeatureSetupError)
		}

	}
	testcases := common.TestCasesList
	for index, test := range testcases {
		indexedProcessing(index, test)
		testFn(test, t)
	}
}

func indexedProcessing(index int, test common.Testcase) {
	if index == 1 {
		test.App.Spec.SparkConf["spark.kubernetes.local.dirs.tmpfs"] = "false"
		test.App.Spec.SparkConf["spark.kubernetes.driver.SPARK_LOCAL_DIRS"] = "/tmp/spark-local-dir-2,/tmp/spark-local-dir-1,/tmp/spark-local-dir-100"
	}
	if index == 0 {
		test.App.Spec.SparkConf["spark.kubernetes.local.dirs.tmpfs"] = "true"
	}
	if index == 2 {
		test.App.Spec.Volumes = nil
		test.App.Spec.Driver.VolumeMounts = nil
		test.App.Spec.SparkConf["spark.kubernetes.local.dirs.tmpfs"] = "true"
	}
}
