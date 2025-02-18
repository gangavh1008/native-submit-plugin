package test

import (
	corev1 "k8s.io/api/core/v1"
	"nativesubmit"
	"testing"
)

func TestHandleLocalDirsFeatureStep(t *testing.T) {

	testFn := func(test nativesubmit.testcase, t *testing.T) {
		//Pod Volumes setup
		var driverPodVolumes []corev1.Volume

		driverPodContainerSpec, resolvedLocalDirs := nativesubmit.CreateDriverPodContainerSpec(test.app)
		//(app *v1beta2.SparkApplication, driverPodVolumes *[]apiv1.Volume, volumeMounts *[]apiv1.VolumeMount, envVariables *[]apiv1.EnvVar)
		localDirFeatureSetupError := nativesubmit.handleLocalDirsFeatureStep(test.app, resolvedLocalDirs, &driverPodVolumes, &driverPodContainerSpec.VolumeMounts, &driverPodContainerSpec.Env, test.app.Spec.Driver.VolumeMounts, test.app.Spec.Volumes)
		if localDirFeatureSetupError != nil {
			t.Errorf("failed to setup local directory for the driver pod: %v", localDirFeatureSetupError)
		}

	}
	testcases := nativesubmit.TestCasesList
	for index, test := range testcases {
		nativesubmit.indexedProcessing(index, test)
		testFn(test, t)
	}
}
