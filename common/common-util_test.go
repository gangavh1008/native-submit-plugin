package common

import (
	"testing"
)

func TestGetDriverPodName(t *testing.T) {
	testcases := []Testcase{
		{
			App: TestApp,
		},
		{
			App: TestApp,
		},
	}
	testFn := func(test Testcase, t *testing.T) {
		podName := GetDriverPodName(test.App)
		if podName == "" {
			t.Fatalf(`Unit test for getDriverPodName() failed`)
		}
	}
	for index, test := range testcases {
		if index == 0 {
			test.App.Spec.Driver.PodName = StringPointer("")
			test.App.Spec.SparkConf["spark.kubernetes.driver.pod.name"] = "test-app-driver"
		}
		testFn(test, t)
	}

}
