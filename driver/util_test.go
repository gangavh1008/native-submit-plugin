package driver

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestDoFetchFile(t *testing.T) {
	filePath := "file//a.txt"
	ftpPath := "ftp://localhost/a.txt"
	httpFile := "http://test.com/a.txt"
	httpsFile := "https://test.com/a.txt"
	sparkFile := "spark://a.txt"
	localFilePath := "local//a.txt"
	fileName := "a.txt"

	sparkConf := make(map[string]string)
	_, err := doFetchFile(filePath, createTempDir(), fileName, sparkConf)
	if err != nil {
		t.Log("Error occcurred while testing doFetchFile function")
	}
	_, err = downloadFile(filePath, createTempDir(), sparkConf)
	if err != nil {
		t.Log("Error occcurred while testing downloadFile function")
	}
	_, err = doFetchFile(ftpPath, createTempDir(), fileName, sparkConf)
	if err != nil {
		t.Log("Error occcurred while testing doFetchFile function")
	}
	_, err = downloadFile(ftpPath, createTempDir(), sparkConf)
	if err != nil {
		t.Log("Error occcurred while testing downloadFile function")
	}
	_, err = doFetchFile(httpFile, createTempDir(), fileName, sparkConf)
	if err != nil {
		t.Log("Error occcurred while testing doFetchFile function")
	}
	_, err = downloadFile(httpFile, createTempDir(), sparkConf)
	if err != nil {
		t.Log("Error occcurred while testing downloadFile function")
	}
	_, err = doFetchFile(httpsFile, createTempDir(), fileName, sparkConf)
	if err != nil {
		t.Log("Error occcurred while testing doFetchFile function")
	}
	_, err = downloadFile(httpsFile, createTempDir(), sparkConf)
	if err != nil {
		t.Log("Error occcurred while testing downloadFile function")
	}
	_, err = doFetchFile(sparkFile, createTempDir(), fileName, sparkConf)
	if err != nil {
		t.Log("Error occcurred while testing doFetchFile function")
	}
	_, err = downloadFile(sparkFile, createTempDir(), sparkConf)
	if err != nil {
		t.Log("Error occcurred while testing downloadFile function")
	}
	_, err = downloadFile(localFilePath, createTempDir(), sparkConf)
	if err != nil {
		t.Log("Error occcurred while testing downloadFile function")
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
	podWithSelectedContainer := selectSparkContainer(pod, "spark-kubernetes-driver")
	if podWithSelectedContainer.Spec.Containers[0].Name != "spark-kubernetes-driver" {
		t.Fatalf(`Unit test for selectSparkContainer() function failed`)
	}

}
