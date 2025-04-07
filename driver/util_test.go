package driver

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestDoFetchFile(t *testing.T) {
	tests := []struct {
		name     string
		filePath string
		wantErr  bool
	}{
		{
			name:     "file protocol",
			filePath: "file//a.txt",
			wantErr:  false,
		},
		{
			name:     "ftp protocol",
			filePath: "ftp://localhost/a.txt",
			wantErr:  false,
		},
		{
			name:     "http protocol",
			filePath: "http://test.com/a.txt",
			wantErr:  false,
		},
		{
			name:     "https protocol",
			filePath: "https://test.com/a.txt",
			wantErr:  false,
		},
		{
			name:     "spark protocol",
			filePath: "spark://a.txt",
			wantErr:  false,
		},
		{
			name:     "local protocol",
			filePath: "local//a.txt",
			wantErr:  false,
		},
	}

	sparkConf := make(map[string]string)
	fileName := "a.txt"

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test doFetchFile
			_, err := doFetchFile(tt.filePath, createTempDir(), fileName, sparkConf)
			if (err != nil) != tt.wantErr {
				t.Errorf("doFetchFile() error = %v, wantErr %v", err, tt.wantErr)
			}

			// Test downloadFile
			_, err = downloadFile(tt.filePath, createTempDir(), sparkConf)
			if (err != nil) != tt.wantErr {
				t.Errorf("downloadFile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSelectSparkContainer(t *testing.T) {
	tests := []struct {
		name          string
		pod           corev1.Pod
		containerName string
		expectedName  string
	}{
		{
			name: "select spark driver container",
			pod: corev1.Pod{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "dummy-container-2"},
						{Name: "spark-kubernetes-driver"},
						{Name: "dummy-container"},
					},
				},
			},
			containerName: "spark-kubernetes-driver",
			expectedName:  "spark-kubernetes-driver",
		},
		{
			name: "container not found",
			pod: corev1.Pod{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "dummy-container-2"},
						{Name: "dummy-container"},
					},
				},
			},
			containerName: "spark-kubernetes-driver",
			expectedName:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			podWithSelectedContainer := selectSparkContainer(tt.pod, tt.containerName)
			if tt.expectedName == "" {
				if len(podWithSelectedContainer.Spec.Containers) != 0 {
					t.Errorf("selectSparkContainer() should return empty containers when container not found")
				}
				return
			}
			if podWithSelectedContainer.Spec.Containers[0].Name != tt.expectedName {
				t.Errorf("selectSparkContainer() = %v, want %v", podWithSelectedContainer.Spec.Containers[0].Name, tt.expectedName)
			}
		})
	}
}
