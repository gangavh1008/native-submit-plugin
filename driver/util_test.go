package driver

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

// MockTransport implements http.RoundTripper
type MockTransport struct {
	Response *http.Response
	Err      error
}

func (m *MockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if m.Err != nil {
		return nil, m.Err
	}
	if m.Response != nil {
		return m.Response, nil
	}
	// Return a default response for HTTP/HTTPS requests
	if req.URL.Scheme == "http" || req.URL.Scheme == "https" {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewBufferString("test content")),
			Header:     make(http.Header),
		}, nil
	}
	// Return an error for unsupported schemes
	return nil, &url.Error{
		Op:  "Get",
		URL: req.URL.String(),
		Err: fmt.Errorf("unsupported protocol scheme %q", req.URL.Scheme),
	}
}

func TestDoFetchFile(t *testing.T) {
	// Create a temporary directory for file operations
	tempDir, err := os.MkdirTemp("", "test-fetch")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a test file
	testFile := filepath.Join(tempDir, "test.txt")
	if err := os.WriteFile(testFile, []byte("test content"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	tests := []struct {
		name     string
		filePath string
		wantErr  bool
	}{
		{
			name:     "file protocol",
			filePath: "file://" + testFile,
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
			name:     "ftp protocol",
			filePath: "ftp://test.com/a.txt",
			wantErr:  true,
		},
		{
			name:     "spark protocol",
			filePath: "spark://test.com/a.txt",
			wantErr:  true,
		},
		{
			name:     "local protocol",
			filePath: "local://" + testFile,
			wantErr:  false,
		},
	}

	sparkConf := map[string]string{
		"spark.files.fetchTimeout": "100ms", // Use a shorter timeout for tests
	}
	fileName := "a.txt"

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock HTTP client with a custom transport for each test case
			mockClient := &http.Client{
				Transport: &MockTransport{},
			}

			// Replace the default HTTP client with our mock
			originalClient := http.DefaultClient
			http.DefaultClient = mockClient
			defer func() { http.DefaultClient = originalClient }()

			// Create a new test file for each test case
			testFile := filepath.Join(tempDir, "test.txt")
			if err := os.WriteFile(testFile, []byte("test content"), 0644); err != nil {
				t.Fatalf("Failed to create test file: %v", err)
			}

			// Test doFetchFile
			_, err := doFetchFile(tt.filePath, tempDir, fileName, sparkConf)
			if (err != nil) != tt.wantErr {
				t.Errorf("doFetchFile() error = %v, wantErr %v", err, tt.wantErr)
			}

			// Test downloadFile
			_, err = downloadFile(tt.filePath, tempDir, sparkConf)
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
