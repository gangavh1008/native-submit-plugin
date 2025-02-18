package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/kubeflow/spark-operator/api/v1beta2"
	"io"
	apiv1 "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/util/retry"
	"log"
	"nativesubmit/config"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	ctrlClient "sigs.k8s.io/controller-runtime/pkg/client"
	"strconv"
	"strings"
	"time"
)

// CreateConfigMapUtil Helper func to create Spark Application configmap
func createConfigMapUtil(configMapName string, app *v1beta2.SparkApplication, configMapData map[string]string, kubeClient ctrlClient.Client) error {
	configMap := &apiv1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            configMapName,
			Namespace:       app.Namespace,
			OwnerReferences: []metav1.OwnerReference{*getOwnerReference(app)},
		},
		Data: configMapData,
	}

	createConfigMapErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		existingConfigMap := &apiv1.ConfigMap{}
		err := kubeClient.Get(context.TODO(), ctrlClient.ObjectKeyFromObject(configMap), existingConfigMap)
		//cm, err := kubeClient.CoreV1().ConfigMaps(app.Namespace).Get(context.TODO(), configMapName, metav1.GetOptions{})
		if apiErrors.IsNotFound(err) {
			createErr := kubeClient.Create(context.TODO(), configMap)
			//_, createErr := kubeClient.CoreV1().ConfigMaps(app.Namespace).Create(context.TODO(), configMap, metav1.CreateOptions{})
			return createErr
		}
		if err != nil {
			return err
		}
		existingConfigMap.Data = configMapData
		updateErr := kubeClient.Update(context.TODO(), existingConfigMap)
		//_, updateErr := kubeClient.CoreV1().ConfigMaps(app.Namespace).Update(context.TODO(), cm, metav1.UpdateOptions{})
		return updateErr
	})
	return createConfigMapErr
}

// Helper func to create random string of given length to create unique service name for the Spark Application Driver Pod
func randomHex(n int) (string, error) {
	bytes := make([]byte, n)
	return hex.EncodeToString(bytes), nil
}

// getServiceName Helper function to get Spark Application Driver Pod's Service Name
func getServiceName(app *v1beta2.SparkApplication) string {
	driverPodServiceName := getDriverPodName(app) + ServiceNameExtension
	if !(len(driverPodServiceName) <= KubernetesDNSLabelNameMaxLength) {
		timeInString := strconv.Itoa(int(time.Now().Unix()))
		randomHexString, _ := randomHex(10)
		randomServiceId := randomHexString + timeInString
		driverPodServiceName = SparkWithDash + randomServiceId + SparkAppDriverServiceNameExtension
	}
	return driverPodServiceName
}

type SupplementalGroup []int64

func Int32Pointer(a int32) *int32 {
	return &a
}

func Int64Pointer(a int64) *int64 {
	return &a
}

func BoolPointer(a bool) *bool {
	return &a
}

func StringPointer(a string) *string {
	return &a
}

func SupplementalGroups(a int64) SupplementalGroup {
	s := SupplementalGroup{a}
	return s
}

func AddEscapeCharacter(configMapArg string) string {
	configMapArg = strings.ReplaceAll(configMapArg, ":", "\\:")
	configMapArg = strings.ReplaceAll(configMapArg, "=", "\\=")
	return configMapArg
}

func GetAppNamespace(app *v1beta2.SparkApplication) string {
	namespace := "default"
	if app.Namespace != "" {
		namespace = app.Namespace
	} else {
		spakConfNamespace, namespaceExists := app.Spec.SparkConf[config.SparkAppNamespaceKey]
		if namespaceExists {
			namespace = spakConfNamespace
		}
	}
	return namespace
}

// Helper func to get driver pod name from Spark Application CRD instance
func getDriverPodName(app *v1beta2.SparkApplication) string {
	name := app.Spec.Driver.PodName
	if name != nil && len(*name) > 0 {
		return *name
	}
	sparkConf := app.Spec.SparkConf
	if sparkConf[config.SparkDriverPodNameKey] != "" {
		return sparkConf[config.SparkDriverPodNameKey]
	}
	return fmt.Sprintf("%s-driver", app.Name)
}

// Helper func to get Owner references to be added to Spark Application resources - pod, service, configmap
func getOwnerReference(app *v1beta2.SparkApplication) *metav1.OwnerReference {
	controller := false
	return &metav1.OwnerReference{
		APIVersion: v1beta2.SchemeGroupVersion.String(),
		Kind:       reflect.TypeOf(v1beta2.SparkApplication{}).Name(),
		Name:       app.Name,
		UID:        app.UID,
		Controller: &controller,
	}
}

func getMasterURL() (string, error) {
	kubernetesServiceHost := os.Getenv(kubernetesServiceHostEnvVar)
	if kubernetesServiceHost == "" {
		kubernetesServiceHost = "localhost"
	}
	kubernetesServicePort := os.Getenv(kubernetesServicePortEnvVar)
	if kubernetesServicePort == "" {
		kubernetesServicePort = "443"
	}

	return fmt.Sprintf("k8s://https://%s:%s", kubernetesServiceHost, kubernetesServicePort), nil
}

const (
	kubernetesServiceHostEnvVar = "KUBERNETES_SERVICE_HOST"
	kubernetesServicePortEnvVar = "KUBERNETES_SERVICE_PORT"
)

// addLocalDirConfOptions excludes local dir volumes, update SparkApplication and returns local dir config options
func addLocalDirConfOptions(app *v1beta2.SparkApplication) ([]string, error) {
	var localDirConfOptions []string

	sparkLocalVolumes := map[string]apiv1.Volume{}
	var mutateVolumes []apiv1.Volume

	// Filter local dir volumes
	for _, volume := range app.Spec.Volumes {
		if strings.HasPrefix(volume.Name, config.SparkLocalDirVolumePrefix) {
			sparkLocalVolumes[volume.Name] = volume
		} else {
			mutateVolumes = append(mutateVolumes, volume)
		}
	}
	app.Spec.Volumes = mutateVolumes

	// Filter local dir volumeMounts and set mutate volume mounts to driver and executor
	if app.Spec.Driver.VolumeMounts != nil {
		driverMutateVolumeMounts, driverLocalDirConfConfOptions := filterMutateMountVolumes(app.Spec.Driver.VolumeMounts, config.SparkDriverVolumesPrefix, sparkLocalVolumes)
		app.Spec.Driver.VolumeMounts = driverMutateVolumeMounts
		localDirConfOptions = append(localDirConfOptions, driverLocalDirConfConfOptions...)
	}

	if app.Spec.Executor.VolumeMounts != nil {
		executorMutateVolumeMounts, executorLocalDirConfConfOptions := filterMutateMountVolumes(app.Spec.Executor.VolumeMounts, config.SparkExecutorVolumesPrefix, sparkLocalVolumes)
		app.Spec.Executor.VolumeMounts = executorMutateVolumeMounts
		localDirConfOptions = append(localDirConfOptions, executorLocalDirConfConfOptions...)
	}

	return localDirConfOptions, nil
}

func filterMutateMountVolumes(volumeMounts []apiv1.VolumeMount, prefix string, sparkLocalVolumes map[string]apiv1.Volume) ([]apiv1.VolumeMount, []string) {
	var mutateMountVolumes []apiv1.VolumeMount
	var localDirConfOptions []string
	for _, volumeMount := range volumeMounts {
		if volume, ok := sparkLocalVolumes[volumeMount.Name]; ok {
			options := buildLocalVolumeOptions(prefix, volume, volumeMount)
			for _, option := range options {
				localDirConfOptions = append(localDirConfOptions, option)
			}
		} else {
			mutateMountVolumes = append(mutateMountVolumes, volumeMount)
		}
	}

	return mutateMountVolumes, localDirConfOptions
}

func buildLocalVolumeOptions(prefix string, volume apiv1.Volume, volumeMount apiv1.VolumeMount) []string {
	VolumeMountPathTemplate := prefix + "%s.%s.mount.path=%s"
	VolumeMountOptionTemplate := prefix + "%s.%s.options.%s=%s"

	var options []string
	switch {
	case volume.HostPath != nil:
		options = append(options, fmt.Sprintf(VolumeMountPathTemplate, "hostPath", volume.Name, volumeMount.MountPath))
		options = append(options, fmt.Sprintf(VolumeMountOptionTemplate, "hostPath", volume.Name, "path", volume.HostPath.Path))
		if volume.HostPath.Type != nil {
			options = append(options, fmt.Sprintf(VolumeMountOptionTemplate, "hostPath", volume.Name, "type", *volume.HostPath.Type))
		}
	case volume.EmptyDir != nil:
		options = append(options, fmt.Sprintf(VolumeMountPathTemplate, "emptyDir", volume.Name, volumeMount.MountPath))
	case volume.PersistentVolumeClaim != nil:
		options = append(options, fmt.Sprintf(VolumeMountPathTemplate, "persistentVolumeClaim", volume.Name, volumeMount.MountPath))
		options = append(options, fmt.Sprintf(VolumeMountOptionTemplate, "persistentVolumeClaim", volume.Name, "claimName", volume.PersistentVolumeClaim.ClaimName))
	}

	return options
}

func getResourceLabels(app *v1beta2.SparkApplication) map[string]string {
	labels := map[string]string{config.SparkAppNameLabel: app.Name}
	if app.Status.SubmissionID != "" {
		labels[config.SubmissionIDLabel] = app.Status.SubmissionID
	}
	return labels
}

func getDriverPort(sparkConfKeyValuePairs map[string]string) int {
	//Checking if port information is passed in the spec, and using same
	// or using the default ones
	driverPortToBeUsed := DefaultDriverPort
	driverPort, valueExists := sparkConfKeyValuePairs[SparkDriverPort]
	if valueExists {
		driverPortSupplied, err := strconv.Atoi(driverPort)
		if err != nil {
			panic("Driver Port not parseable - hence failing the spark submit" + fmt.Sprint(err))
		} else {
			driverPortToBeUsed = driverPortSupplied
		}
	}
	return driverPortToBeUsed
}

func getBlockManagerPort(sparkConfKeyValuePairs map[string]string) int {
	//BlockManager Port
	blockManagerPortToBeUsed := DefaultBlockManagerPort
	sparkBlockManagerPortSupplied, sparkBlockManagerPortSuppliedValueExists := sparkConfKeyValuePairs[SparkBlockManagerPort]
	sparkDriverBlockManagerPortSupplied, sparkDriverBlockManagerPortSuppliedValueExists := sparkConfKeyValuePairs[SparkDriverBlockManagerPort]
	if sparkDriverBlockManagerPortSuppliedValueExists {
		blockManagerPortFromConfig, err := strconv.Atoi(sparkDriverBlockManagerPortSupplied)
		if err != nil {
			panic("Block Manager Port not parseable to integer value - hence failing the spark submit" + fmt.Sprint(err))
		} else {
			blockManagerPortToBeUsed = blockManagerPortFromConfig
		}
	} else if sparkBlockManagerPortSuppliedValueExists {
		blockManagerPortFromConfig, err := strconv.Atoi(sparkBlockManagerPortSupplied)
		if err != nil {
			panic("Driver Block Manager Port not parseable to integer value - hence failing the spark submit" + fmt.Sprint(err))
		} else {
			blockManagerPortToBeUsed = blockManagerPortFromConfig
		}
	}
	return blockManagerPortToBeUsed
}

func checkSparkConf(sparkConf map[string]string, configKey string) bool {
	valueExists := false
	_, valueExists = sparkConf[configKey]
	return valueExists
}

func addSecret(secret v1beta2.SecretInfo, volumeExtension string, driverPodVolumes []apiv1.Volume, driverPodContainerSpec apiv1.Container) ([]apiv1.Volume, apiv1.Container) {
	secretVolume := apiv1.Volume{
		Name: fmt.Sprintf("%s%s", secret.Name, volumeExtension),
		VolumeSource: apiv1.VolumeSource{
			Secret: &apiv1.SecretVolumeSource{
				SecretName: secret.Name,
			},
		},
	}
	driverPodVolumes = append(driverPodVolumes, secretVolume)

	//Volume mount for the local temp folder
	volumeMount := apiv1.VolumeMount{
		Name:      fmt.Sprintf("%s%s", secret.Name, volumeExtension),
		MountPath: secret.Path,
	}
	driverPodContainerSpec.VolumeMounts = append(driverPodContainerSpec.VolumeMounts, volumeMount)
	return driverPodVolumes, driverPodContainerSpec
}

func loadPodFromTemplate(templateFileName string, containerName string, conf map[string]string) (apiv1.Pod, error) {
	var file apiv1.Pod
	localFile, err := downloadFile(templateFileName, createTempDir(), conf)
	if err != nil {
		fmt.Errorf("Encountered exception while attempting to download the pod template file : %v", err)
		return file, err
	} else {
		data, _ := os.ReadFile(localFile)
		manifests := string(data)
		decode := scheme.Codecs.UniversalDeserializer().Decode
		obj, _, _ := decode([]byte(manifests), nil, nil)
		pod := obj.(*apiv1.Pod)
		newPod := selectSparkContainer(*pod, containerName)
		return newPod, nil
	}
}

func downloadFile(path string, targetDir string, sparkConf map[string]string) (string, error) {
	if path == "" {
		return "", fmt.Errorf("Pod template file's download path is empty")
	}
	uri, err := url.Parse(path)
	if err != nil {
		return "", err
	}

	switch uri.Scheme {
	case "file", "local":
		return path, nil
	case "http", "https", "ftp":

		fname := filepath.Base(uri.Path)
		localFile, _ := doFetchFile(uri.String(), targetDir, fname, sparkConf)
		return localFile, nil
	default:
		fname := filepath.Base(uri.Path)
		localFile, err := doFetchFile(uri.String(), targetDir, fname, sparkConf)
		if err != nil {
			return "", err
		}
		return localFile, nil
	}

	return "", err
}

func selectSparkContainer(pod apiv1.Pod, containerName string) apiv1.Pod {
	selectNamedContainer := func(containers []apiv1.Container, name string) (*apiv1.Container, []apiv1.Container, bool) {
		var rest []apiv1.Container
		for _, container := range containers {
			if container.Name == name {
				return &container, rest, true
			}
			rest = append(rest, container)
		}
		log.Printf("specified container %s not found on pod template, falling back to taking the first container", name)
		return nil, nil, false
	}

	containers := pod.Spec.Containers
	if containerName != "" {
		if selectedContainer, _, found := selectNamedContainer(containers, containerName); found {
			return apiv1.Pod{
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{*selectedContainer},
				},
			}
		}
	}

	if len(containers) > 0 {
		var containerList []apiv1.Container
		containerList = append(containerList, containers[0])
		for _, container := range containers[1:] {
			containerList = append(containerList, container)
		}
		return apiv1.Pod{
			Spec: apiv1.PodSpec{
				Containers: containerList,
			},
		}

	}
	return apiv1.Pod{}
}

func createTempDir() string {
	dir, err := os.MkdirTemp("", "spark")
	if err != nil {
		log.Fatal(err)
	}
	return dir
}

func doFetchFile(urlStr string, targetDir string, filename string, conf map[string]string) (string, error) {
	targetFile := filepath.Join(targetDir, filename)
	parsedURL, err := url.Parse(urlStr)
	if err != nil {
		return "", err
	}

	switch scheme := parsedURL.Scheme; scheme {
	case "spark":
		return "", err
	case "http", "https", "ftp":
		fetchTimeoutDuration, _ := time.ParseDuration("5s")
		fetchTimeout, fetchTimeoutExists := conf["spark.files.fetchTimeout"]
		if fetchTimeoutExists {
			fetchTimeoutDuration, _ = time.ParseDuration(fetchTimeout)
		}
		client := http.Client{
			Timeout: fetchTimeoutDuration,
		}
		resp, err := client.Get(urlStr)
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()
		return downloadFileFromURL(resp.Body, targetFile)
	case "file":
		sourceFile := parsedURL.Path
		return copyFile(sourceFile, targetFile, false)
	default:
		return "", err
	}
}

func downloadFileFromURL(reader io.Reader, destFile string) (string, error) {
	tempFile, err := os.CreateTemp(filepath.Dir(destFile), "fetchFileTemp")
	if err != nil {
		return "", err
	}
	defer tempFile.Close()
	defer os.Remove(tempFile.Name())

	if _, err := io.Copy(tempFile, reader); err != nil {
		return "", err
	}

	return copyFile(tempFile.Name(), destFile, true)
}

func copyFile(sourceFile string, destFile string, removeSourceFile bool) (string, error) {
	if err := os.Rename(sourceFile, destFile); err != nil {
		return "", err
	}
	if removeSourceFile {
		if err := os.Remove(sourceFile); err != nil {
			return "", err
		}
	}
	return destFile, nil
}

func getMemoryOverheadFactor(app *v1beta2.SparkApplication) string {
	var memoryOverheadFactor string
	if app.Spec.Type == v1beta2.SparkApplicationTypeJava || app.Spec.Type == v1beta2.SparkApplicationTypeScala {
		memoryOverheadFactor = JavaScalaMemoryOverheadFactor
	} else {

		memoryOverheadFactor = OtherLanguageMemoryOverheadFactor
	}
	return memoryOverheadFactor
}
