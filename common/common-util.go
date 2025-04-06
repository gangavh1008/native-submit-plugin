package common

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func Int32Pointer(a int32) *int32 {
	return &a
}
func LabelsForSpark() map[string]string {
	return map[string]string{"version": "3.0.1"}
}
func BoolPointer(a bool) *bool {
	return &a
}

const (
	DefaultUiPort                     = 4040
	DefaultDriverPort                 = 7078
	DefaultBlockManagerPort           = 7079
	SparkDriverPort                   = "spark.driver.port"
	JavaScalaMemoryOverheadFactor     = "0.10"
	OtherLanguageMemoryOverheadFactor = "0.40"
	// SparkAppNamespaceKey is the configuration property for application namespace.
	SparkAppNamespaceKey = "spark.kubernetes.namespace"
	// SparkDriverPodNameKey is the Spark configuration key for driver pod name.
	SparkDriverPodNameKey = "spark.kubernetes.driver.pod.name"
	// SparkImagePullSecretKey is the configuration property for specifying the comma-separated list of image-pull
	// secrets.
	SparkImagePullSecretKey = "spark.kubernetes.container.image.pullSecrets"
	// SparkDriverSecretKeyPrefix is the configuration property prefix for specifying secrets to be mounted into the
	// driver.
	SparkDriverSecretKeyPrefix = "spark.kubernetes.driver.secrets."
	// SparkConfDirEnvVar is the environment variable to add to the driver and executor Pods that point
	// to the directory where the Spark ConfigMap is mounted.
	SparkConfDirEnvVar = "SPARK_CONF_DIR"
	// SparkDriverContainerName is name of driver container in spark driver pod
	SparkDriverContainerName = "spark-kubernetes-driver"
	// SparkDriverSecretKeyRefKeyPrefix is the configuration property prefix for specifying environment variables
	// from SecretKeyRefs for the driver.
	SparkDriverSecretKeyRefKeyPrefix = "spark.kubernetes.driver.secretKeyRef."
	// SparkDriverCoreLimitKey is the configuration property for specifying the hard CPU limit for the driver pod.
	SparkDriverCoreLimitKey = "spark.kubernetes.driver.limit.cores"
	// SparkDriverCoreRequestKey is the configuration property for specifying the physical CPU request for the driver.
	SparkDriverCoreRequestKey = "spark.kubernetes.driver.request.cores"
)

func StringPointer(a string) *string {
	return &a
}

func GetAppNamespace(app *v1beta2.SparkApplication) string {
	namespace := "default"
	if app.Namespace != "" {
		namespace = app.Namespace
	} else {
		spakConfNamespace, namespaceExists := app.Spec.SparkConf[SparkAppNamespaceKey]
		if namespaceExists {
			namespace = spakConfNamespace
		}
	}
	return namespace
}

// Helper func to get driver pod name from Spark Application CRD instance
func GetDriverPodName(app *v1beta2.SparkApplication) string {
	name := app.Spec.Driver.PodName
	if name != nil && len(*name) > 0 {
		return *name
	}
	sparkConf := app.Spec.SparkConf
	if sparkConf[SparkDriverPodNameKey] != "" {
		return sparkConf[SparkDriverPodNameKey]
	}
	return fmt.Sprintf("%s-driver", app.Name)
}
func GetDriverPort(sparkConfKeyValuePairs map[string]string) int {
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

// Helper func to get Owner references to be added to Spark Application resources - pod, service, configmap
func GetOwnerReference(app *v1beta2.SparkApplication) *metav1.OwnerReference {
	controller := false
	return &metav1.OwnerReference{
		APIVersion: v1beta2.SchemeGroupVersion.String(),
		Kind:       reflect.TypeOf(v1beta2.SparkApplication{}).Name(),
		Name:       app.Name,
		UID:        app.UID,
		Controller: &controller,
	}
}

func GetMemoryOverheadFactor(app *v1beta2.SparkApplication) string {
	var memoryOverheadFactor string
	if app.Spec.Type == v1beta2.SparkApplicationTypeJava || app.Spec.Type == v1beta2.SparkApplicationTypeScala {
		memoryOverheadFactor = JavaScalaMemoryOverheadFactor
	} else {

		memoryOverheadFactor = OtherLanguageMemoryOverheadFactor
	}
	return memoryOverheadFactor
}
func CheckSparkConf(sparkConf map[string]string, configKey string) bool {
	valueExists := false
	_, valueExists = sparkConf[configKey]
	return valueExists
}
func Int64Pointer(a int64) *int64 {
	return &a
}
