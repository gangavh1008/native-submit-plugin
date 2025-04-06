package main

import (
	"encoding/hex"
	"fmt"
	"nativesubmit/common"
	"nativesubmit/configmap"
	"nativesubmit/driver"
	"nativesubmit/service"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/kubeflow/spark-operator/api/v1beta2"
	ctrlClient "sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	True                               = "true"
	Spark                              = "spark"
	SparkWithDash                      = "spark-"
	SparkAppDriverServiceNameExtension = "-driver-svc"
	KubernetesDNSLabelNameMaxLength    = 63
	ServiceNameExtension               = "-svc"
	DotSeparator                       = "."
	Version                            = "version"
	SparkAppName                       = "spark-app-name"
	ConfigMapExtension                 = "-conf-map"
	SparkApplicationSelectorLabel      = "spark-app-selector"
	// LabelAnnotationPrefix is the prefix of every labels and annotations added by the controller.
	LabelAnnotationPrefix = "sparkoperator.k8s.io/"
	// SparkAppNameLabel is the name of the label for the SparkApplication object name.
	SparkAppNameLabel = LabelAnnotationPrefix + "app-name"
	SparkRoleLabel    = "spark-role"
	// SparkDriverRole is the value of the spark-role label for the driver.
	SparkDriverRole                = "driver"
	SparkAppSubmissionIDAnnotation = "sparkoperator.k8s.io/submission-id"
	SparkAppLauncherSOAnnotation   = "sparkoperator.k8s.io/launched-by-spark-operator"
)

// |      +-+--+----+    |    +-----v--+-+
// |      |         |    |    |          |
// |      |         |    |    |          |
// |      |   New   +---------> Submitted|
// |      |         |    |    |          |
// |      |         |    |    |          |
// |      |         |    |    |          |
// |      +---------+    |    +----^-----|
// Logic involved in moving "New" Spark Application to "Submitted" state is implemented in Golang with this function RunAltSparkSubmit as starting step
// 3 Resources are created in this logic per new Spark Application, in the order listed: ConfigMap for the Spark Application, Driver Pod, Driver Service

func runAltSparkSubmitWrapper(app *v1beta2.SparkApplication, cl ctrlClient.Client) error {
	_, err := runAltSparkSubmit(app, app.Status.SubmissionID, cl)
	return err
}

func runAltSparkSubmit(app *v1beta2.SparkApplication, submissionID string, kubeClient ctrlClient.Client) (bool, error) {

	appSpecVolumeMounts := app.Spec.Driver.VolumeMounts
	appSpecVolumes := app.Spec.Volumes

	// Create Application ID with the convention followed in Scala/Java
	uuidString := strings.ReplaceAll(uuid.New().String(), "-", "")
	createdApplicationId := fmt.Sprintf("%s-%s", Spark, uuidString)

	//Updte Application CRD Instnce with Spark Application ID
	app.Status.SparkApplicationID = createdApplicationId

	//Create Spark Application ConfigMap Name with the convention followed in Scala/Java
	driverConfigMapName := common.GetDriverPodName(app) + ConfigMapExtension

	serviceName := getServiceName(app)

	//Updte Application CRD Instnce with Submission ID
	app.Status.SubmissionID = submissionID

	//Create Service Labels by aggregating Spark Application Specification level, driver specification level and dynamic lables
	serviceLabels := map[string]string{
		SparkAppNameLabel:              app.Name,
		SparkAppName:                   app.Name,
		SparkApplicationSelectorLabel:  createdApplicationId,
		SparkRoleLabel:                 SparkDriverRole,
		SparkAppSubmissionIDAnnotation: submissionID,
		SparkAppLauncherSOAnnotation:   True,
	}

	if app.Spec.Driver.Labels != nil {
		_, versionLabelExists := app.Spec.Driver.Labels[Version]
		if versionLabelExists {
			serviceLabels[Version] = app.Spec.Driver.Labels[Version]
		}
		for key, val := range app.Spec.Driver.Labels {
			serviceLabels[key] = val
		}
	}

	if app.Labels != nil {
		for key, val := range app.Labels {
			serviceLabels[key] = val
		}
	}
	// labels passed in sparkConf
	sparkConfKeyValuePairs := app.Spec.SparkConf
	for sparkConfKey, sparkConfValue := range sparkConfKeyValuePairs {
		if strings.Contains(sparkConfKey, "spark.kubernetes.driver.label.") {
			lastDotIndex := strings.LastIndex(sparkConfKey, DotSeparator)
			labelKey := sparkConfKey[lastDotIndex+1:]
			labelValue := sparkConfValue
			serviceLabels[labelKey] = labelValue
		}
	}

	//Spark Application ConfigMap Creation
	createErr := configmap.Create(app, submissionID, createdApplicationId, kubeClient, driverConfigMapName, serviceName)
	if createErr != nil {
		return false, fmt.Errorf("error while creating config map: %w", createErr)
	}

	//Spark Application Driver Pod Creation
	createPodErr := driver.Create(app, serviceLabels, driverConfigMapName, kubeClient, appSpecVolumeMounts, appSpecVolumes)
	if createPodErr != nil {
		return false, fmt.Errorf("error while creating driver pod: %w", createPodErr)
	}
	//Spark Application Driver Pod's Service Creation
	createServiceErr := service.Create(app, serviceLabels, kubeClient, createdApplicationId, serviceName)
	if createServiceErr != nil {
		return false, fmt.Errorf("error while creating driver service: %w", createServiceErr)
	}
	return true, nil
}

// getServiceName Helper function to get Spark Application Driver Pod's Service Name
func getServiceName(app *v1beta2.SparkApplication) string {
	driverPodServiceName := fmt.Sprintf("%s%s", common.GetDriverPodName(app), ServiceNameExtension)
	if !(len(driverPodServiceName) <= KubernetesDNSLabelNameMaxLength) {
		timeInString := strconv.Itoa(int(time.Now().Unix()))
		randomHexString, _ := randomHex(10)
		randomServiceId := randomHexString + timeInString
		driverPodServiceName = SparkWithDash + randomServiceId + SparkAppDriverServiceNameExtension
	}
	return driverPodServiceName
}

// Helper func to create random string of given length to create unique service name for the Spark Application Driver Pod
func randomHex(n int) (string, error) {
	bytes := make([]byte, n)
	return hex.EncodeToString(bytes), nil
}
