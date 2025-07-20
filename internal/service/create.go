package service

import (
	"context"
	"fmt"
	"log"
	"nativesubmit/common"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/kubeflow/spark-operator/api/v1beta2"
	apiv1 "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
)

// Helper func to create Service for the Driver Pod of the Spark Application
func Create(app *v1beta2.SparkApplication, serviceSelectorLabels map[string]string, kubeClient *kubernetes.Clientset, createdApplicationId string, serviceName string) error {
	log.Printf("=== Starting Driver Service creation for app: %s, namespace: %s ===", app.Name, app.Namespace)
	log.Printf("Service name: %s, Application ID: %s", serviceName, createdApplicationId)
	log.Printf("Service selector labels count: %d", len(serviceSelectorLabels))

	if app == nil {
		log.Printf("ERROR: Spark application is nil")
		return fmt.Errorf("spark application cannot be nil")
	}

	//Service Schema populating with specific values/data
	var serviceObjectMetaData metav1.ObjectMeta

	serviceObjectMetaData.ResourceVersion = ""

	//Driver Pod Service name
	serviceObjectMetaData.Name = serviceName
	// Spark Application Namespace
	serviceObjectMetaData.Namespace = common.GetAppNamespace(app)
	// Service Schema Owner References
	serviceObjectMetaData.OwnerReferences = []metav1.OwnerReference{*common.GetOwnerReference(app)}
	//Service Schema label
	serviceLabels := map[string]string{SparkApplicationSelectorLabel: createdApplicationId}
	log.Printf("Service object metadata - Name: %s, Namespace: %s", serviceObjectMetaData.Name, serviceObjectMetaData.Namespace)

	ipFamilyString, ipFamilyExists := app.Spec.SparkConf["spark.kubernetes.driver.service.ipFamilies"]
	var ipFamily apiv1.IPFamily
	if ipFamilyExists {
		ipFamily = apiv1.IPFamily(ipFamilyString)
		log.Printf("Using IP family from sparkConf: %s", ipFamilyString)
	} else {
		//Default Value
		ipFamily = apiv1.IPFamily("IPv4")
		log.Printf("Using default IP family: IPv4")
	}
	var ipFamilies [1]apiv1.IPFamily
	ipFamilies[0] = ipFamily

	// labels passed in sparkConf
	sparkConfKeyValuePairs := app.Spec.SparkConf
	for sparkConfKey, sparkConfValue := range sparkConfKeyValuePairs {
		if strings.Contains(sparkConfKey, "spark.kubernetes.driver.service.label.") {
			lastDotIndex := strings.LastIndex(sparkConfKey, DotSeparator)
			labelKey := sparkConfKey[lastDotIndex+1:]
			labelValue := sparkConfValue
			serviceLabels[labelKey] = labelValue
		}
	}
	log.Printf("Service labels count after sparkConf processing: %d", len(serviceLabels))

	serviceObjectMetaData.Labels = serviceLabels
	//Service Schema Annotation
	if app.Spec.Driver.Annotations != nil {
		serviceObjectMetaData.Annotations = app.Spec.Driver.Annotations
		log.Printf("Using driver annotations for service")
	}

	//Service Schema Creation
	driverPodService := &apiv1.Service{
		ObjectMeta: serviceObjectMetaData,
		Spec: apiv1.ServiceSpec{
			ClusterIP: None,
			Ports: []apiv1.ServicePort{
				{
					Name:     DriverPortName,
					Port:     getDriverNBlockManagerPort(app, DriverPortProperty, common.DefaultDriverPort),
					Protocol: Protocol,
					TargetPort: intstr.IntOrString{
						IntVal: common.DefaultDriverPort,
					},
				},
				{
					Name:     BlockManagerPortName,
					Port:     getDriverPodBlockManagerPort(app),
					Protocol: Protocol,
					TargetPort: intstr.IntOrString{
						IntVal: common.DefaultBlockManagerPort,
					},
				},
				{
					Name:     UiPortName,
					Port:     UiPort,
					Protocol: Protocol,
					TargetPort: intstr.IntOrString{
						IntVal: UiPort,
					},
				},
			},
			Selector:        serviceSelectorLabels,
			SessionAffinity: None,
			Type:            ClusterIP,
			IPFamilies:      ipFamilies[:],
		},
	}
	log.Printf("Driver service object created with %d ports", len(driverPodService.Spec.Ports))

	//K8S API Server Call to create Service
	log.Printf("Attempting to create/update driver service...")
	createServiceErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		existingService := &apiv1.Service{}
		_, err := kubeClient.CoreV1().Services(app.Namespace).Get(context.TODO(), driverPodService.Name, metav1.GetOptions{})
		if apiErrors.IsNotFound(err) {
			log.Printf("Service not found, creating new one...")
			_, createErr := kubeClient.CoreV1().Services(app.Namespace).Create(context.TODO(), driverPodService, metav1.CreateOptions{})
			if createErr == nil {
				log.Printf("Service created successfully, checking service availability...")
				return createAndCheckDriverService(kubeClient, app, driverPodService, 5, serviceName)
			}
			log.Printf("ERROR: Failed to create service: %v", createErr)
			return createErr
		}
		if err != nil {
			log.Printf("ERROR: Failed to get existing service: %v", err)
			return err
		}

		log.Printf("Service exists, updating...")
		//Copying over the data to existing service
		existingService.ObjectMeta = serviceObjectMetaData
		existingService.Spec = driverPodService.Spec
		//updateErr := kubeClient.Update(context.TODO(), existingService)
		_, updateErr := kubeClient.CoreV1().Services(app.Namespace).Update(context.TODO(), existingService, metav1.UpdateOptions{})

		if updateErr != nil {
			log.Printf("ERROR: Failed to update service: %v", updateErr)
			return fmt.Errorf("error while updating driver service: %w", updateErr)
		}
		log.Printf("Service updated successfully")
		return updateErr
	})

	if createServiceErr != nil {
		log.Printf("ERROR: Final service creation/update failed: %v", createServiceErr)
	} else {
		log.Printf("=== Successfully completed Driver Service creation ===")
	}

	return createServiceErr
}

func createAndCheckDriverService(kubeClient *kubernetes.Clientset, app *v1beta2.SparkApplication, driverPodService *apiv1.Service, attemptCount int, serviceName string) error {
	const sleepDuration = 2000 * time.Millisecond

	for iteration := 0; iteration < attemptCount; iteration++ {

		_, err := kubeClient.CoreV1().Services(driverPodService.Namespace).Get(context.TODO(), driverPodService.Name, metav1.GetOptions{})

		if apiErrors.IsNotFound(err) {
			time.Sleep(sleepDuration)
			glog.Info("Service does not exist, attempt #", iteration+2, " to create service for the app %s", app.Name)
			driverPodService.ResourceVersion = ""
			_, dvrSvcErr := kubeClient.CoreV1().Services(app.Namespace).Create(context.TODO(), driverPodService, metav1.CreateOptions{})
			if dvrSvcErr != nil {
				if !apiErrors.IsAlreadyExists(dvrSvcErr) {
					return fmt.Errorf("unable to create driver service : %w", dvrSvcErr)
				} else {
					glog.Info("Driver service already exists, ignoring attempt to create it")
				}
			}
		} else {
			glog.Info("Driver Service found in attempt", iteration+2, "for the app %s", app.Name)
			return nil
		}
	}

	return nil
}

func getDriverPodBlockManagerPort(app *v1beta2.SparkApplication) int32 {
	if common.CheckSparkConf(app.Spec.SparkConf, DriverBlockManagerPortProperty) {
		return getDriverNBlockManagerPort(app, DriverBlockManagerPortProperty, common.DefaultBlockManagerPort)
	}
	return common.DefaultBlockManagerPort
}

func getDriverNBlockManagerPort(app *v1beta2.SparkApplication, portConfig string, defaultPort int32) int32 {
	if common.CheckSparkConf(app.Spec.SparkConf, portConfig) {
		value, _ := app.Spec.SparkConf[portConfig]
		portVal, parseError := strconv.ParseInt(value, 10, 64)
		if parseError != nil {
			glog.Errorf("failed to parse %s in namespace %s: %v", portConfig, app.Namespace, parseError)
			return defaultPort
		}
		return int32(portVal)
	}
	return defaultPort
}
