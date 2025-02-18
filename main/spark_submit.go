package main

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/kubeflow/spark-operator/api/v1beta2"
	"nativesubmit/config"
	ctrlClient "sigs.k8s.io/controller-runtime/pkg/client"
	"strings"
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
	uuidString := uuid.New().String()
	uuidString = strings.ReplaceAll(uuidString, "-", "")
	createdApplicationId := fmt.Sprintf("%s-%s", Spark, uuidString)

	//Updte Application CRD Instnce with Spark Application ID
	app.Status.SparkApplicationID = createdApplicationId

	//Create Spark Application ConfigMap Name with the convention followed in Scala/Java
	driverConfigMapName := getDriverPodName(app) + ConfigMapExtension

	serviceName := getServiceName(app)

	//Updte Application CRD Instnce with Submission ID
	app.Status.SubmissionID = submissionID

	//Create Service Labels by aggregating Spark Application Specification level, driver specification level and dynamic lables
	serviceLabels := map[string]string{config.SparkAppNameLabel: app.Name}
	serviceLabels[SparkAppName] = app.Name
	serviceLabels[config.SparkApplicationSelectorLabel] = createdApplicationId
	serviceLabels[config.SparkRoleLabel] = config.SparkDriverRole
	serviceLabels[SparkAppSubmissionIDAnnotation] = submissionID
	serviceLabels[SparkAppLauncherSOAnnotation] = True

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
	if sparkConfKeyValuePairs != nil {
		for sparkConfKey, sparkConfValue := range sparkConfKeyValuePairs {
			if strings.Contains(sparkConfKey, "spark.kubernetes.driver.label.") {
				lastDotIndex := strings.LastIndex(sparkConfKey, DotSeparator)
				labelKey := sparkConfKey[lastDotIndex+1:]
				labelValue := sparkConfValue
				serviceLabels[labelKey] = labelValue
			}
		}
	}

	//Spark Application ConfigMap Creation
	createErr := createSparkAppConfigMap(app, submissionID, createdApplicationId, kubeClient, driverConfigMapName, serviceName)
	if createErr != nil {
		return false, fmt.Errorf("error while creating config mape: %w", createErr)
	}

	//Spark Application Driver Pod Creation
	createPodErr := createDriverPod(app, serviceLabels, driverConfigMapName, kubeClient, appSpecVolumeMounts, appSpecVolumes)
	if createPodErr != nil {
		return false, fmt.Errorf("error while creating driver pod: %w", createPodErr)
	}
	//Spark Application Driver Pod's Service Creation
	createServiceErr := createDriverService(app, serviceLabels, kubeClient, createdApplicationId, serviceName)
	if createServiceErr != nil {
		return false, fmt.Errorf("error while creating driver service: %w", createServiceErr)
	}
	return true, nil
}

const (
	SparkDriverArg                       = "driver"
	SparkDriverArgPropertiesFile         = "--properties-file"
	SparkDriverArgClass                  = "--class"
	SparkDriverArgPropertyFilePath       = "/opt/spark/conf/spark.properties"
	SparkDefaultsConfigFilePath          = "/opt/spark/conf/"
	SparkUser                            = "SPARK_USER"
	SparkApplicationID                   = "SPARK_APPLICATION_ID"
	SparkDriverBindAddress               = "SPARK_DRIVER_BIND_ADDRESS"
	ApiVersionV1                         = "v1"
	SparkDriverPodIP                     = "status.podIP"
	SparkLocalDir                        = "SPARK_LOCAL_DIRS"
	SparkLocalDirPath                    = "/var/data/spark-"
	DefaultDriverPort                    = 7078
	DefaultBlockManagerPort              = 7079
	DriverPortName                       = "driver-rpc-port"
	BlockManagerPortName                 = "blockmanager"
	Protocol                             = "TCP"
	UiPortName                           = "spark-ui"
	UiPort                               = 4040
	Memory                               = "memory"
	Cpu                                  = "cpu"
	True                                 = "true"
	All                                  = "ALL"
	Version                              = "version"
	DriverPodTerminationLogPath          = "/dev/termination-log"
	DriverPodTerminationMessagePolicy    = "File"
	SparkConfVolumeDriver                = "spark-conf-volume-driver"
	SparkConfVolumeDriverMountPath       = "/opt/spark/conf"
	SparkLocalDirectoryName              = "spark-local-dir-"
	SparkDriverDNSPolicy                 = "ClusterFirst"
	DefaultTerminationGracePeriodSeconds = 30
	TolerationEffect                     = "NoExecute"
	NodeNotReady                         = "node.kubernetes.io/not-ready"
	NodeNotReachable                     = "node.kubernetes.io/unreachable"
	Operator                             = "Exists"
	DefaultTolerationSeconds             = 300
	SparkEnvScriptFileName               = "spark-env.sh"
	SparkEnvScriptFileCommand            = "export SPARK_LOCAL_IP=$(hostname -i)\n"
	SparkPropertiesFileName              = "spark.properties"
	ConfigMapExtension                   = "-conf-map"
	SparkAppSubmissionIDAnnotation       = "sparkoperator.k8s.io/submission-id"
	SparkAppLauncherSOAnnotation         = "sparkoperator.k8s.io/launched-by-spark-operator"
	ServiceNameExtension                 = "-svc"
	None                                 = "None"
	ClusterIP                            = "ClusterIP"
	SparkUserId                          = "185"
	ImagePullPolicyIfNotPresent          = "IfNotPresent"
	SparkMaster                          = "spark.master"
	SparkDriverHost                      = "spark.driver.host"
	ServiceShortForm                     = "svc"
	DotSeparator                         = "."
	SparkSubmitDeploymentMode            = "spark.submit.deployMode"
	NewLineString                        = "\n"
	CommaSeparator                       = ","
	EqualsSign                           = "="
	SparkJars                            = "spark.jars"
	SparkFiles                           = "spark.files"
	SparkPyFiles                         = "spark.pyFiles"
	SparkPackages                        = "spark.packages"
	SparkExcludePackages                 = "spark.excludePackages"
	SparkRepositories                    = "spark.repositories"
	SparkAppId                           = "spark.app.id"
	DriverPodRestartPolicyNever          = "Never"
	Spark                                = "spark"
	OpencensusPrometheusTarget           = "opencensus.k8s-integration.sfdc.com/prometheus-targets"
	SubmitInDriver                       = "spark.kubernetes.submitInDriver"
	SparkDriverBlockManagerPort          = "spark.driver.blockManager.port"
	SparkBlockManagerPort                = "spark.blockManager.port"
	SparkDriverPort                      = "spark.driver.port"
	SparkApplicationType                 = "spark.kubernetes.resource.type"
	SparkApplicationSubmitTime           = "spark.app.submitTime"
	SparkAppName                         = "spark-app-name"
	SparkDriverEnvPrefix                 = "spark.kubernetes.driverEnv"
	SparkNodeSelectorPrefix              = "spark.kubernetes.node.selector."
	SparkDriverPodNodeSelectorPrefix     = "spark.kubernetes.driver.node.selector."
	SparkUIProxyBase                     = "spark.ui.proxyBase"
	ForwardSlash                         = "/"
	SparkMetricsNamespaceKey             = "spark.metrics.namespace"
	SparkMetricConfKey                   = "spark.metrics.conf"
	SparkUIProxyRedirectURI              = "spark.ui.proxyRedirectUri"
	SparkDriverCores                     = "spark.driver.cores"
	SparkDriverMemory                    = "spark.driver.memory"
	DefaultSparkConfFileName             = "spark-defaults.conf"
	DriverDefaultMemory                  = "1024m"
	ExecutorDefaultMemory                = "1g"
	DriverDefaultCores                   = "1"
	DriverMemoryOverheadDefault          = "0.1"
	SparkAppTypeJava                     = "java"
	SparkAppTypeJavaCamelCase            = "Java"
	SparkAppTypeScala                    = "Scala"
	SparkAppTypePython                   = "python"
	SparkAppTypePythonWithP              = "Python"
	SparkAppTypeR                        = "r"
	SparkAppTypeRWithR                   = "R"
	SparkDriverExtraClassPath            = "spark.driver.extraClassPath"
	SparkExecutorExtraClassPath          = "spark.executor.extraClassPath"
	KubernetesDNSLabelNameMaxLength      = 63
	HadoopConfDirPath                    = "/opt/hadoop/conf"
	HadoopConfVolume                     = "HADOOP_CONF_VOLUME"
	HadoopConfDir                        = "HADOOP_CONF_DIR"
	KerberosPath                         = "spark.kubernetes.kerberos.krb5.path"
	KerberosConfigMapName                = "spark.kubernetes.kerberos.krb5.configMapName"
	KerberosFileVolume                   = "krb5-file"
	KerberosFileDirectoryPath            = "/etc"
	KerberosFileName                     = "krb5.conf"
	KerberosTokenSecretItemKey           = "spark.kubernetes.kerberos.tokenSecret.itemKey"
	KerberosHadoopSecretFilePathKey      = "HADOOP_TOKEN_FILE_LOCATION"
	KerberosHadoopSecretFilePath         = "/mnt/secrets/hadoop-credentials/"

	OAuthTokenConfFile                   = "spark.kubernetes.authenticate.driver.oauthTokenFile"
	ClientKeyFile                        = "spark.kubernetes.authenticate.driver.clientKeyFile"
	ClientCertFile                       = "spark.kubernetes.authenticate.driver.clientCertFile"
	CaCertFile                           = "spark.kubernetes.authenticate.driver.caCertFile"
	KubernetesCredentials                = "kubernetes-credentials"
	KubernetesCredentialsVolumeMountPath = "/mnt/secrets/spark-kubernetes-credentials"
	LocalStoragePrefix                   = "spark-local-dir-"
	SparkWithDash                        = "spark-"
	SparkAppDriverServiceNameExtension   = "-driver-svc"
	DriverPodSecurityContextID           = 185
	JavaScalaMemoryOverheadFactor        = "0.10"
	OtherLanguageMemoryOverheadFactor    = "0.40"
	DriverPortProperty                   = "spark.driver.port"
	DriverBlockManagerPortProperty       = "spark.driver.blockManager.port"
	SparkBlockManagerPortProperty        = "spark.blockManager.port"
)
