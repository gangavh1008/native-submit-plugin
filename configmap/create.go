package configmap

import (
	"fmt"
	"nativesubmit/common"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/magiconair/properties"
	ctrlClient "sigs.k8s.io/controller-runtime/pkg/client"
)

// Function to create Spark Application Configmap
// Spark Application ConfigMap is pre-requisite for Driver Pod Creation; this configmap is mounted on driver pod
// Spark Application ConfigMap acts as configuration repository for the Driver, executor pods
func Create(app *v1beta2.SparkApplication, submissionID string, createdApplicationId string, kubeClient ctrlClient.Client, driverConfigMapName string, serviceName string) error {
	var errorSubmissionCommandArgs error

	//ConfigMap is created with Key, Value Pairs
	driverConfigMapData := make(map[string]string)
	//Followed the convention of Scala Implementation for this attribute, constant value is assigned
	driverConfigMapData[SparkEnvScriptFileName] = SparkEnvScriptFileCommand
	//Spark Application namespace can be passed in either application spec metadata or in sparkConf property
	driverConfigMapData[SparkAppNamespaceKey] = common.GetAppNamespace(app)

	// Utility function buildAltSubmissionCommandArgs to add other key, value configuration pairs
	driverConfigMapData[SparkPropertiesFileName], errorSubmissionCommandArgs = buildAltSubmissionCommandArgs(app, common.GetDriverPodName(app), submissionID, createdApplicationId, serviceName)
	if errorSubmissionCommandArgs != nil {
		return fmt.Errorf("failed to create submission command args for the driver configmap %s in namespace %s: %v", driverConfigMapName, app.Namespace, errorSubmissionCommandArgs)
	}
	//Create Spark Application ConfigMap
	createErr := createConfigMapUtil(driverConfigMapName, app, driverConfigMapData, kubeClient)
	if createErr != nil {
		return fmt.Errorf("failed to create/update driver configmap %s in namespace %s: %v", driverConfigMapName, app.Namespace, createErr)
	}
	return nil
}

// CreateConfigMapUtil Helper func to create Spark Application configmap

// Helper func to create key/value pairs required for the Spark Application Configmap
// Majority of the code borrowed from Scala implementation
func buildAltSubmissionCommandArgs(app *v1beta2.SparkApplication, driverPodName string, submissionID string, createdApplicationId string, serviceName string) (string, error) {
	var args string
	sparkConfKeyValuePairs := app.Spec.SparkConf
	masterURL, err := getMasterURL()
	if err != nil {
		return args, err
	}
	masterURL = AddEscapeCharacter(masterURL)

	//Construct Service Name
	//<servicename>.<namespace>.svc
	serviceName = serviceName + DotSeparator + app.Namespace + DotSeparator + ServiceShortForm

	args = args + fmt.Sprintf("%s=%s", SparkDriverHost, serviceName) + NewLineString

	args = args + fmt.Sprintf("%s=%s", SparkAppId, createdApplicationId) + NewLineString

	args = args + fmt.Sprintf("%s=%s", SparkMaster, masterURL) + NewLineString

	args = args + fmt.Sprintf("%s=%s", SparkSubmitDeploymentMode, string(app.Spec.Mode)) + NewLineString

	args = args + fmt.Sprintf("%s=%s", SparkAppNamespaceKey, app.Namespace) + NewLineString

	args = args + fmt.Sprintf("%s=%s", SparkAppNameKey, app.Name) + NewLineString

	args = args + fmt.Sprintf("%s=%s", SparkDriverPodNameKey, driverPodName) + NewLineString

	args = populateArtifacts(args, *app)

	args = populateContainerImageDetails(args, *app)
	if app.Spec.PythonVersion != nil {
		args = args + fmt.Sprintf("%s=%s", SparkPythonVersion, *app.Spec.PythonVersion) + NewLineString
	}
	if app.Spec.MemoryOverheadFactor != nil {
		args = args + fmt.Sprintf("%s=%s", SparkMemoryOverheadFactor, *app.Spec.MemoryOverheadFactor) + NewLineString
	} else {

		args = args + fmt.Sprintf("%s=%s", SparkMemoryOverheadFactor, common.GetMemoryOverheadFactor(app)) + NewLineString
	}

	// Operator triggered spark-submit should never wait for App completion
	args = args + fmt.Sprintf("%s=false", SparkWaitAppCompletion) + NewLineString

	args = populateSparkConfProperties(args, *app, sparkConfKeyValuePairs)

	// Add Hadoop configuration properties.
	for key, value := range app.Spec.HadoopConf {
		args = args + fmt.Sprintf("spark.hadoop.%s=%s", key, value) + NewLineString
	}
	if app.Spec.HadoopConf != nil || app.Spec.HadoopConfigMap != nil {
		// Adding Environment variable
		args = args + fmt.Sprintf("spark.hadoop.%s=%s", HadoopConfDir, HadoopConfDirPath) + NewLineString
	}

	// Add the driver and executor configuration options.
	// Note that when the controller submits the application, it expects that all dependencies are local
	// so init-container is not needed and therefore no init-container image needs to be specified.
	args = args + fmt.Sprintf("%s%s=%s", SparkDriverLabelKeyPrefix, SparkAppNameLabel, app.Name) + NewLineString
	//driverConfOptions = append(driverConfOptions,
	args = args + fmt.Sprintf("%s%s=%s", SparkDriverLabelKeyPrefix, LaunchedBySparkOperatorLabel, "true") + NewLineString

	args = args + fmt.Sprintf("%s%s=%s", SparkDriverLabelKeyPrefix, SubmissionIDLabel, submissionID) + NewLineString

	if app.Spec.Driver.Image != nil {
		args = args + fmt.Sprintf("%s=%s", SparkDriverContainerImageKey, *app.Spec.Driver.Image) + NewLineString
	}

	args, err = populateComputeInfo(args, *app, sparkConfKeyValuePairs)
	if err != nil {
		return "Driver cores should be an integer", err
	}

	args = populateMemoryInfo(args, *app, sparkConfKeyValuePairs)

	if app.Spec.Driver.ServiceAccount != nil {
		args = args + fmt.Sprintf("%s=%s", SparkDriverServiceAccountName, *app.Spec.Driver.ServiceAccount) + NewLineString
	}

	if app.Spec.Driver.JavaOptions != nil {
		driverJavaOptionsList := AddEscapeCharacter(*app.Spec.Driver.JavaOptions)
		args = args + fmt.Sprintf("%s=%s", SparkDriverJavaOptions, driverJavaOptionsList) + NewLineString
	}

	if app.Spec.Driver.KubernetesMaster != nil {
		args = args + fmt.Sprintf("%s=%s", SparkDriverKubernetesMaster, *app.Spec.Driver.KubernetesMaster) + NewLineString
	}

	//Populate SparkApplication Labels to Driver
	driverLabels := make(map[string]string)
	for key, value := range app.Labels {
		driverLabels[key] = value
	}
	for key, value := range app.Spec.Driver.Labels {
		driverLabels[key] = value
	}

	for key, value := range driverLabels {
		args = args + fmt.Sprintf("%s%s=%s", SparkDriverLabelKeyPrefix, key, value) + NewLineString
	}
	args = populateDriverAnnotations(args, *app)

	for key, value := range app.Spec.Driver.EnvSecretKeyRefs {
		args = args + fmt.Sprintf("%s%s=%s:%s", SparkDriverSecretKeyRefKeyPrefix, key, value.Name, value.Key) + NewLineString
	}

	for key, value := range app.Spec.Driver.ServiceAnnotations {
		args = args + fmt.Sprintf("%s%s=%s", SparkDriverServiceAnnotationKeyPrefix, key, value) + NewLineString
	}

	args = populateDriverSecrets(args, *app)

	for key, value := range app.Spec.Driver.EnvVars {
		args = args + fmt.Sprintf("%s%s=%s", SparkDriverEnvVarConfigKeyPrefix, key, value) + NewLineString

	}

	for key, value := range app.Spec.Driver.Env {
		args = args + fmt.Sprintf("%s%d=%s", SparkDriverEnvVarConfigKeyPrefix, key, value) + NewLineString
	}

	args = args + fmt.Sprintf("%s%s=%s", SparkExecutorLabelKeyPrefix, SparkAppNameLabel, app.Name) + NewLineString

	args = args + fmt.Sprintf("%s%s=%s", SparkExecutorLabelKeyPrefix, LaunchedBySparkOperatorLabel, "true") + NewLineString

	args = args + fmt.Sprintf("%s%s=%s", SparkExecutorLabelKeyPrefix, SubmissionIDLabel, submissionID) + NewLineString

	if app.Spec.Executor.Instances != nil {
		args = args + fmt.Sprintf("spark.executor.instances=%d", *app.Spec.Executor.Instances) + NewLineString
	}

	if app.Spec.Executor.Image != nil {
		args = args + fmt.Sprintf("%s=%s", SparkExecutorContainerImageKey, *app.Spec.Executor.Image) + NewLineString
	}

	if app.Spec.Executor.ServiceAccount != nil {
		args = args + fmt.Sprintf("%s=%s", SparkExecutorAccountName, *app.Spec.Executor.ServiceAccount) + NewLineString
	}

	if app.Spec.Executor.DeleteOnTermination != nil {
		args = args + fmt.Sprintf("%s=%t", SparkExecutorDeleteOnTermination, *app.Spec.Executor.DeleteOnTermination) + NewLineString
	}

	//Populate SparkApplication Labels to Executors
	executorLabels := make(map[string]string)
	for key, value := range app.Labels {
		executorLabels[key] = value
	}
	for key, value := range app.Spec.Executor.Labels {
		executorLabels[key] = value
	}
	for key, value := range executorLabels {
		args = args + fmt.Sprintf("%s%s=%s", SparkExecutorLabelKeyPrefix, key, value) + NewLineString
	}

	args = populateExecutorAnnotations(args, *app)

	for key, value := range app.Spec.Executor.EnvSecretKeyRefs {
		args = args + fmt.Sprintf("%s%s=%s:%s", SparkExecutorSecretKeyRefKeyPrefix, key, value.Name, value.Key) + NewLineString
	}

	if app.Spec.Executor.JavaOptions != nil {
		args = args + fmt.Sprintf("%s=%s", SparkExecutorJavaOptions, *app.Spec.Executor.JavaOptions) + NewLineString
	}

	args = populateExecutorSecrets(args, *app)

	for key, value := range app.Spec.Executor.EnvVars {
		args = args + fmt.Sprintf("%s%s=%s", SparkExecutorEnvVarConfigKeyPrefix, key, value) + NewLineString
	}

	args = populateDynamicAllocation(args, *app)
	for key, value := range app.Spec.NodeSelector {
		args = args + fmt.Sprintf("%s%s=%s", SparkNodeSelectorKeyPrefix, key, value) + NewLineString
	}
	for key, value := range app.Spec.Driver.NodeSelector {
		args = args + fmt.Sprintf("%s%s=%s", SparkDriverNodeSelectorKeyPrefix, key, value) + NewLineString
	}
	for key, value := range app.Spec.Executor.NodeSelector {
		args = args + fmt.Sprintf("%s%s=%s", SparkExecutorNodeSelectorKeyPrefix, key, value) + NewLineString
	}

	args = args + fmt.Sprintf("%s=%s", SubmitInDriver, True) + NewLineString

	args = args + fmt.Sprintf("%s=%v", SparkDriverBlockManagerPort, common.DefaultBlockManagerPort) + NewLineString
	//
	args = args + fmt.Sprintf("%s=%v", common.SparkDriverPort, common.GetDriverPort(sparkConfKeyValuePairs)) + NewLineString
	//
	args = populateAppSpecType(args, *app)

	args = args + fmt.Sprintf("%s=%v", SparkApplicationSubmitTime, time.Now().UnixMilli()) + NewLineString

	sparkUIProxyBase := ForwardSlash + common.GetAppNamespace(app) + ForwardSlash + app.Name

	args = args + fmt.Sprintf("%s=%s", SparkUIProxyBase, sparkUIProxyBase) + NewLineString

	args = args + fmt.Sprintf("%s=%s", SparkUIProxyRedirectURI, ForwardSlash) + NewLineString

	args = populateProperties(args, *app)

	args = populateMonitoringInfo(args, *app)

	// Volumes
	if app.Spec.Volumes != nil {
		options, err := addLocalDirConfOptions(app)
		if err != nil {
			return "Error occcurred while building configmap", err
		}
		for _, option := range options {
			args = args + option + NewLineString
		}
	}

	if app.Spec.MainApplicationFile != nil {
		// Add the main application file if it is present.
		sparkAppJar := AddEscapeCharacter(*app.Spec.MainApplicationFile)
		args = args + fmt.Sprintf("%s=%s", SparkJars, sparkAppJar) + NewLineString
	}
	// Add application arguments.
	for _, argument := range app.Spec.Arguments {
		args = args + argument + NewLineString
	}

	return args, nil
}
func populateDriverAnnotations(args string, app v1beta2.SparkApplication) string {
	for key, value := range app.Spec.Driver.Annotations {
		if key == OpencensusPrometheusTarget {
			value = strings.Replace(value, "\n", "", -1)
			value = AddEscapeCharacter(value)
		}
		args = args + fmt.Sprintf("%s%s=%s", SparkDriverAnnotationKeyPrefix, key, value) + NewLineString
	}
	return args
}
func populateSparkConfProperties(args string, app v1beta2.SparkApplication, sparkConfKeyValuePairs map[string]string) string {
	// Priority wise: Spark Application Specification value, if not, then value in sparkConf, if not, then, defaults that get applied by Spark Environment of Driver pod
	// Add Spark configuration properties.
	for key, value := range sparkConfKeyValuePairs {
		// Configuration property for the driver pod name has already been set.
		if key != SparkDriverPodNameKey {
			//Adding escape character for the spark.executor.extraClassPath and spark.driver.extraClassPath
			if key == SparkDriverExtraClassPath || key == SparkExecutorExtraClassPath {
				value = AddEscapeCharacter(value)
			}
			args = args + fmt.Sprintf("%s=%s", key, value) + NewLineString
		}
	}
	return args
}
func populateDriverSecrets(args string, app v1beta2.SparkApplication) string {
	for _, s := range app.Spec.Driver.Secrets {
		args = args + fmt.Sprintf("%s%s=%s", SparkDriverSecretKeyPrefix, s.Name, s.Path) + NewLineString
		//secretConfOptions = append(secretConfOptions, conf)
		if s.Type == v1beta2.SecretTypeGCPServiceAccount {
			args = args + fmt.Sprintf(
				"%s%s=%s",
				SparkDriverEnvVarConfigKeyPrefix,
				GoogleApplicationCredentialsEnvVar,
				filepath.Join(s.Path, ServiceAccountJSONKeyFileName)) + NewLineString

		} else if s.Type == v1beta2.SecretTypeHadoopDelegationToken {
			args = args + fmt.Sprintf(
				"%s%s=%s",
				SparkDriverEnvVarConfigKeyPrefix,
				HadoopTokenFileLocationEnvVar,
				filepath.Join(s.Path, HadoopDelegationTokenFileName)) + NewLineString

		}
	}
	return args
}
func populateExecutorAnnotations(args string, app v1beta2.SparkApplication) string {
	for key, value := range app.Spec.Executor.Annotations {
		if key == OpencensusPrometheusTarget {
			value = strings.Replace(value, "\n", "", -1)
			value = AddEscapeCharacter(value)
		}
		args = args + fmt.Sprintf("%s%s=%s", SparkExecutorAnnotationKeyPrefix, key, value) + NewLineString
	}
	return args
}
func populateExecutorSecrets(args string, app v1beta2.SparkApplication) string {
	for _, s := range app.Spec.Executor.Secrets {
		args = args + fmt.Sprintf("%s%s=%s", SparkExecutorSecretKeyPrefix, s.Name, s.Path) + NewLineString
		if s.Type == v1beta2.SecretTypeGCPServiceAccount {
			args = args + fmt.Sprintf(
				"%s%s=%s",
				SparkExecutorEnvVarConfigKeyPrefix,
				GoogleApplicationCredentialsEnvVar,
				filepath.Join(s.Path, ServiceAccountJSONKeyFileName)) + NewLineString

		} else if s.Type == v1beta2.SecretTypeHadoopDelegationToken {
			args = args + fmt.Sprintf(
				"%s%s=%s",
				SparkExecutorEnvVarConfigKeyPrefix,
				HadoopTokenFileLocationEnvVar,
				filepath.Join(s.Path, HadoopDelegationTokenFileName)) + NewLineString
		}
	}
	return args
}
func populateDynamicAllocation(args string, app v1beta2.SparkApplication) string {
	if app.Spec.DynamicAllocation != nil {
		args = args + fmt.Sprintf("%s=true", SparkDynamicAllocationEnabled) + NewLineString
		// Turn on shuffle tracking if dynamic allocation is enabled.
		args = args + fmt.Sprintf("%s=true", SparkDynamicAllocationShuffleTrackingEnabled) + NewLineString
		dynamicAllocation := app.Spec.DynamicAllocation
		if dynamicAllocation.InitialExecutors != nil {
			args = args + fmt.Sprintf("%s=%d", SparkDynamicAllocationInitialExecutors, *dynamicAllocation.InitialExecutors) + NewLineString
		}
		if dynamicAllocation.MinExecutors != nil {
			args = args + fmt.Sprintf("%s=%d", SparkDynamicAllocationMinExecutors, *dynamicAllocation.MinExecutors) + NewLineString
		}
		if dynamicAllocation.MaxExecutors != nil {
			args = args + fmt.Sprintf("%s=%d", SparkDynamicAllocationMaxExecutors, *dynamicAllocation.MaxExecutors) + NewLineString
		}
		if dynamicAllocation.ShuffleTrackingTimeout != nil {
			args = args + fmt.Sprintf("%s=%d", SparkDynamicAllocationShuffleTrackingTimeout, *dynamicAllocation.ShuffleTrackingTimeout) + NewLineString
		}
	}
	return args
}
func populateAppSpecType(args string, app v1beta2.SparkApplication) string {
	appSpecType := app.Spec.Type
	if appSpecType == SparkAppTypeScala || appSpecType == SparkAppTypeJavaCamelCase {
		appSpecType = SparkAppTypeJava
	} else if appSpecType == SparkAppTypePythonWithP {
		appSpecType = SparkAppTypePython
	} else if appSpecType == SparkAppTypeRWithR {
		appSpecType = SparkAppTypeR
	}
	args = args + fmt.Sprintf("%s=%s", SparkApplicationType, appSpecType) + NewLineString
	return args
}
func populateProperties(args string, app v1beta2.SparkApplication) string {
	sparkDefaultConfFilePath := SparkDefaultsConfigFilePath + DefaultSparkConfFileName
	propertyPairs, propertyFileReadError := properties.LoadFile(sparkDefaultConfFilePath, properties.UTF8)
	if propertyFileReadError == nil {
		keysList := propertyPairs.Keys()
		for _, key := range keysList {
			value, _ := propertyPairs.Get(key)
			args = args + fmt.Sprintf("%s=%s", key, value) + NewLineString
		}
	}
	return args
}
func populateMonitoringInfo(args string, app v1beta2.SparkApplication) string {
	//Monitoring Section
	if app.Spec.Monitoring != nil {
		SparkMetricsNamespace := common.GetAppNamespace(&app) + DotSeparator + app.Name
		args = args + fmt.Sprintf("%s=%s", SparkMetricsNamespaceKey, SparkMetricsNamespace) + NewLineString

		// Spark Metric Properties file
		if app.Spec.Monitoring.MetricsPropertiesFile != nil {
			args = args + fmt.Sprintf("%s=%s", SparkMetricConfKey, *app.Spec.Monitoring.MetricsPropertiesFile) + NewLineString
		}
	}
	return args
}
func populateArtifacts(args string, app v1beta2.SparkApplication) string {
	if app.Spec.Deps.Jars != nil && len(app.Spec.Deps.Jars) > 0 {
		modifiedJarList := make([]string, len(app.Spec.Deps.Jars))
		for _, sparkjar := range app.Spec.Deps.Jars {
			sparkjar = AddEscapeCharacter(sparkjar)
			modifiedJarList = append(modifiedJarList, sparkjar)
		}
		args = args + SparkJars + EqualsSign + strings.Join(modifiedJarList, CommaSeparator) + NewLineString
	}
	if app.Spec.Deps.Files != nil && len(app.Spec.Deps.Files) > 0 {
		args = args + SparkFiles + EqualsSign + strings.Join(app.Spec.Deps.Files, CommaSeparator) + NewLineString
	}
	if app.Spec.Deps.PyFiles != nil && len(app.Spec.Deps.PyFiles) > 0 {
		args = args + SparkPyFiles + EqualsSign + strings.Join(app.Spec.Deps.PyFiles, CommaSeparator) + NewLineString
	}
	if app.Spec.Deps.Packages != nil && len(app.Spec.Deps.Packages) > 0 {
		args = args + SparkPackages + EqualsSign + strings.Join(app.Spec.Deps.Packages, CommaSeparator) + NewLineString
	}
	if app.Spec.Deps.ExcludePackages != nil && len(app.Spec.Deps.ExcludePackages) > 0 {
		args = args + SparkExcludePackages + EqualsSign + strings.Join(app.Spec.Deps.ExcludePackages, CommaSeparator) + NewLineString
	}
	if app.Spec.Deps.Repositories != nil && len(app.Spec.Deps.Repositories) > 0 {
		args = args + SparkRepositories + EqualsSign + strings.Join(app.Spec.Deps.Repositories, CommaSeparator) + NewLineString
	}
	return args
}
func populateContainerImageDetails(args string, app v1beta2.SparkApplication) string {
	if app.Spec.Image != nil {
		sparkContainerImage := AddEscapeCharacter(*app.Spec.Image)
		args = args + fmt.Sprintf("%s=%s", SparkContainerImageKey, sparkContainerImage) + NewLineString
	}
	if app.Spec.ImagePullPolicy != nil {
		args = args + fmt.Sprintf("%s=%s", SparkContainerImagePullPolicyKey, *app.Spec.ImagePullPolicy) + NewLineString
	}
	if len(app.Spec.ImagePullSecrets) > 0 {
		secretNames := strings.Join(app.Spec.ImagePullSecrets, CommaSeparator)
		args = args + fmt.Sprintf("%s=%s", SparkImagePullSecretKey, secretNames) + NewLineString
	}
	return args
}
func populateComputeInfo(args string, app v1beta2.SparkApplication, sparkConfKeyValuePairs map[string]string) (string, error) {
	if app.Spec.Driver.Cores != nil {
		args = args + fmt.Sprintf("%s=%d", SparkDriverCores, *app.Spec.Driver.Cores) + NewLineString
	} else if common.CheckSparkConf(sparkConfKeyValuePairs, SparkDriverCores) {
		driverCores, err := strconv.ParseInt(sparkConfKeyValuePairs[SparkDriverCores], 10, 32)
		if err != nil {
			return "Driver cores should be an integer", err
		}
		args = args + fmt.Sprintf("%s=%d", SparkDriverCores, driverCores) + NewLineString
	} else { // Driver default cores
		args = args + fmt.Sprintf("%s=%s", SparkDriverCores, DriverDefaultCores) + NewLineString
	}

	if app.Spec.Driver.CoreRequest != nil {
		args = args + fmt.Sprintf("%s=%s", SparkDriverCoreRequestKey, *app.Spec.Driver.CoreRequest) + NewLineString
	} else if common.CheckSparkConf(sparkConfKeyValuePairs, SparkDriverCoreRequestKey) {
		args = args + fmt.Sprintf("%s=%s", SparkDriverCoreRequestKey, sparkConfKeyValuePairs[SparkDriverCoreRequestKey]) + NewLineString
	}

	if app.Spec.Driver.CoreLimit != nil {
		args = args + fmt.Sprintf("%s=%s", SparkDriverCoreLimitKey, *app.Spec.Driver.CoreLimit) + NewLineString
	} else if common.CheckSparkConf(sparkConfKeyValuePairs, SparkDriverCoreLimitKey) {
		args = args + fmt.Sprintf("%s=%s", SparkDriverCoreLimitKey, sparkConfKeyValuePairs[SparkDriverCoreLimitKey]) + NewLineString
	}

	if app.Spec.Executor.CoreRequest != nil {
		args = args + fmt.Sprintf("%s=%s", SparkExecutorCoreRequestKey, *app.Spec.Executor.CoreRequest) + NewLineString
	} else if common.CheckSparkConf(sparkConfKeyValuePairs, SparkExecutorCoreRequestKey) {
		args = args + fmt.Sprintf("%s=%s", SparkExecutorCoreRequestKey, sparkConfKeyValuePairs[SparkExecutorCoreRequestKey]) + NewLineString
	}
	return args, nil
}
func populateMemoryInfo(args string, app v1beta2.SparkApplication, sparkConfKeyValuePairs map[string]string) string {

	if app.Spec.Driver.Memory != nil {
		args = args + fmt.Sprintf("spark.driver.memory=%s", *app.Spec.Driver.Memory) + NewLineString
	} else { //Driver default memory
		args = args + fmt.Sprintf("spark.driver.memory=%s", DriverDefaultMemory) + NewLineString

	}

	if app.Spec.Driver.MemoryOverhead != nil {
		args = args + fmt.Sprintf("spark.driver.memoryOverhead=%v", *app.Spec.Driver.MemoryOverhead) + NewLineString
	} else if common.CheckSparkConf(app.Spec.SparkConf, "spark.driver.memoryOverhead") {
		memoryOverhead, _ := app.Spec.SparkConf["spark.driver.memoryOverhead"]
		args = args + fmt.Sprintf("spark.driver.memoryOverhead=%v", memoryOverhead) + NewLineString
	} else if common.CheckSparkConf(app.Spec.SparkConf, "spark.kubernetes.memoryOverhead") {
		memoryOverhead, _ := app.Spec.SparkConf["spark.kubernetes.memoryOverhead"]
		args = args + fmt.Sprintf("spark.driver.memoryOverhead=%v", memoryOverhead) + NewLineString
	}

	// Property "spark.executor.cores" does not allow float values.
	if app.Spec.Executor.Cores != nil {
		args = args + fmt.Sprintf("%s=%d", SparkExecutorCoreKey, *app.Spec.Executor.Cores) + NewLineString
	} else if common.CheckSparkConf(sparkConfKeyValuePairs, SparkExecutorCoreKey) {
		val, _ := strconv.ParseInt(sparkConfKeyValuePairs[SparkExecutorCoreKey], 10, 32)
		args = args + fmt.Sprintf("%s=%d", SparkExecutorCoreKey, val) + NewLineString
	}

	if app.Spec.Executor.CoreLimit != nil {
		args = args + fmt.Sprintf("%s=%s", SparkExecutorCoreLimitKey, *app.Spec.Executor.CoreLimit) + NewLineString
	} else if common.CheckSparkConf(sparkConfKeyValuePairs, SparkExecutorCoreLimitKey) {
		args = args + fmt.Sprintf("%s=%s", SparkExecutorCoreLimitKey, sparkConfKeyValuePairs[SparkExecutorCoreLimitKey]) + NewLineString
	}

	if app.Spec.Executor.Memory != nil {
		args = args + fmt.Sprintf("spark.executor.memory=%s", *app.Spec.Executor.Memory) + NewLineString
	} else { //Setting default 1g
		//Executor default memory
		args = args + fmt.Sprintf("spark.executor.memory=%s", ExecutorDefaultMemory) + NewLineString
	}

	if app.Spec.Executor.MemoryOverhead != nil {
		args = args + fmt.Sprintf("spark.executor.memoryOverhead=%v", *app.Spec.Executor.MemoryOverhead) + NewLineString
	} else if common.CheckSparkConf(app.Spec.SparkConf, "spark.executor.memoryOverhead") {
		memoryOverhead, _ := app.Spec.SparkConf["spark.executor.memoryOverhead"]
		args = args + fmt.Sprintf("spark.executor.memoryOverhead=%v", memoryOverhead) + NewLineString
	} else if common.CheckSparkConf(app.Spec.SparkConf, "spark.kubernetes.memoryOverhead") {
		memoryOverhead, _ := app.Spec.SparkConf["spark.kubernetes.memoryOverhead"]
		args = args + fmt.Sprintf("spark.executor.memoryOverhead=%v", memoryOverhead) + NewLineString
	}

	return args
}
