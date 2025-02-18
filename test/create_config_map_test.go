package test

import (
	"context"
	"github.com/kubeflow/spark-operator/api/v1beta2"
	apiv1 "k8s.io/api/core/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"nativesubmit"
	ctrlClient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"testing"
)

var memoryQuantity resource.Quantity
var cpuQuantity resource.Quantity
var TestApp = &v1beta2.SparkApplication{
	ObjectMeta: metav1.ObjectMeta{
		Name:      "test-app",
		Namespace: "default",
		Labels:    map[string]string{"sparkdeps": "v3"},
	},
	Spec: v1beta2.SparkApplicationSpec{
		Arguments: []string{"pwd"},
		Deps: v1beta2.Dependencies{
			Jars:            []string{"local:///sample-apps/sample-basic-spark-operator/spark-extra-jars/examples.jar"},
			Files:           []string{"local:///sample-apps/sample-basic-spark-operator/test.properties"},
			PyFiles:         []string{"local:///sample-apps/sample-basic-spark-operator/test.py"},
			Packages:        []string{"datastax:spark-cassandra-connector:1.4.4-s_2.10"},
			ExcludePackages: []string{"com.amazonaws:amazon-kinesis-client"},
			Repositories:    []string{"test-maven-repository"},
		},
		DynamicAllocation: &v1beta2.DynamicAllocation{Enabled: true, InitialExecutors: int32Pointer(1), MinExecutors: int32Pointer(1), MaxExecutors: int32Pointer(1), ShuffleTrackingTimeout: nativesubmit.Int64Pointer(1)},
		HadoopConf:        map[string]string{"abc.def": "xyz", "jkl.ghi": "uvw"},
		Type:              v1beta2.SparkApplicationTypeScala,
		Mode:              v1beta2.DeployModeCluster,
		Image:             stringPointer("871501607754.dkr.ecr.us-west-2.amazonaws.com/sfci/dva-transformation/spark-on-k8s-sample-apps/flowsnake-basic-operator-integration-spark-3.3.2:jenkins-dva-transformation-spark-on-k8s-sample-apps-spark-3.3.2-sfdc-3-itest"),
		ImagePullPolicy:   stringPointer("IfNotPresent"),
		ImagePullSecrets: []string{
			"dummy-data",
		},
		MainClass:           stringPointer("org.apache.spark.examples.SparkPi"),
		MainApplicationFile: stringPointer("local:///sample-apps/sample-basic-spark-operator/sample-basic-spark-operator.jar"),
		PythonVersion:       stringPointer("/usr/local/bin/python3"),
		SparkVersion:        "",
		NodeSelector: map[string]string{
			"spark.authenticate": "true"},
		RestartPolicy: v1beta2.RestartPolicy{
			Type: "Never",
		},
		SparkConf: map[string]string{
			"spark.authenticate":                              "true",
			"spark.driver.extraClassPath":                     "local:///sample-apps/sample-basic-spark-operator/spark-extra-jars/*",
			"spark.executor.extraClassPath":                   "local:///sample-apps/sample-basic-spark-operator/spark-extra-jars/*",
			"spark.io.encryption.enabled":                     "true",
			"spark.network.crypto.enabled":                    "true",
			"spark.kubernetes.kerberos.tokenSecret.itemKey":   "test",
			"spark.dynamicAllocation.enabled":                 "true",
			"spark.kubernetes.driver.annotation.test":         "test-annotation",
			"spark.kubernetes.node.selector.test":             "test-node-selector",
			"spark.kubernetes.driver.node.selector.test":      "test-driver-node-selector",
			"spark.kubernetes.driver.secretKeyRef.testSecret": "test-secret",
		},
		Driver: v1beta2.DriverSpec{
			SparkPodSpec: v1beta2.SparkPodSpec{
				Cores: int32Pointer(1),
				ConfigMaps: []v1beta2.NamePath{
					{
						Name: "test-configmap",
						Path: "/etc/ccp/lldc/rsyslog",
					},
				},
				CoreLimit: stringPointer("1200m"),
				Env: []corev1.EnvVar{{
					Name:  "Dummy-env",
					Value: "dummy-env-val",
				},
				},
				EnvVars: map[string]string{
					"Name":  "Dummy-env",
					"Value": "dummy-env-val",
				},
				EnvSecretKeyRefs: map[string]v1beta2.NameKey{"test": {Key: "test", Name: "test"}},
				Memory:           stringPointer("512m"),
				Labels:           labelsForSpark(),
				ServiceAccount:   stringPointer("fit-driver-serviceaccount"),

				Annotations: map[string]string{
					"opencensus.k8s-integration.sfdc.com/inject":             "enabled",
					"opencensus.k8s-integration.sfdc.com/prometheus-targets": `[{"path": "/metrics","port": "8090","container_name": "spark-kubernetes-driver"}]`,
				},
				PodSecurityContext: &corev1.PodSecurityContext{
					RunAsUser:    int64Pointer(185),
					RunAsNonRoot: booleanPointer(true),
				},
				SecurityContext: &corev1.SecurityContext{
					RunAsUser:    int64Pointer(185),
					RunAsNonRoot: booleanPointer(true),
				},
				Secrets: []v1beta2.SecretInfo{
					{
						Name: "f8d3bdf3-a20b-448c-b2ae-bf14ba4bffc6",
						Path: "/etc/ccp-secrets",
						Type: "Generic",
					},
				},

				Sidecars: []corev1.Container{
					{
						Command: []string{
							"/usr/sbin/rsyslogd",
							"-n",
							"-f",
							"/etc/ccp/lldc/rsyslog/rsyslog.conf",
						},
						Env: []corev1.EnvVar{
							{
								Name:  "LLDC_BUS",
								Value: "einstein.test.streaming__aws.perf2-uswest2.einstein.ajnalocal1__strm.lldcbus",
							},
						},
						Image: "331455399823.dkr.ecr.us-east-2.amazonaws.com/sfci/monitoring/sfdc_rsyslog_gcp:latest",
						Name:  "ccp-lldc",
						Resources: corev1.ResourceRequirements{
							Limits: corev1.ResourceList{
								nativesubmit.Memory: memoryQuantity,
								nativesubmit.Cpu:    cpuQuantity,
							},
							Requests: corev1.ResourceList{
								nativesubmit.Memory: memoryQuantity,
								nativesubmit.Cpu:    cpuQuantity,
							},
						},
						VolumeMounts: []corev1.VolumeMount{
							{
								MountPath: "/etc/ccp/lldc/applogs",
								Name:      "ccp-lldc-applogs",
							},
							{
								MountPath: "/etc/ccp/lldc/statedir",
								Name:      "ccp-lldc-statedir",
							},
						},
					},
				},

				VolumeMounts: []corev1.VolumeMount{

					{
						MountPath: "/etc/ccp/lldc/applogs",
						Name:      "ccp-lldc-applogs",
					},
					{
						MountPath: "/etc/ccp/lldc/statedir",
						Name:      "ccp-lldc-statedir",
					},
					{
						MountPath: "/etc/test1",
						Name:      "spark-local-dir-11",
					},
					{
						MountPath: "/etc/test2",
						Name:      "spark-local-dir-22",
					},
				},
			},
			CoreRequest:        stringPointer("1000m"),
			ServiceAnnotations: map[string]string{"test": "test-annotation"},

			JavaOptions:      stringPointer("XX:+PrintGCDetails -XX:+PrintGCTimeStamps"),
			KubernetesMaster: stringPointer(" k8s://http://example.com:8080"),
		},
		Executor: v1beta2.ExecutorSpec{
			SparkPodSpec: v1beta2.SparkPodSpec{
				Annotations: map[string]string{
					"opencensus.k8s-integration.sfdc.com/inject":             "enabled",
					"opencensus.k8s-integration.sfdc.com/prometheus-targets": `[{"path": "/metrics","port": "8090","container_name": "spark-kubernetes-driver"}]`,
				},
				Cores:  int32Pointer(1),
				Labels: labelsForSpark(),
				Memory: stringPointer("512m"),
				PodSecurityContext: &corev1.PodSecurityContext{
					RunAsUser:    int64Pointer(185),
					RunAsNonRoot: booleanPointer(true),
				},
				Secrets: []v1beta2.SecretInfo{{Name: "test", Path: "\\spark\\bin", Type: "GCPServiceAccount"}, {Name: "test", Path: "\\spark\\bin", Type: "HadoopDelegationToken"}},
				VolumeMounts: []corev1.VolumeMount{
					{
						MountPath: "/etc/ccp/lldc/applogs",
						Name:      "ccp-lldc-applogs",
					},
					{
						MountPath: "/etc/ccp/lldc/statedir",
						Name:      "ccp-lldc-statedir",
					},
					{
						MountPath: "/etc/test1",
						Name:      "spark-local-dir-11",
					},
					{
						MountPath: "/etc/test2",
						Name:      "spark-local-dir-22",
					},
				},
			},
			Instances:   int32Pointer(1),
			JavaOptions: stringPointer("XX:+PrintGCDetails -XX:+PrintGCTimeStamps"),
		},
		Monitoring: &v1beta2.MonitoringSpec{
			ExposeDriverMetrics:   true,
			ExposeExecutorMetrics: true,
			MetricsPropertiesFile: stringPointer("/sample-apps/sample-basic-spark-operator/metrics.properties"),
			Prometheus: &v1beta2.PrometheusSpec{
				ConfigFile:     stringPointer("/sample-apps/sample-basic-spark-operator/jmx-agent-config.yaml"),
				JmxExporterJar: "/sample-apps/sample-basic-spark-operator/agents/prometheus-jmx-agent.jar",
				Port:           int32Pointer(8900),
			},
		},
		Volumes: []corev1.Volume{
			{
				Name: "ccp-lldc-applogs",
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{
						Medium: "Memory",
					},
				},
			},
			{
				Name: "ccp-lldc-statedir",
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{
						Medium: "Memory",
					},
				},
			},
			{
				Name: "spark-local-dir-11",
				VolumeSource: corev1.VolumeSource{
					HostPath: &corev1.HostPathVolumeSource{
						Path: "/home/test",
					},
				},
			},
			{
				Name: "spark-local-dir-22",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: "test",
					},
				},
			},
		},
	},
}

func TestCreateSparkAppConfigMap(t *testing.T) {

	type testcase struct {
		app                  *v1beta2.SparkApplication
		submissionID         string
		createdApplicationId string
		driverConfigMapName  string
	}

	// Create a fake client
	scheme := runtime.NewScheme()
	utilruntime.Must(corev1.AddToScheme(scheme))
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		Build()

	testFn := func(test testcase, t *testing.T) {
		err := nativesubmit.createSparkAppConfigMap(test.app, test.submissionID, test.createdApplicationId, fakeClient, test.driverConfigMapName, "test")
		if err != nil {
			t.Errorf("failed to create configmap: %v", err)
		}
		configMap := &apiv1.ConfigMap{}
		configMap.Name = "test-app-driver-configmap"
		configMap.Namespace = "default"
		err = fakeClient.Get(context.TODO(), ctrlClient.ObjectKeyFromObject(configMap), configMap)
		if err != nil && configMap.Data != nil {
			t.Errorf("failed to get ConfigMap %s: %v", "test-app-driver-configmap", err)
		}
	}
	testcases := []testcase{
		{
			app:                  TestApp,
			submissionID:         "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			createdApplicationId: "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			driverConfigMapName:  "test-app-driver-configmap",
		},
	}
	for _, test := range testcases {
		testFn(test, t)
	}

}

func TestBuildAltSubmissionCommandArgs(t *testing.T) {

	type testcase struct {
		app                  *v1beta2.SparkApplication
		driverPodName        string
		submissionID         string
		createdApplicationId string
	}
	testFn := func(test testcase, t *testing.T) {
		_, err := nativesubmit.buildAltSubmissionCommandArgs(test.app, test.driverPodName, test.submissionID, test.createdApplicationId, "testservicename")
		if err != nil {
			t.Errorf("failed to build Spark Application Submission Arguments: %v", err)
		}
	}
	testcases := []testcase{
		{
			app:                  TestApp,
			submissionID:         "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			createdApplicationId: "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			driverPodName:        "test-app-driver",
		},
		{
			app:                  TestApp,
			submissionID:         "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			createdApplicationId: "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			driverPodName:        "test-app-driver",
		},
		{
			app:                  TestApp,
			submissionID:         "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			createdApplicationId: "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			driverPodName:        "test-app-driver",
		},
		{
			app:                  TestApp,
			submissionID:         "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			createdApplicationId: "bJskVrN0XoSAdLypytgZ8WJNZwGJF9eO",
			driverPodName:        "test-app-driver",
		},
	}
	for index, test := range testcases {
		if index == 0 {
			test.app.Spec.Driver.Cores = nil
			test.app.Spec.Driver.CoreRequest = nil
			test.app.Spec.Driver.CoreLimit = nil
			test.app.Spec.Executor.CoreRequest = nil
			test.app.Spec.Executor.Cores = nil
			test.app.Spec.Executor.CoreLimit = nil
			test.app.Spec.Driver.Secrets = []v1beta2.SecretInfo{
				{
					Name: "f8d3bdf3-a20b-448c-b2ae-bf14ba4bffc6",
					Path: "/etc/ccp-secrets",
					Type: "GCPServiceAccount",
				},
				{Name: "test", Path: "etc/ccp-secrets", Type: "HadoopDelegationToken"},
			}

			test.app.Spec.Executor.Image = stringPointer("871501607754.dkr.ecr.us-west-2.amazonaws.com/sfci/dva-transformation/spark-on-k8s-sample-apps/flowsnake-basic-operator-integration-spark-3.3.2:jenkins-dva-transformation-spark-on-k8s-sample-apps-spark-3.3.2-sfdc-3-itest")
			test.app.Spec.Driver.EnvSecretKeyRefs = map[string]v1beta2.NameKey{"test": {Key: "test", Name: "test"}}
			test.app.Spec.Executor.EnvSecretKeyRefs = map[string]v1beta2.NameKey{"test": {Key: "test", Name: "test"}}

		}
		if index == 1 {
			test.app.Spec.Executor.CoreRequest = stringPointer("1")
		}
		if index == 2 {
			test.app.Spec.SparkConf["spark.kubernetes.executor.request.cores"] = "1200"
			test.app.Spec.SparkConf["spark.driver.cores"] = "1200"
			test.app.Spec.SparkConf["spark.kubernetes.driver.request.cores"] = "1200"
			test.app.Spec.SparkConf["spark.kubernetes.driver.limit.cores"] = "1200"
			test.app.Spec.SparkConf["spark.executor.cores"] = "1200"
			test.app.Spec.SparkConf["spark.executor.limit.cores"] = "1200"
			test.app.Spec.Executor.MemoryOverhead = stringPointer("0.2")
			test.app.Spec.Type = v1beta2.SparkApplicationTypePython
			test.app.Spec.Executor.ServiceAccount = stringPointer("fit-driver-serviceaccount")
			test.app.Spec.Executor.DeleteOnTermination = booleanPointer(true)
		}
		if index == 3 {
			test.app.Spec.Executor.Memory = nil
			test.app.Spec.Type = v1beta2.SparkApplicationTypeR
			test.app.Spec.Executor.EnvVars = map[string]string{
				"Name":  "Dummy-env",
				"Value": "dummy-env-val",
			}
		}
		testFn(test, t)
	}

}
func stringPointer(a string) *string {
	return &a
}

func int32Pointer(a int32) *int32 {
	return &a
}

func int64Pointer(a int64) *int64 {
	return &a
}

func booleanPointer(a bool) *bool {
	return &a
}
func labelsForSpark() map[string]string {
	return map[string]string{"version": "3.0.1"}
}
