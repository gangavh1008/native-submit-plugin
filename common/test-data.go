package common

import (
	"github.com/kubeflow/spark-operator/api/v1beta2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

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
		DynamicAllocation: &v1beta2.DynamicAllocation{Enabled: true, InitialExecutors: Int32Pointer(1), MinExecutors: Int32Pointer(1), MaxExecutors: Int32Pointer(1), ShuffleTrackingTimeout: Int64Pointer(1)},
		HadoopConf:        map[string]string{"abc.def": "xyz", "jkl.ghi": "uvw"},
		Type:              v1beta2.SparkApplicationTypeScala,
		Mode:              v1beta2.DeployModeCluster,
		Image:             StringPointer("871501607754.dkr.ecr.us-west-2.amazonaws.com/sfci/dva-transformation/spark-on-k8s-sample-apps/flowsnake-basic-operator-integration-spark-3.3.2:jenkins-dva-transformation-spark-on-k8s-sample-apps-spark-3.3.2-sfdc-3-itest"),
		ImagePullPolicy:   StringPointer("IfNotPresent"),
		ImagePullSecrets: []string{
			"dummy-data",
		},
		MainClass:           StringPointer("org.apache.spark.examples.SparkPi"),
		MainApplicationFile: StringPointer("local:///sample-apps/sample-basic-spark-operator/sample-basic-spark-operator.jar"),
		PythonVersion:       StringPointer("/usr/local/bin/python3"),
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
				Cores: Int32Pointer(1),
				ConfigMaps: []v1beta2.NamePath{
					{
						Name: "test-configmap",
						Path: "/etc/ccp/lldc/rsyslog",
					},
				},
				CoreLimit: StringPointer("1200m"),
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
				Memory:           StringPointer("512m"),
				Labels:           LabelsForSpark(),
				ServiceAccount:   StringPointer("fit-driver-serviceaccount"),

				Annotations: map[string]string{
					"opencensus.k8s-integration.sfdc.com/inject":             "enabled",
					"opencensus.k8s-integration.sfdc.com/prometheus-targets": `[{"path": "/metrics","port": "8090","container_name": "spark-kubernetes-driver"}]`,
				},
				PodSecurityContext: &corev1.PodSecurityContext{
					RunAsUser:    Int64Pointer(185),
					RunAsNonRoot: BoolPointer(true),
				},
				SecurityContext: &corev1.SecurityContext{
					RunAsUser:    Int64Pointer(185),
					RunAsNonRoot: BoolPointer(true),
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
								corev1.ResourceMemory: resource.MustParse("512Mi"),
								corev1.ResourceCPU:    resource.MustParse("500m"),
							},
							Requests: corev1.ResourceList{
								corev1.ResourceMemory: resource.MustParse("512Mi"),
								corev1.ResourceCPU:    resource.MustParse("500m"),
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
			CoreRequest:        StringPointer("1000m"),
			ServiceAnnotations: map[string]string{"test": "test-annotation"},

			JavaOptions:      StringPointer("XX:+PrintGCDetails -XX:+PrintGCTimeStamps"),
			KubernetesMaster: StringPointer(" k8s://http://example.com:8080"),
		},
		Executor: v1beta2.ExecutorSpec{
			SparkPodSpec: v1beta2.SparkPodSpec{
				Annotations: map[string]string{
					"opencensus.k8s-integration.sfdc.com/inject":             "enabled",
					"opencensus.k8s-integration.sfdc.com/prometheus-targets": `[{"path": "/metrics","port": "8090","container_name": "spark-kubernetes-driver"}]`,
				},
				Cores:  Int32Pointer(1),
				Labels: LabelsForSpark(),
				Memory: StringPointer("512m"),
				PodSecurityContext: &corev1.PodSecurityContext{
					RunAsUser:    Int64Pointer(185),
					RunAsNonRoot: BoolPointer(true),
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
			Instances:   Int32Pointer(1),
			JavaOptions: StringPointer("XX:+PrintGCDetails -XX:+PrintGCTimeStamps"),
		},
		Monitoring: &v1beta2.MonitoringSpec{
			ExposeDriverMetrics:   true,
			ExposeExecutorMetrics: true,
			MetricsPropertiesFile: StringPointer("/sample-apps/sample-basic-spark-operator/metrics.properties"),
			Prometheus: &v1beta2.PrometheusSpec{
				ConfigFile:     StringPointer("/sample-apps/sample-basic-spark-operator/jmx-agent-config.yaml"),
				JmxExporterJar: "/sample-apps/sample-basic-spark-operator/agents/prometheus-jmx-agent.jar",
				Port:           Int32Pointer(8900),
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

type Testcase struct {
	App *v1beta2.SparkApplication
}

var TestCasesList = []Testcase{
	{
		App: TestApp,
	},
	{
		App: TestApp,
	},
	{
		App: TestApp,
	},
	{
		App: TestApp,
	},
	{
		App: TestApp,
	},
}
