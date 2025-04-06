package main

import (
	"github.com/kubeflow/spark-operator/api/v1beta2"
)

var getServiceNameFunctionTestData1 = &v1beta2.SparkApplication{
	Spec: v1beta2.SparkApplicationSpec{
		Type: v1beta2.SparkApplicationTypeScala,
		Driver: v1beta2.DriverSpec{
			PodName: StringPointer("abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"),
		},
	},
}

func StringPointer(a string) *string {
	return &a
}

var getServiceNameFunctionTestData2 = &v1beta2.SparkApplication{
	Spec: v1beta2.SparkApplicationSpec{
		Type: v1beta2.SparkApplicationTypeScala,
		Driver: v1beta2.DriverSpec{
			PodName: StringPointer("abcdefghijk"),
		},
	},
}
