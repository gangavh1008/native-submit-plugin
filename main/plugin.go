package main

import (
	"fmt"
	"github.com/kubeflow/spark-operator/api/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type NativeSubmit struct{}

func (a *NativeSubmit) LaunchSparkApplication(app *v1beta2.SparkApplication, cl client.Client) error {
	fmt.Println("Launching spark application")
	runAltSparkSubmitWrapper(app, cl)
	return nil
}

func New() v1beta2.SparkAppLauncher {
	return &NativeSubmit{}
}
