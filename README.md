# README

This plugin provides a native alternative to `spark-submit` for launching Spark applications via the Spark Operator in a Kubernetes cluster. By bypassing the default mechanism of using `spark-submit` command to launch Spark applications, users can avoid the JVM spin-up overhead associated with spark-submit.

## Build
Run the following command from the root directory of the project

`go build -buildmode=plugin -o plugin.so ./main`
