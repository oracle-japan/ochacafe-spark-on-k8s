#!/bin/bash

K8S_API_SERVER=https://132.226.43.168:6443

$SPARK_HOME/bin/spark-submit --master k8s://$K8S_API_SERVER $@
