#!/bin/bash

if [ -f $(dirname $0)/../../setenv.sh ]; then
      source $(dirname $0)/../../setenv.sh
fi

$SPARK_HOME/bin/spark-submit \
    --master k8s://$K8S_API_SERVER \
    --deploy-mode cluster \
    --conf spark.executor.instances=2 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.container.image=iad.ocir.io/ochacafens/ochacafe/spark:3.2.1-vanilla \
    --conf spark.kubernetes.container.image.pullSecrets=docker-registry-secret \
    --conf spark.kubernetes.report.interval=10s \
    --class org.apache.spark.examples.SparkPi \
    --name spark-pi \
    local:///opt/spark/examples/jars/spark-examples_2.12-3.2.1.jar $@

#   --conf spark.eventLog.dir=$EVENT_LOG_DIR \
#   --conf spark.eventLog.rolling.enabled=true \
