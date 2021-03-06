#!/bin/bash

if [ -f $(dirname $0)/../../setenv.sh ]; then
      source $(dirname $0)/../../setenv.sh
fi

$SPARK_HOME/bin/spark-submit \
    --master k8s://$K8S_API_SERVER \
    --deploy-mode cluster \
    --conf spark.driver.cores=2 \
    --conf spark.driver.memory=2g \
    --conf spark.executor.instances=3 \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=2g \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.container.image=iad.ocir.io/ochacafens/ochacafe/spark-py:3.2.1 \
    --conf spark.kubernetes.container.image.pullSecrets=docker-registry-secret \
    --conf spark.kubernetes.container.image.pullPolicy=Always \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=$EVENT_LOG_DIR \
    --conf spark.kubernetes.node.selector.name=pool2 \
    --conf spark.kubernetes.report.interval=10s \
    --name avg_arrival_delay_by_airline_py \
    $OS_BUCKET_NAME/lib/avg_arrival_delay_by_airline.py \
    --input_dir $OS_BUCKET_NAME/data \
    --output_dir $OS_BUCKET_NAME/out $@
