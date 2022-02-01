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
    --conf spark.kubernetes.container.image=iad.ocir.io/ochacafens/ochacafe/spark:3.2.1 \
    --conf spark.kubernetes.container.image.pullSecrets=docker-registry-secret \
    --conf spark.kubernetes.container.image.pullPolicy=Always \
    --conf spark.kubernetes.report.interval=10s \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=$EVENT_LOG_DIR \
    --name num_airports_by_state \
    --class com.example.demo.NumAirportsByState \
    $OS_BUCKET_NAME/lib/us-flights-app_2.12-0.1.jar \
    --input_file $OS_BUCKET_NAME/data/airports.csv \
    --output_file $OS_BUCKET_NAME/out/num_airports_by_state $@
