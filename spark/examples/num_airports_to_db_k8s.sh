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
    --conf spark.kubernetes.container.image=iad.ocir.io/ochacafens/ochacafe/spark:3.2.0 \
    --conf spark.kubernetes.container.image.pullSecrets=docker-registry-secret \
    --conf spark.kubernetes.container.image.pullPolicy=Always \
    --conf spark.kubernetes.driver.secretKeyRef.DB_USER=db-config-secret:user \
    --conf spark.kubernetes.driver.secretKeyRef.DB_PASSWORD=db-config-secret:password \
    --conf spark.kubernetes.driver.secretKeyRef.DB_JDBCURL=db-config-secret:jdbcUrl \
    --conf spark.kubernetes.report.interval=10s \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=$EVENT_LOG_DIR \
    --name num_airports_to_db \
    --class com.example.demo.NumAirportsToDb \
    $OS_BUCKET_NAME/lib/us-flights-app_2.12-0.1.jar \
    --input_file $OS_BUCKET_NAME/data/airports.csv $@



#    --jars $OS_BUCKET_NAME/lib/jars/ojdbc8-21.3.0.0.jar \
