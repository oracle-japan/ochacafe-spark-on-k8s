#!/bin/bash

if [ -f $(dirname $0)/../../setenv.sh ]; then
      source $(dirname $0)/../../setenv.sh
fi

$SPARK_HOME/bin/spark-submit \
  --master local[*] \
  --deploy-mode client \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=$EVENT_LOG_DIR \
  --name avg_arrival_delay_by_airline \
  --class com.example.demo.AvgArrivalDelayByAirline \
  $OS_BUCKET_NAME/lib/scala-2.12/us-flights-app_2.12-0.1.jar \
  --input_dir $OS_BUCKET_NAME/data \
  --output_dir $OS_BUCKET_NAME/out $@
