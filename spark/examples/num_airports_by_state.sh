#!/bin/bash

if [ -f $(dirname $0)/../../setenv.sh ]; then
      source $(dirname $0)/../../setenv.sh
fi

$SPARK_HOME/bin/spark-submit \
  --master local[*] \
  --deploy-mode client \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=$EVENT_LOG_DIR \
  --name num_airports_by_state \
  --class com.example.demo.NumAirportsByState \
  $OS_BUCKET_NAME/lib/us-flights-app_2.12-0.1.jar \
  --input_file $OS_BUCKET_NAME/data/airports.csv \
  --output_file $OS_BUCKET_NAME/out/num_airports_by_state $@

