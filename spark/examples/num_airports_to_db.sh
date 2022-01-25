#!/bin/bash

if [ -f $(dirname $0)/../../setenv.sh ]; then
      source $(dirname $0)/../../setenv.sh
fi

$SPARK_HOME/bin/spark-submit \
  --master local[*] \
  --deploy-mode client \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=$EVENT_LOG_DIR \
  --name num_airports_to_db \
  --packages com.oracle.database.jdbc:ojdbc8:21.3.0.0 \
  --class com.example.demo.NumAirportsToDb \
  $OS_BUCKET_NAME/lib/us-flights-app_2.12-0.1.jar \
  --input_file $OS_BUCKET_NAME/data/airports.csv $@

