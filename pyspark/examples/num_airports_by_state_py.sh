#!/bin/bash

if [ -f $(dirname $0)/../../setenv.sh ]; then
      source $(dirname $0)/../../setenv.sh
fi

SOURCE_DIR=$(dirname $0)/../py

$SPARK_HOME/bin/spark-submit \
  --master local[*] \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=$EVENT_LOG_DIR \
  $SOURCE_DIR/num_airports_by_state.py \
  --input_file $OS_BUCKET_NAME/data/airports.csv \
  --output_file $OS_BUCKET_NAME/out/num_airports_by_state $@
