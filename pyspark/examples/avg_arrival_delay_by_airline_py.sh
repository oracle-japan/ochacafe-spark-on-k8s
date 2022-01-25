#!/bin/bash

if [ -f $(dirname $0)/../../setenv.sh ]; then
      source $(dirname $0)/../../setenv.sh
fi

SOURECE_DIR=$(dirname $0)/../py

$SPARK_HOME/bin/spark-submit \
  --master local[*] \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:///tmp/spark-events \
  --conf spark.eventLog.rolling.enabled=true \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=$EVENT_LOG_DIR \
  $SOURECE_DIR/avg_arrival_delay_by_airline.py \
  --input_dir $OS_BUCKET_NAME/data \
  --output_dir $OS_BUCKET_NAME/out $@
