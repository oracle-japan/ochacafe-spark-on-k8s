#!/bin/bash

if [ -f $(dirname $0)/../../setenv.sh ]; then
      source $(dirname $0)/../../setenv.sh
fi

$SPARK_HOME/bin/spark-submit \
      --master local[*] \
      --deploy-mode client \
      --conf spark.eventLog.enabled=true \
      --conf spark.eventLog.dir=$EVENT_LOG_DIR \
      --name spark-pi \
      $SPARK_HOME/examples/src/main/python/pi.py $@

