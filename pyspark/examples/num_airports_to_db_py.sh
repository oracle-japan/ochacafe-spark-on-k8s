#!/bin/bash

if [ -f $(dirname $0)/../../setenv.sh ]; then
      source $(dirname $0)/../../setenv.sh
fi

SOURCE_DIR=$(dirname $0)/../py

$SPARK_HOME/bin/spark-submit \
  --master local[*] \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:///tmp/spark-events \
  --conf spark.eventLog.rolling.enabled=true \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=$EVENT_LOG_DIR \
  --packages com.oracle.database.jdbc:ojdbc8:21.3.0.0 \
  $SOURCE_DIR/num_airports_to_db.py \
  --input_file $OS_BUCKET_NAME/data/airports.csv $@
