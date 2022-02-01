#!/bin/bash

if [ -f $(dirname $0)/../../setenv.sh ]; then
      source $(dirname $0)/../../setenv.sh
fi

$SPARK_HOME/bin/spark-submit \
      --master local[*] \
      --deploy-mode client \
      --class org.apache.spark.examples.SparkPi \
      --name spark-pi \
      $SPARK_HOME/examples/jars/spark-examples_2.12-3.2.1.jar

