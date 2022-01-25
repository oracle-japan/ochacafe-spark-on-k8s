#!/bin/bash

if [ -f $(dirname $0)/../../setenv.sh ]; then
      source $(dirname $0)/../../setenv.sh
fi

$SPARK_HOME/bin/spark-submit \
      --master k8s://$K8S_API_SERVER \
      --deploy-mode cluster \
      --conf spark.executor.instances=2 \
      --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
      --conf spark.kubernetes.container.image=iad.ocir.io/ochacafens/ochacafe/spark-py:3.2.0 \
      --conf spark.kubernetes.container.image.pullSecrets=docker-registry-secret \
      --name spark-pi \
      local:///opt/spark/examples/src/main/python/pi.py $@

#     --conf spark.eventLog.enabled=true \
#     --conf spark.eventLog.dir=$EVENT_LOG_DIR \      