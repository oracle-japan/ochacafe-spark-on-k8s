#!/bin/bash

DOCKER_REGISTRY=iad.ocir.io/ochacafe

SPARK_HOME=$(pwd)/spark-3.2.1-bin-hadoop3.2
spark_dl_url=https://dlcdn.apache.org/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz
hdfs_connector_dl_url=https://github.com/oracle/oci-hdfs-connector/releases/download/v3.3.1.0.3.2/oci-hdfs.zip
jdbc_jar_url=https://repo.maven.apache.org/maven2/com/oracle/database/jdbc/ojdbc8/21.3.0.0/ojdbc8-21.3.0.0.jar

# download spark
wget -q --no-check-certificate ${spark_dl_url}
mkdir $SPARK_HOME
tar xzvf *.tgz -C $SPARK_HOME --strip-components 1

# download oci hdfs connector
wget -q --no-check-certificate ${hdfs_connector_dl_url}
unzip *.zip -d oci-hdfs

cp oci-hdfs/lib/*.jar $SPARK_HOME/jars
mv $SPARK_HOME/jars/jsr305-3.0.0.jar $SPARK_HOME/jars/jsr305-3.0.0.jar.original
cp oci-hdfs/third-party/lib/*.jar $SPARK_HOME/jars

# download oracle jdbc driver
wget -P $SPARK_HOME/jars $jdbc_jar_url

# generate log4j.properties
cat $SPARK_HOME/conf/log4j.properties.template \
 | sed -e '$a\\nlog4j.logger.com.oracle.bmc=ERROR'\
 > $SPARK_HOME/conf/log4j.properties

# generate spark-defaults.conf
cat $SPARK_HOME/conf/spark-defaults.conf.template \
 | sed -e '$a\\nspark.sql.hive.metastore.sharedPrefixes=shaded.oracle,com.oracle.bmc' \
 | sed -e '$a\spark.hadoop.fs.oci.client.hostname=https://objectstorage.us-ashburn-1.oraclecloud.com' \
 | sed -e '$a\spark.hadoop.fs.oci.client.custom.authenticator=com.oracle.bmc.hdfs.auth.InstancePrincipalsCustomAuthenticator' \
 > $SPARK_HOME/conf/spark-defaults.conf

# create k8s spark image
$SPARK_HOME/bin/docker-image-tool.sh -r $DOCKER_REGISTRY -t 3.2.1-hadoop3.2 \
  -f $SPARK_HOME/kubernetes/dockerfiles/spark/Dockerfile build

# create k8s pyspark image
$SPARK_HOME/bin/docker-image-tool.sh -r $DOCKER_REGISTRY -t 3.2.1-hadoop3.2 \
  -p $SPARK_HOME/kubernetes/dockerfiles/spark/bindings/python/Dockerfile build

# push images to registry
#docker push $DOCKER_REGISTRY/spark:3.2.1-hadoop3.2
#docker push $DOCKER_REGISTRY/spark-py:3.2.1-hadoop3.2


# rm *.zip *.tgz
