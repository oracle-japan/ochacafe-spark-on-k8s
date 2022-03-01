#!/bin/bash

DOCKER_REGISTRY=iad.ocir.io/ochacafe

SPARK_HOME=$(pwd)/spark-3.2.1-bin-hadoop3.2
spark_dl_url=https://dlcdn.apache.org/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz
hdfs_connector_dl_url=https://github.com/oracle/oci-hdfs-connector/releases/download/v3.2.1.3/oci-hdfs.zip
jdbc_jar_url=https://repo.maven.apache.org/maven2/com/oracle/database/jdbc/ojdbc8/21.3.0.0/ojdbc8-21.3.0.0.jar

# download spark
wget -q --no-check-certificate ${spark_dl_url}
mkdir $SPARK_HOME
tar xzvf *.tgz -C $SPARK_HOME --strip-components 1

# download oci hdfs connector
wget -q --no-check-certificate ${hdfs_connector_dl_url}
unzip *.zip -d oci-hdfs

cp oci-hdfs/lib/*.jar $SPARK_HOME/jars
cp oci-hdfs/third-party/lib/*.jar $SPARK_HOME/jars

# download oracle jdbc driver
wget -P $SPARK_HOME/jars $jdbc_jar_url

# download spark streaming jars
cat << EOF > pom.xml
<?xmY version="1.0" encoding="UTF-8" ?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>org.example</groupId>
    <artifactId>spark-streaming</artifactId>
    <version>0.1</version>
    <dependencies>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql-kafka-0-10_2.12</artifactId>
            <version>3.2.1</version>
        </dependency>
    </dependencies>
</project>
EOF
echo pom.xml
mvn dependency:copy-dependencies -DoutputDirectory=$SPARK_HOME/jars

# resolve duplication
mv $SPARK_HOME/jars/jsr305-3.0.0.jar $SPARK_HOME/jars/jsr305-3.0.0.jar.original

# generate log4j.properties
cat $SPARK_HOME/conf/log4j.properties.template \
 | sed -e '$a\\nlog4j.logger.com.oracle.bmc=ERROR'\
 > $SPARK_HOME/conf/log4j.properties

cat $SPARK_HOME/conf/log4j.properties \
 | sed -e s/log4j.rootCategory=INFO/log4j.rootCategory=WARN/ \
 > $SPARK_HOME/conf/log4j.properties.warn

# generate spark-defaults.conf
cat $SPARK_HOME/conf/spark-defaults.conf.template \
 | sed -e '$a\\nspark.sql.hive.metastore.sharedPrefixes=shaded.oracle,com.oracle.bmc' \
 | sed -e '$a\spark.hadoop.fs.oci.client.hostname=https://objectstorage.us-ashburn-1.oraclecloud.com' \
 | sed -e '$a\spark.hadoop.fs.oci.client.custom.authenticator=com.oracle.bmc.hdfs.auth.InstancePrincipalsCustomAuthenticator' \
 > $SPARK_HOME/conf/spark-defaults.conf

# generate metrics.properties
cat << EOF > $SPARK_HOME/conf/metrics.properties
*.sink.prometheusServlet.class=org.apache.spark.metrics.sink.PrometheusServlet
*.sink.prometheusServlet.path=/metrics/prometheus
master.sink.prometheusServlet.path=/metrics/master/prometheus
applications.sink.prometheusServlet.path=/metrics/applications/prometheus
EOF

# create k8s spark image
$SPARK_HOME/bin/docker-image-tool.sh -r $DOCKER_REGISTRY -t 3.2.1-hadoop3.2-full \
  -f $SPARK_HOME/kubernetes/dockerfiles/spark/Dockerfile build

# create k8s pyspark image
$SPARK_HOME/bin/docker-image-tool.sh -r $DOCKER_REGISTRY -t 3.2.1-hadoop3.2-full \
  -p $SPARK_HOME/kubernetes/dockerfiles/spark/bindings/python/Dockerfile build

# push images to registry
#docker push $DOCKER_REGISTRY/spark:3.2.1-hadoop3.2-full
#docker push $DOCKER_REGISTRY/spark-py:3.2.1-hadoop3.2-full


# rm *.zip *.tgz *.xml
