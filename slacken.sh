#!/bin/bash

#SPARK_MASTER=spark://localhost:7077
SPARK_MASTER=${SPARK_MASTER:-local[*]}

#SPARK_HOME=/local/spark-3.5.3-bin-hadoop3/
if [ -z "$SPARK_HOME" ]
then
  echo Please set SPARK_HOME to the location of your Spark installation.
  exit 1
fi

#SLACKEN_TMP=/tmp
if [ -z "$SLACKEN_TMP" ]
then
  echo Please set SLACKEN_TMP to a scratch space location.
  exit 1
fi

#Try to find the directory that this file is located in
SLACKEN_HOME=${SLACKEN_HOME:-$(dirname -- "$(readlink "${BASH_SOURCE}")")}

#For standalone mode (one process), it is helpful to provide as much memory as possible.
#This sets the default value to 16g if the variable is unassigned.
SLACKEN_MEMORY=${SLACKEN_MEMORY:-16g}
MEMORY="spark.driver.memory=$SLACKEN_MEMORY"

#Scratch space location. This has a big effect on performance; should ideally be a fast SSD or similar.
LOCAL_DIR="spark.local.dir=$SLACKEN_TMP"

#On Windows: Change bin/spark-submit to bin/spark-submit.cmd.

#Make sure that generated files are group-writable when we are running from Docker
UMASK="spark.hadoop.fs.permissions.umask-mode=002"

exec $SPARK_HOME/bin/spark-submit \
  --conf spark.driver.maxResultSize=2g \
  --driver-java-options -Dlog4j.configuration="file:$SLACKEN_HOME/log4j.properties" \
  --conf $MEMORY \
  --conf $LOCAL_DIR \
  --conf $UMASK \
  --master $SPARK_MASTER \
  --class com.jnpersson.slacken.Slacken $SLACKEN_HOME/target/scala-2.12/Slacken-assembly-2.0.0.jar $*
