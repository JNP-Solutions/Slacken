#!/bin/bash

#CAMI2 read mapping to Kraken report conversion tool. See the CAMIToKrakenReport source code for comments.

MASTER=local[*]
SPARK=/ext/src/spark-3.5.1-bin-hadoop3

SLACKEN_HOME=/home/johan/Slacken

#For standalone mode (one process), it is helpful to provide as much memory as possible.
MEMORY=spark.driver.memory=32g

#Scratch space location. This has a big effect on performance; should ideally be a fast SSD or similar.
LOCAL_DIR="spark.local.dir=/fast/spark"

exec $SPARK/bin/spark-submit \
  --conf spark.driver.maxResultSize=4g \
  --driver-java-options -Dlog4j.configuration="file:$SLACKEN_HOME/log4j.properties" \
  --conf $MEMORY \
  --conf $LOCAL_DIR \
  --master $MASTER \
  --class com.jnpersson.slacken.analysis.CAMIToKrakenReport $SLACKEN_HOME/target/scala-2.12/Slacken-assembly-0.1.0.jar $*
