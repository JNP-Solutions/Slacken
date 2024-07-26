#!/bin/bash

MASTER=local[*]
SPARK=/ext/src/spark-3.5.1-bin-hadoop3

#Max size of input splits in bytes. A smaller number reduces memory usage but increases the number of 
#partitions for the first stage. If this variable is unset, Spark's default of 128 MB will be used.
#SPLIT="spark.hadoop.mapreduce.input.fileinputformat.split.maxsize=$((1 * 1024 * 1024))"

#--conf $SPLIT

DISCOUNT_HOME=/home/johan/ws/jnps/Hypercut-git

#For standalone mode (one process), it is helpful to provide as much memory as possible.
MEMORY=spark.driver.memory=32g

#Scratch space location. This has a big effect on performance; should ideally be a fast SSD or similar.
LOCAL_DIR="spark.local.dir=/fast/spark"

#On Windows: Change bin/spark-submit to bin/spark-submit.cmd.

exec $SPARK/bin/spark-submit \
  --conf spark.driver.maxResultSize=4g \
  --driver-java-options -Dlog4j.configuration="file:$DISCOUNT_HOME/log4j.properties" \
  --conf $MEMORY \
  --conf $LOCAL_DIR \
  --master $MASTER \
  --class com.jnpersson.slacken.analysis.CAMIToKrakenReport $DISCOUNT_HOME/target/scala-2.12/Slacken-assembly-0.1.0.jar $*
