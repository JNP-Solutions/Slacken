#!/bin/bash
#Script to submit as an Amazon AWS EMR step. Copy this file to aws-discount.sh and 
#edit variables accordingly.

#For this script to work, it is necessary to install and configure the AWS CLI.


if [ -z "${AWS_EMR_CLUSTER}" ]
then
  echo "Please set AWS_EMR_CLUSTER"
  exit 1
fi

#Bucket to store discount jars and data files
BUCKET=s3://jnp-bio/discount

DISCOUNT_HOME="$(dirname -- "$(readlink "${BASH_SOURCE}")")"

aws s3 cp "$DISCOUNT_HOME/target/scala-2.12/Hypercut-assembly-0.1.0.jar" $BUCKET/
#aws s3 sync "$DISCOUNT_HOME/resources/PASHA" $BUCKET/PASHA/

#Max size of input splits in bytes. A smaller number reduces memory usage but increases the number of 
#partitions for the first stage. If this variable is unset, Spark's default of 128 MB will be used.
SPLIT="spark.hadoop.mapreduce.input.fileinputformat.split.maxsize=$((64 * 1024 * 1024))"
MEM="spark.driver.memory=16G"

#To set SPLIT or other variables, uncomment below.
COMMAND=( \
#  --conf $SPLIT \
  --conf $MEM \
  --class com.jnpersson.hypercut.Hypercut $BUCKET/Hypercut-assembly-0.1.0.jar $*)

#Turn off paging for output
export AWS_PAGER=""

RUNNER_ARGS="spark-submit"
for PARAM in ${COMMAND[@]}
do
  RUNNER_ARGS="$RUNNER_ARGS,$PARAM"
done

aws emr add-steps --cluster $AWS_EMR_CLUSTER --steps Type=CUSTOM_JAR,Name=Discount,ActionOnFailure=CONTINUE,Jar=command-runner.jar,Args=\[$RUNNER_ARGS\]
