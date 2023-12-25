#!/bin/bash
#Script to submit as an AWS EMR Serverless job run.

#For this script to work, it is necessary to install and configure the AWS CLI.
#In AWS EMR Studio, an application should be configured, with the appropriate execution role.
#Then the application ID and role can be copied to here.

#Tokyo
#APPLICATION_ID=00f7eiiu6qg5p12h
#US-east-1
APPLICATION_ID=00f7jce5kld68609
ROLE_IAM=arn:aws:iam::362032979160:role/EMR_jnp-bio

REGION=us-east-1

#Bucket to store discount jars and data files
BUCKET=s3://jnp-bio/discount

DISCOUNT_HOME="$(dirname -- "$(readlink "${BASH_SOURCE}")")"

#aws s3 cp "$DISCOUNT_HOME/target/scala-2.12/Hypercut-assembly-0.1.0.jar" $BUCKET/
#aws s3 sync "$DISCOUNT_HOME/resources/PASHA" $BUCKET/PASHA/

#Max size of input splits in bytes. A smaller number reduces memory usage but increases the number of 
#partitions for the first stage. If this variable is unset, Spark's default of 128 MB will be used.
#SPLIT="spark.hadoop.mapreduce.input.fileinputformat.split.maxsize=$((64 * 1024 * 1024))"

#To set SPLIT or other variables, uncomment below.
COMMAND=( \
#  --conf $SPLIT \
  --class com.jnpersson.hypercut.Hypercut $BUCKET/Hypercut-assembly-0.1.0.jar $*)

#Turn off paging for output
export AWS_PAGER=""

ENTRY_ARGS="\"$1\""
shift
for PARAM in $*
do
  ENTRY_ARGS="$ENTRY_ARGS,\"$PARAM\""
done

#Disk and memory for each executor. By default, an executor has 4 vCPUs.
DISK="--conf spark.emr-serverless.executor.disk=40G"
#Default 14G, sometimes needs 22G
MEMORY="--conf spark.executor.memory=14G"
CORES="--conf spark.executor.cores=4"

JOB=$(cat <<EOF
{ "sparkSubmit":{
     "entryPoint":"$BUCKET/Hypercut-assembly-0.1.0.jar",
      "entryPointArguments":[$ENTRY_ARGS],
      "sparkSubmitParameters":"--class com.jnpersson.hypercut.Hypercut $MEMORY $DISK $CORES"
  }
}
EOF
)

aws emr-serverless start-job-run \
  --region $REGION \
  --name Hypercut \
  --application-id $APPLICATION_ID \
  --execution-role-arn $ROLE_IAM \
  --job-driver "$JOB"
