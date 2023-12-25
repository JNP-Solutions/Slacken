#!/bin/bash
CLUSTER=$AWS_EMR_CLUSTER

#turn off paging
export AWS_PAGER=""
aws emr list-steps --cluster-id $CLUSTER --step-states PENDING | jq '.["Steps"] | .[] | .["Id"]' |while read id
do
aws emr cancel-steps --step-ids $(echo $id | sed s/\"//g)  --cluster-id $CLUSTER
done


