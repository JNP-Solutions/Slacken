#!/bin/bash

ARGS=$1
INPUT=$2
EXPECTED=$3
WORKDIR=/fast/scratch/spark/hypercut_check

echo Testing $INPUT

RESULT=$WORKDIR/$(basename $INPUT)
time ./spark-submit.sh $ARGS --minimizers minimizers -k 31 $INPUT stats -o $RESULT
egrep "Distinct|Unique|Total" ${RESULT}_stats.txt > ${RESULT}_validate

diff -w ${RESULT}_validate $EXPECTED || exit 1

echo Results for $INPUT are correct
