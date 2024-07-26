#!/bin/bash

#e.g. strain
FAMILY=$1
shift
LNAME=$1
shift
CS="0.00 0.05 0.10 0.15"
DEST=${FAMILY}_v2

BUCKET=s3://onr-emr
ROOT=$BUCKET/scratch/classified/cami2
SAMPLES=10

aws --profile sbi s3 sync --exclude "*" --include "*kreport.txt" --include "*metrics.tsv" $ROOT/$FAMILY/$LNAME $DEST/$LNAME

#Sync files and run bracken.
#We only sync metrics and kreports (summary information). Individual sample classifications are too large and costly to download.
for f in $CS
do
  DIR=${LNAME}_c${f}_classified
  aws --profile sbi s3 sync --exclude "*" --include "*kreport.txt" --include "*metrics.tsv" $ROOT/$FAMILY/$DIR $DEST/$DIR
  for ((i = 0; i < $SAMPLES; i++)) 
  do
    bracken -d $LNAME -i $DEST/$DIR/S${i}_kreport.txt -r 150 -o $DEST/$DIR/S${i}_bracken > /dev/null
  done
done

#Sync reference kreport
aws --profile sbi s3 sync $BUCKET/cami2/$FAMILY/mapping $FAMILY/mapping

#Compare bracken against reference for each sample
for ((i = 0; i < $SAMPLES; i++))
do
  FILES=""
  for f in $CS
  do
    FILES="$FILES $DEST/${LNAME}_c${f}_classified/S${i}_bracken"
  done

  scala compare_bracken_kraken.scala $FAMILY/mapping/sample${i}_kreport.txt $FILES > $DEST/$LNAME/S${i}_bmetrics.tsv
done

