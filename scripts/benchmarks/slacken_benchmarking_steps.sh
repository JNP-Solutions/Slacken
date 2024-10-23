#!/bin/bash

#Slacken benchmarking pipeline. Currently runs on AWS.

#Regular 1-step classify
function classify {
  LIB=$1
  LNAME=$2

  CLASS_OUT=$ROOT/scratch/classified/$FAMILY/$LNAME
  ./slacken2-aws.sh taxonIndex $DATA/$LIB classify \
    --sample-regex "(S[0-9]+)" -p -c $"${CS[@]}" -o $CLASS_OUT \
  "${SAMPLES[@]}"
}

#Classify with "gold set" dynamic library.
#Enabled by -d
function classifyGS {
  LIB=$1
  LNAME=$2
  #--report-dynamic-index
  #-p 3000
  CLASS_OUT=$ROOT/scratch/classified/$FAMILY/$LNAME
  ./slacken2-aws.sh -p 3000 taxonIndex $DATA/$LIB classify --classify-with-gold-standard -g $SPATH/${LABEL}_gold.txt \
      --dynamic-bracken-length 150 \
     -d $K2 --sample-regex "(S[0-9]+)" -p -c $"${CS[@]}" -o $CLASS_OUT \
  "${SAMPLES[@]}"
}

#2-step classify with dynamic library.
#Enabled by -d
function classifyDynamic {
  LIB=$1
  LNAME=$2
  RVALUE=$3

  CLASS_OUT=$ROOT/scratch/classified/$FAMILY/$LNAME
  ./slacken2-aws.sh -p 10000 taxonIndex $DATA/$LIB classify -g $SPATH/${LABEL}_gold.txt \
    --dynamic-bracken-length 150 \
    --dynamic-min-reads $RVALUE -d $K2 --sample-regex "(S[0-9]+)" -p -c $"${CS[@]}" -o $CLASS_OUT \
  "${SAMPLES[@]}"
}

#Compare classifications of a single sample against a reference
function compare {
  SAMPLE=$1
  LIB=$2

  CLASSIFICATIONS=""
  for C in "${CS[@]}"
  do
    CLASSIFICATIONS="$CLASSIFICATIONS $ROOT/scratch/classified/$FAMILY/${LIB}_c${C}_classified/sample=S$SAMPLE"
  done

  REF=$SPATH/sample$SAMPLE/reads_mapping.tsv
  ./slacken2-aws.sh -t $TAXONOMY compare -r $REF -i 1 -T 3 -h \
    -o $ROOT/scratch/classified/$FAMILY/$LIB/sample$SAMPLE $CLASSIFICATIONS
}


