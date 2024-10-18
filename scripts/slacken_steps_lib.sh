#!/bin/bash

#Slacken evaluation pipeline. Currently runs on AWS.
#Supports three main functions:
# 1) build libraries, 2) classify samples, 3) compare classified output with reference.
#Work in progress.

#Main bucket
ROOT=s3://onr-emr

#data directory where objects are deleted after a few days
#DATA=$ROOT/scratch
#Directory for permanently kept data
DATA=$ROOT/keep

#Standard library
#K2=$ROOT/kraken2
#K2=$ROOT/standard-224c
#Refseq
K2=$ROOT/refseq-224pc

TAXONOMY=$K2/taxonomy
#TAXONOMY=$ROOT/k2-nt/taxonomy

#Regular 1-step classify
function classify {
  LIB=$1
  LNAME=$2
  CLASS_OUT=$ROOT/scratch/classified/$FAMILY/$LNAME
  ./slacken-aws.sh taxonIndex $DATA/$LIB classify \
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
  ./slacken-aws.sh -p 3000 taxonIndex $DATA/$LIB classify --classify-with-gold-standard -g $SPATH/${LABEL}_gold.txt \
      --dynamic-bracken-length 150 \
     -d $K2 --sample-regex "(S[0-9]+)" -p -c $"${CS[@]}" -o $CLASS_OUT \
  "${SAMPLES[@]}"
}

#2-step classify with dynamic library.
#Enabled by -d
function classifyDynamic {
  LIB=$1
  LNAME=$2
  #--classify-with-gold-standard
  #--report-dynamic-index
  #--dynamic-min-count
  #--dynamic-min-fraction
  #--report-dynamic-index

  CLASS_OUT=$ROOT/scratch/classified/$FAMILY/$LNAME
  ./slacken-aws.sh -p 3000 taxonIndex $DATA/$LIB classify -g $SPATH/${LABEL}_gold.txt \
    --dynamic-bracken-length 150 \
    --dynamic-min-fraction 1e-5 -d $K2 --sample-regex "(S[0-9]+)" -p -c $"${CS[@]}" -o $CLASS_OUT \
  "${SAMPLES[@]}"
}

function build {
  PREFIX=$1
  K=$2
  M=$3
  S=$4
  NAME=${PREFIX}_${K}_${M}_s${S}
  BUCKETS=$5
  OTHER=$6

  PARAMS="-k $K -m $M --spaces $S -p $BUCKETS"
  ./slacken-aws.sh $PARAMS -t $TAXONOMY taxonIndex $DATA/$NAME build -l $K2 $OTHER
  histogram $NAME
}

function histogram {
  LIB=$1
  ./slacken-aws.sh taxonIndex $DATA/$LIB histogram
}

function report {
  LIB=$1
  #-l $K2
  ./slacken-aws.sh taxonIndex $DATA/$LIB report -o $DATA/$LIB
}

function brackenWeights {
  LIB=$1
  READ_LENGTH=150
  #Note the special run script that sets $SPLIT properly for this job
  ./slacken-aws.sh -p 10000 taxonIndex $DATA/$LIB brackenWeights -l $K2 -r $READ_LENGTH
}

#Compare classifications of multiple samples and classifications against references
function compare {
  LIB=$1

  #Directories expected to contain multi-sample classifications
  CLASSIFICATIONS=""
  for C in "${CS[@]}"
  do
    CLASSIFICATIONS="$CLASSIFICATIONS $ROOT/scratch/classified/$FAMILY/${LIB}_c${C}_classified"
  done

  #Directory expected to contain reads_mapping.tsv reference files for each sample
  REF=$SPATH
  ./slacken-aws.sh -t $TAXONOMY compare -r $REF -i 1 -T 3 -h \
    -o $ROOT/scratch/classified/$FAMILY/$LIB/samples --multi-dirs $CLASSIFICATIONS
}
