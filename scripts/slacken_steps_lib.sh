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
  #--classify-with-gold-standard
  #--report-dynamic-index

  CLASS_OUT=$ROOT/scratch/classified/$FAMILY/$LNAME
  ./slacken2-aws.sh -p 3000 taxonIndex $DATA/$LIB classify -g $SPATH/${LABEL}_gold.txt \
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
  ./slacken2-aws.sh $PARAMS -t $TAXONOMY taxonIndex $DATA/$NAME build -l $K2 $OTHER
  histogram $NAME
}

function respace {
  LIB=$1
  SPACES=$2
  #The output path will be renamed automatically as long as the _s naming convention is followed
  ./slacken2-aws.sh taxonIndex $DATA/$LIB respace -s $SPACES -o $DATA/$LIB
}

function histogram {
  LIB=$1
  ./slacken2-aws.sh taxonIndex $DATA/$LIB histogram
}

function report {
  LIB=$1
  K=$2
  #-l $K2/seqid2taxid.map
  ./slacken2-aws.sh -k $K taxonIndex $DATA/$LIB report -l $K2 -o $DATA/$LIB
}

function brackenWeights {
  LIB=$1
  READ_LENGTH=150
  #Note the special run script that sets $SPLIT properly for this job
  ./slacken2-aws.sh -p 10000 taxonIndex $DATA/$LIB brackenWeights -l $K2 -r $READ_LENGTH
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


