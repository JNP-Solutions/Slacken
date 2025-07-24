#!/bin/bash

#Slacken steps for running on AWS

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
  CLASS_OUT=$ROOT/scratch/classified/$FAMILY/${LNAME}_classified
  ./slacken-aws.sh classify -i $DATA/$LIB \
    --sample-regex "(S[0-9]+)" -p -c $"${CS[@]}" -o $CLASS_OUT \
  "${SAMPLES[@]}"
}

#Classify with "gold set" dynamic library.
#Enabled by -d
function classifyGS {
  LIB=$1
  LNAME=$2
  #--index-reports
  CLASS_OUT=$ROOT/scratch/classified/$FAMILY/${LNAME}_classified
  ./slacken-aws.sh classify2 -i $DATA/$LIB --sample-regex "(S[0-9]+)" -p -c $"${CS[@]}" \
  -o $CLASS_OUT \
  -l $K2 --classify-with-gold -g $SPATH/${LABEL}_gold.txt \
      --bracken-length 150 \
      "${SAMPLES[@]}"
}

#2-step classify with dynamic library.
function classifyDynamic {
  LIB=$1
  LNAME=$2
  #--index-reports
  #--min-count
  #--min-distinct
  #--reads

  CLASS_OUT=$ROOT/scratch/classified/$FAMILY/${LNAME}_classified
  ./slacken-aws.sh classify2 -i $DATA/$LIB --sample-regex "(S[0-9]+)" -p -c $"${CS[@]}" \
  -o $CLASS_OUT \
    -l $K2 -g $SPATH/${LABEL}_gold.txt \
    --bracken-length 150 --reads 100 \
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

  PARAMS="-k $K -m $M --spaces $S "
  ./slacken-aws.sh -p $BUCKETS build $PARAMS -t $TAXONOMY -i $DATA/$NAME -l $K2 $OTHER
  histogram $NAME
}

function histogram {
  LIB=$1
  ./slacken-aws.sh stats -h -i $DATA/$LIB
}

function report {
  LIB=$1
  #-l $K2
  ./slacken-aws.sh inspect -i $DATA/$LIB -o $DATA/$LIB
}

function brackenBuild {
  LIB=$1
  READ_LENGTH=150
  #Note the special run script that sets $SPLIT properly for this job
  ./slacken-aws.sh -p 10000 bracken-build -i $DATA/$LIB -l $K2 -r $READ_LENGTH
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
