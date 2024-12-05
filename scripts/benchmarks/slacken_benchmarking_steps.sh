#!/bin/bash

#Slacken benchmarking pipeline. Currently runs on AWS.

#Regular 1-step classify
function classify {
  LIB=$1
  LNAME=$2

  CLASS_OUT=$ROOT/scratch/classified/$FAMILY/$LNAME
  ./slacken-aws.sh taxonIndex $DATA/$LIB classify \
    --sample-regex "(S[0-9]+)" -p -c $"${CS[@]}" -o $CLASS_OUT \
  "${SAMPLES[@]}"

  #   CLASS_OUT=$ROOT/scratch/classified/$FAMILY/$LNAME
  # ./slacken2-aws.sh taxonIndex $DATA/$LIB classify \
  #   --sample-regex "_lactobacillus_0\.(05|10|15|20)" -c $"${CS[@]}" -o $CLASS_OUT \
  # "${SAMPLES[@]}"
}

#Classify with "gold set" dynamic library.
#Enabled by -d
function classifyGS {
  LIB=$1
  LNAME=$2
  #--report-index
  #-p 3000
  CLASS_OUT=$ROOT/scratch/classified/$FAMILY/$LNAME
  ./slacken-aws.sh -p 3000 taxonIndex $DATA/$LIB classify --sample-regex "(S[0-9]+)" -p -c $"${CS[@]}" \
  -o $CLASS_OUT "${SAMPLES[@]}" \
  dynamic $K2 --classify-with-gold -g $SPATH/${LABEL}_gold.txt \
      --bracken-length 150
}

#2-step classify with dynamic library.
function classifyDynamic {
  LIB=$1
  LNAME=$2
  RVALUE=$3
  #--report-index
  #--min-count
  #--min-reads
  #--min-distinct

  CLASS_OUT=$ROOT/scratch/classified/$FAMILY/$LNAME
  ./slacken-aws.sh -p 3000 taxonIndex $DATA/$LIB classify --sample-regex "(S[0-9]+)" -p -c $"${CS[@]}" \
  -o $CLASS_OUT "${SAMPLES[@]}" \
    dynamic $K2 -g $SPATH/${LABEL}_gold.txt \
    --bracken-length 150 --min-reads $RVALUE
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


