#!/bin/bash

#Slacken evaluation pipeline. Currently runs on AWS.
#Supports three main functions:
# 1) build libraries, 2) classify samples, 3) compare classified output with reference.
#Work in progress.

#Regional buckets. jnp-bio is Japan.
#ROOT=s3://jnp-bio
#ROOT=s3://jnp-bio-us
ROOT=s3://jnp-bio-eu

#data directory where objects are deleted after a few days
#DATA=$ROOT/scratch
#Directory for permanently kept data
DATA=$ROOT/keep

#Standard library
K2=$ROOT/kraken2
#NT library
#K2=$ROOT/k2-nt
#Refseq
#K2=$ROOT/refseq-223

#TAXONOMY=$K2/taxonomy
TAXONOMY=$ROOT/k2-nt/taxonomy
#slacken2-aws.sh always picks the jar from this (JP) region bucket
BUCKET=s3://jnp-bio/discount
DISCOUNT_HOME=/home/johan/ws/jnps/Hypercut-git

aws s3 cp $DISCOUNT_HOME/target/scala-2.12/Slacken-assembly-0.1.0.jar $BUCKET/

function classify {
  LIB=$1
  LNAME=$2
  CLASS_OUT=$ROOT/scratch/classified/$FAMILY/$LNAME
  ./slacken2-aws.sh -p 3000 taxonIndex $DATA/$LIB classify --classify-with-gold-standard -g $SPATH/${LABEL}_gold.txt \
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
  ./slacken2-aws.sh taxonIndex $DATA/$LIB report -l $K2/seqid2taxid.map -o $DATA/$LIB
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

#build s2_2023 35 31 7 2000
#build nt 35 31 7 20000
#build s2-nt 35 31 7 2000 "--negative $ROOT/k2-nt"

#build rs 35 31 7 30000
#build rs 45 41 7 30000
#respace rs_45_41_s7 12
#histogram rs_45_41_s12

#In this script, please always use two decimal points, e.g. 0.10, not 0.1
#CS=(0.05 0.10)
CS=(0.00 0.05 0.10 0.15)

#airskinurogenital strain marine plant_associated
LABEL=plant_associated
FAMILY=cami2/$LABEL
SPATH=$ROOT/$FAMILY
SAMPLES=()

for ((i = 0; i <= 9; i++))
do
  SAMPLES+=($SPATH/sample$i/anonymous_reads.part_001.fq $SPATH/sample$i/anonymous_reads.part_002.fq)
done

#build s2_2023 45 41 7 2000
#report s2-nt_35_31_s7
#classify s2_2023_i_35_31_s7 s2_2023_gold_35_31_s7
#classify rs_35_31_s7 rs_dyn3_35_31_s7
classify rs_35_31_s7 rs_gold_35_31_s7
#classify rs_45_41_s7
#classify rs_45_41_s12

for ((s = 0; s <= 9; s++))
do
  compare $s rs_gold_35_31_s7
  sleep 10
done

