#!/bin/bash

#Slacken evaluation pipeline. Currently runs on AWS.
#Supports three main functions:
# 1) build libraries, 2) classify samples, 3) compare classified output with reference.
#Work in progress.

#Regional buckets. jnp-bio is Japan.
#ROOT=s3://jnp-bio
#ROOT=s3://jnp-bio-us
#ROOT=s3://jnp-bio-us
ROOT=s3://onr-emr

#data directory where objects are deleted after a few days
#DATA=$ROOT/scratch
#Directory for permanently kept data
DATA=$ROOT/keep

#Standard library
#K2=$ROOT/kraken2
#K2=$ROOT/standard-224c
#NT library
#K2=$ROOT/k2-nt
#Refseq
#K2=$ROOT/refseq-223
K2=$ROOT/refseq-224c

TAXONOMY=$K2/taxonomy
#TAXONOMY=$ROOT/k2-nt/taxonomy

#slacken2-aws.sh always picks the jar from this (US) region bucket
BUCKET=s3://onr-emr/slacken
DISCOUNT_HOME=/home/johan/ws/jnps/Hypercut-git

aws s3 cp $DISCOUNT_HOME/target/scala-2.12/Slacken-assembly-0.1.0.jar $BUCKET/

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
  ./slacken2-aws.sh taxonIndex $DATA/$LIB brackenWeights -l $K2 -r $READ_LENGTH
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
  SAMPLES+=($SPATH/sample$i/anonymous_reads.part_001.f.fq $SPATH/sample$i/anonymous_reads.part_002.f.fq)
done

#build s2_2023 45 41 7 2000

#histogram std_35_31_s7
#report s2-nt_35_31_s7
#report s2_2023_i_35_31_s7
#report rsc_35_31_s7 35
#report std_35_31_s7

#classifyGS s2_2023_i_35_31_s7 s2_2023_gold_35_31_s7
#classify rsc_35_31_s7 rsc_35_31_s7
#classifyGS rsc_35_31_s7 rsc_gold_35_31_s7
#classify std_35_31_s7 std_35_31_s7
#classify rs_45_41_s7
#classify rs_45_41_s12

#for ((s = 0; s <= 9; s++))
#do
#  compare $s rsc_35_31_s7
#  sleep 10
#done

brackenWeights rsc_35_31_s7

