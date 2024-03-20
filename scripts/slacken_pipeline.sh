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

TAXONOMY=$K2/taxonomy
BUCKET=$ROOT/discount
DISCOUNT_HOME=/home/johan/ws/jnps/Hypercut-git

#aws s3 cp $DISCOUNT_HOME/target/scala-2.12/Slacken-assembly-0.1.0.jar $BUCKET/

SPATH=$ROOT/cami2/airskinurogenital
CAMI2_SAMPLES=()

for ((i = 4; i <= 9; i++))
do
  CAMI2_SAMPLES+=($SPATH/sample$i/anonymous_reads.part_001.fq $SPATH/sample$i/anonymous_reads.part_002.fq)
done

function classify {
  LIB=$1
  shift
  C="$@"
  OUT=${LIB}
  ./slacken2-aws.sh taxonIndex $DATA/$LIB classify --sample-regex "(S[0-9]+)" -p -c $C -o $SPATH/${OUT}_cami2 \
  "${CAMI2_SAMPLES[@]}"
}

function build {
  NAME=$1
  PARAMS=$2
  ./slacken2-aws.sh $PARAMS -t $TAXONOMY taxonIndex $DATA/$NAME build -l $K2
  ./slacken2-aws.sh taxonIndex $DATA/$NAME histogram
}

#Build a Kraken 2 standard library
function build_std {
  K=$1
  M=$2
  S=$3
  NAME=s2_2023_${K}_${M}_s${S}
  build $NAME "-k $K -m $M --spaces $S -p 2000"
}

#Build an NT library
function build_nt {
  K=$1
  M=$2
  S=$3
  NAME=nt_${K}_${M}_s${S}
  build $NAME "-k $K -m $M --spaces $S -p 20000"
}

#Classify using a standard library
function classify_std {
  LIB=s2_2023_$1
  shift
  classify $LIB "$@"
}

#Classify using the NT library
function classify_nt {
  LIB=nt_$1
  classify $LIB "$@"
}

#Compare classifications of a single sample against a reference
function compare {
  SAMPLE=$1
  LIB=$2

  CLASSIFICATIONS=""
  for C in "${CS[@]}"
  do
    CLASSIFICATIONS="$CLASSIFICATIONS $SPATH/${LIB}_cami2_c${C}_classified/sample=S$SAMPLE"
  done

  REF=$SPATH/sample$SAMPLE/reads_mapping.tsv
  ./slacken2-aws.sh -t $TAXONOMY compare -r $REF -i 1 -T 3 -h -o $SPATH/sample$SAMPLE $CLASSIFICATIONS
}

#build_std 35 31 7
#build_nt 35 31 7

CS=(0.00 0.15 0.30 0.45)

#classify_std 45_41_s12 "$CS"
#classify_std 45_41_s20 "$CS"

#  classify_nt 65_61_s22 "$CS"

#build_std 35 31 7

classify_std 35_31_s7 "${CS[@]}"

for ((s = 4; s <= 9; s++))
do
  compare $s s2_2023_35_31_s7
  sleep 10
done