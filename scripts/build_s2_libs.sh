#!/bin/bash

#Script for building Slacken libraries and classifying samples on AWS. 
#Work in progress.

#Regional buckets. jnp-bio is Japan.
ROOT=s3://jnp-bio
#ROOT=s3://jnp-bio-us
#ROOT=s3://jnp-bio-eu

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

aws s3 cp $DISCOUNT_HOME/target/scala-2.12/Slacken-assembly-0.1.0.jar $BUCKET/

#CAMI 1 samples
#SAMPLES=(M1_S001__insert_5000_reads_anonymous M1_S002__insert_5000_reads_anonymous)
#SPATH=$ROOT/cami_medium

#Tara oceans virome
SPATH=$ROOT/tov

function classify {
  LIB=$1
  C=$2
#$SPATH/ERR594352_1.fastq  $SPATH/ERR594352_2.fastq \
  OUT=${LIB}
  ./slacken2-aws.sh taxonIndex $DATA/$LIB classify --sample-regex "(ERR[0-9]+)" -p -c $C -o $SPATH/${OUT}_tov \
    $SPATH/ERR594352_1.fastq  $SPATH/ERR594352_2.fastq  $SPATH/ERR594353_1.fastq  $SPATH/ERR594353_2.fastq  $SPATH/ERR594354_1.fastq  $SPATH/ERR594354_2.fastq \
    $SPATH/ERR594355_1.fastq  $SPATH/ERR594355_2.fastq \
#    $SPATH/ERR594356_1.fastq  $SPATH/ERR594356_2.fastq  \
#    $SPATH/ERR594357_1.fastq  $SPATH/ERR594357_2.fastq  $SPATH/ERR594358_1.fastq  $SPATH/ERR594358_2.fastq  $SPATH/ERR594359_1.fastq $SPATH/ERR594359_2.fastq

#  for s in ${SAMPLES[@]}
#  do
#    ./slacken2-aws.sh taxonIndex $DATA/$LIB classify -p $SPATH/$s.1.fq $SPATH/$s.2.fq -c $C -o $SPATH/${s%__*}_$OUT
#  done
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

#Classify using a standard library
function classify_std {
  LIB=s2_2023_$1
  classify $LIB "$2"
}

#Build an NT library
function build_nt {
  K=$1
  M=$2
  S=$3
  NAME=nt_${K}_${M}_s${S}
  build $NAME "-k $K -m $M --spaces $S -p 20000"
}

#Classify using the NT library
function classify_nt {
  LIB=nt_$1
  classify $LIB "$2"
}


#build_std 35 31 7
#build_std 45 41 20

#build_nt 35 31 7
#build_nt 55 51 7
CS="0 0.15 0.30 0.45"

#classify_std 45_41_s12 "$CS"
#classify_std 45_41_s20 "$CS"

#  classify_nt 65_61_s22 "$CS"

classify_std 45_41_s20 0
#classify_nt 35_31_s7 0
