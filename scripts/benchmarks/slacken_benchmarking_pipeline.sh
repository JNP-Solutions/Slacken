#!/bin/bash

LIBRARY=$1

if [ "$LIBRARY" == "refseq-224pc" ]; then
    LIBNAME="rspc_35_31_s7"
elif [ "$LIBRARY" == "standard-224c" ]; then
    LIBNAME="std_35_31_s7"
else
    echo "Error: Invalid input library - $LIBRARY. Exiting..." 1>&2
    exit 1
fi

FNAME=$2
LABEL=$3
CTYPE=$4
RVAL=$5
SAMPSTART=$6
SAMPEND=$7

#Main bucket
ROOT=s3://onr-emr
#Directory for permanently kept data
DATA=$ROOT/keep

#Standard library
#K2=$ROOT/standard-224c
#Refseq
#K2=$ROOT/refseq-224pc
K2=$ROOT/$1
TAXONOMY=$K2/taxonomy

. scripts/benchmarks/slacken_benchmarking_steps.sh

# BUCKET=s3://onr-emr/Slacken_nishad
# DISCOUNT_HOME=/Users/n-dawg/IdeaProjects/Slacken-SBI


#In this script, please always use two decimal points, e.g. 0.10, not 0.1
#CS=(0.05 0.10)
CS=(0.00 0.05 0.10 0.15)

#airskinurogenital strain marine plant_associated
#LABEL=strain
#FAMILY=cami2/$LABEL
FAMILY=$FNAME/$LABEL
SPATH=$ROOT/$FAMILY
SAMPLES=()

for ((i = $SAMPSTART; i <= $SAMPEND; i++))
do
  SAMPLES+=($SPATH/sample$i/anonymous_reads.part_001.f.fq $SPATH/sample$i/anonymous_reads.part_002.f.fq)
  #SAMPLES+=($SPATH/sample$i/anonymous_reads_part_001.fq $SPATH/sample$i/anonymous_reads_part_002.fq)
done

if [ "$CTYPE" == "dynamic" ]; then
    RNAME="R$5"
    RUNAME="${LIBNAME/_/_${RNAME}_}"
    #echo "classifyDynamic $LIBNAME $RUNAME $RVAL"
    classifyDynamic $LIBNAME $RUNAME $RVAL

elif [ "$CTYPE" == "gold" ]; then
    RNAME="gold"
    RUNAME="${LIBNAME/_/_${RNAME}_}"
    #echo "classifyGS $LIBNAME $RUNAME"
    classifyGS $LIBNAME $RUNAME

elif [ "$CTYPE" == "kraken" ]; then
    RNAME="c15"
    RUNAME="${LIBNAME/_/_${RNAME}_}"
    #echo "classify $LIBNAME $RUNAME"
    classify $LIBNAME $RUNAME

else
    echo "Error: Invalid CTYPE input - $CTYPE. Exiting..." 1>&2
    exit 1
fi

for ((s = $SAMPSTART; s <= $SAMPEND; s++))
do
  #echo "compare $s $RUNAME"
  compare $s $RUNAME
  sleep 10
done