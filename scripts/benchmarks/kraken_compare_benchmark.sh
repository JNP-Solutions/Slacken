#!/bin/bash
BUCKET=s3://onr-emr/Slacken_nishad
DISCOUNT_HOME=/Users/n-dawg/IdeaProjects/Slacken-SBI
aws --profile sbi s3 cp $DISCOUNT_HOME/target/scala-2.12/Slacken-assembly-1.0.0.jar $BUCKET/

function compare {
  
  RUNAME=kraken2_35_31_s7
  FNAME=$1
  LABEL=$2
  LIB=standard-224c

  #Main bucket
  ROOT=s3://onr-emr
  #Directory for permanently kept data
  DATA=$ROOT/keep

  #Standard library
  #K2=$ROOT/standard-224c
  #Refseq
  #K2=$ROOT/refseq-224pc
  K2=$ROOT/$LIB
  TAXONOMY=$K2/taxonomy

  FAMILY=$FNAME/$LABEL
  SPATH=$ROOT/$FAMILY

  CS=(0.00 0.05 0.10)

  #Directories expected to contain multi-sample classifications
  CLASSIFICATIONS=""
  for C in "${CS[@]}"
  do
    CLASSIFICATIONS="$CLASSIFICATIONS $ROOT/scratch-kraken/classified/$FAMILY/${RUNAME}_c${C}_classified"
  done

  #Directory expected to contain reads_mapping.tsv reference files for each sample
  REF=$SPATH
  ./slacken-aws.sh -t $TAXONOMY compare -r $REF -i 1 -T 3 -h -o $ROOT/scratch-kraken/classified/$FAMILY/$RUNAME/samples --multi-dirs $CLASSIFICATIONS
}

#compare $FNAME $LABEL
compare cami2 strain
compare cami2 marine
compare cami2 plant_associated
compare inSilico Assorted_Genomes_225
compare inSilico Assorted_Genomes_mbarc_225
compare inSilico Assorted_Genomes_Perfect_225