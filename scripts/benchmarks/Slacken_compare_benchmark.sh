#!/bin/bash
BUCKET=s3://onr-emr/Slacken_nishad
DISCOUNT_HOME=/Users/n-dawg/IdeaProjects/Slacken-SBI
aws --profile sbi s3 cp $DISCOUNT_HOME/target/scala-2.12/Slacken-assembly-1.0.0.jar $BUCKET/

function compare {
  
  RUNAME=$1
  FNAME=$2
  LABEL=$3
  LIB=$4

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
    CLASSIFICATIONS="$CLASSIFICATIONS $ROOT/scratch/classified/$FAMILY/${RUNAME}_c${C}_classified"
  done
  #Directory expected to contain reads_mapping.tsv reference files for each sample
  REF=$SPATH
  ./slacken-aws.sh -t $TAXONOMY compare -r $REF -i 1 -T 3 -h -o $ROOT/scratch/classified/$FAMILY/$RUNAME/samples --multi-dirs $CLASSIFICATIONS
}

libraries=("refseq-224pc" "standard-224c")
first_run_names=("rspc_" "std_")
second_run_names=("1-step_35_31_s7" "gold_35_31_s7" "R1_35_31_s7" "R10_35_31_s7" "R100_35_31_s7")
alt_second_run_names=("1-step-0--50_35_31_s7" "gold-0--50_35_31_s7" "R1-0--50_35_31_s7" "R10-0--50_35_31_s7" "R100-0--50_35_31_s7")
length1=${#libraries[@]}
length2=${#second_run_names[@]}

for ((i=0; i<$length1; i++)); do
    for ((j=0; j<$length2; j++)); do
      compare ${first_run_names[$i]}${alt_second_run_names[$j]} cami2 strain ${libraries[$i]}      
      compare ${first_run_names[$i]}${second_run_names[$j]} cami2 strain ${libraries[$i]}
      compare ${first_run_names[$i]}${second_run_names[$j]} cami2 marine ${libraries[$i]}
      compare ${first_run_names[$i]}${second_run_names[$j]} cami2 plant_associated ${libraries[$i]}
      compare ${first_run_names[$i]}${second_run_names[$j]} inSilico Assorted_Genomes_225 ${libraries[$i]}
      compare ${first_run_names[$i]}${second_run_names[$j]} inSilico Assorted_Genomes_mbarc_225 ${libraries[$i]}
      compare ${first_run_names[$i]}${second_run_names[$j]} inSilico Assorted_Genomes_Perfect_225 ${libraries[$i]}
    done
done