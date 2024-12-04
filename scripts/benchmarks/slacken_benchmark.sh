#!/bin/bash
BUCKET=s3://onr-emr/Slacken_nishad
DISCOUNT_HOME=/Users/n-dawg/IdeaProjects/Slacken-SBI
aws --profile sbi s3 cp $DISCOUNT_HOME/target/scala-2.12/Slacken-assembly-1.0.0.jar $BUCKET/

# FAMILY=$1
# LABEL=$2
# SAMPSTART=$3
# SAMPEND=$4

# ./scripts/benchmarks/slacken_benchmarking_pipeline.sh refseq-224pc $1 $2 gold NA $SAMPSTART $SAMPEND
# ./scripts/benchmarks/slacken_benchmarking_pipeline.sh refseq-224pc $1 $2 kraken NA $SAMPSTART $SAMPEND
# ./scripts/benchmarks/slacken_benchmarking_pipeline.sh refseq-224pc $1 $2 dynamic 1 $SAMPSTART $SAMPEND
# ./scripts/benchmarks/slacken_benchmarking_pipeline.sh refseq-224pc $1 $2 dynamic 10 $SAMPSTART $SAMPEND
# ./scripts/benchmarks/slacken_benchmarking_pipeline.sh refseq-224pc $1 $2 dynamic 100 $SAMPSTART $SAMPEND

# ./scripts/benchmarks/slacken_benchmarking_pipeline.sh standard-224c $1 $2 gold NA $SAMPSTART $SAMPEND
# ./scripts/benchmarks/slacken_benchmarking_pipeline.sh standard-224c $1 $2 kraken NA $SAMPSTART $SAMPEND
# ./scripts/benchmarks/slacken_benchmarking_pipeline.sh standard-224c $1 $2 dynamic 1 $SAMPSTART $SAMPEND
# ./scripts/benchmarks/slacken_benchmarking_pipeline.sh standard-224c $1 $2 dynamic 10 $SAMPSTART $SAMPEND
# ./scripts/benchmarks/slacken_benchmarking_pipeline.sh standard-224c $1 $2 dynamic 100 $SAMPSTART $SAMPEND

# lists=(
#   "cami2 marine 0 9"
#   "cami2 strain 0 50"
#   "cami2 strain 51 99"
#   "cami2 plant_associated 0 19"
#   "inSilico Assorted_Genomes_225 0 9"
#   "inSilico Assorted_Genomes_Perfect_225 0 9"
# )

# lists=(
#   "inSilico Assorted_Genomes_225 0 9"
#   "inSilico Assorted_Genomes_Perfect_225 0 9"
#   "inSilico Assorted_Genomes_mbarc_225 0 9"
# )

lists=(
  "cami2 strain 0 0"
)

# Loop through the outer list
for list in "${lists[@]}"
do
  read FAMILY LABEL SAMPSTART SAMPEND <<< "$list"

  # FAMILY=$1
  # LABEL=$2
  # SAMPSTART=$3
  # SAMPEND=$4

  ./scripts/benchmarks/slacken_benchmarking_pipeline.sh refseq-224pc $FAMILY $LABEL gold NA $SAMPSTART $SAMPEND
  ./scripts/benchmarks/slacken_benchmarking_pipeline.sh refseq-224pc $FAMILY $LABEL kraken NA $SAMPSTART $SAMPEND
  ./scripts/benchmarks/slacken_benchmarking_pipeline.sh refseq-224pc $FAMILY $LABEL dynamic 1 $SAMPSTART $SAMPEND
  ./scripts/benchmarks/slacken_benchmarking_pipeline.sh refseq-224pc $FAMILY $LABEL dynamic 10 $SAMPSTART $SAMPEND
  ./scripts/benchmarks/slacken_benchmarking_pipeline.sh refseq-224pc $FAMILY $LABEL dynamic 100 $SAMPSTART $SAMPEND

  ./scripts/benchmarks/slacken_benchmarking_pipeline.sh standard-224c $FAMILY $LABEL gold NA $SAMPSTART $SAMPEND
  ./scripts/benchmarks/slacken_benchmarking_pipeline.sh standard-224c $FAMILY $LABEL kraken NA $SAMPSTART $SAMPEND
  ./scripts/benchmarks/slacken_benchmarking_pipeline.sh standard-224c $FAMILY $LABEL dynamic 1 $SAMPSTART $SAMPEND
  ./scripts/benchmarks/slacken_benchmarking_pipeline.sh standard-224c $FAMILY $LABEL dynamic 10 $SAMPSTART $SAMPEND
  ./scripts/benchmarks/slacken_benchmarking_pipeline.sh standard-224c $FAMILY $LABEL dynamic 100 $SAMPSTART $SAMPEND
done

#marine
#######
# ./scripts/benchmarks/slacken_benchmarking_pipeline.sh refseq-224pc cami2 marine gold
# ./scripts/benchmarks/slacken_benchmarking_pipeline.sh refseq-224pc cami2 marine kraken
# ./scripts/benchmarks/slacken_benchmarking_pipeline.sh refseq-224pc cami2 marine dynamic 1
# ./scripts/benchmarks/slacken_benchmarking_pipeline.sh refseq-224pc cami2 marine dynamic 10
# ./scripts/benchmarks/slacken_benchmarking_pipeline.sh refseq-224pc cami2 marine dynamic 100

# ./scripts/benchmarks/slacken_benchmarking_pipeline.sh standard-224c cami2 marine gold
# ./scripts/benchmarks/slacken_benchmarking_pipeline.sh standard-224c cami2 marine kraken
# ./scripts/benchmarks/slacken_benchmarking_pipeline.sh standard-224c cami2 marine dynamic 1
# ./scripts/benchmarks/slacken_benchmarking_pipeline.sh standard-224c cami2 marine dynamic 10
# ./scripts/benchmarks/slacken_benchmarking_pipeline.sh standard-224c cami2 marine dynamic 100
########

# #plant_associated
# # ########
# ./scripts/benchmarks/slacken_benchmarking_pipeline.sh refseq-224pc cami2 plant_associated gold
# ./scripts/benchmarks/slacken_benchmarking_pipeline.sh refseq-224pc cami2 plant_associated kraken
# ./scripts/benchmarks/slacken_benchmarking_pipeline.sh refseq-224pc cami2 plant_associated dynamic 1
# ./scripts/benchmarks/slacken_benchmarking_pipeline.sh refseq-224pc cami2 plant_associated dynamic 10
# ./scripts/benchmarks/slacken_benchmarking_pipeline.sh refseq-224pc cami2 plant_associated dynamic 100

# ./scripts/benchmarks/slacken_benchmarking_pipeline.sh standard-224c cami2 plant_associated gold
# ./scripts/benchmarks/slacken_benchmarking_pipeline.sh standard-224c cami2 plant_associated kraken
# ./scripts/benchmarks/slacken_benchmarking_pipeline.sh standard-224c cami2 plant_associated dynamic 1
# ./scripts/benchmarks/slacken_benchmarking_pipeline.sh standard-224c cami2 plant_associated dynamic 10
#./scripts/benchmarks/slacken_benchmarking_pipeline.sh standard-224c cami2 plant_associated dynamic 100
# ########


# #inSilico
# # # ########
# ./scripts/benchmarks/slacken_benchmarking_pipeline.sh refseq-224pc inSilico bacteriaSmall214 gold
# ./scripts/benchmarks/slacken_benchmarking_pipeline.sh refseq-224pc inSilico bacteriaSmall214 kraken
# ./scripts/benchmarks/slacken_benchmarking_pipeline.sh refseq-224pc inSilico bacteriaSmall214 dynamic 1
# ./scripts/benchmarks/slacken_benchmarking_pipeline.sh refseq-224pc inSilico bacteriaSmall214 dynamic 10
# ./scripts/benchmarks/slacken_benchmarking_pipeline.sh refseq-224pc inSilico bacteriaSmall214 dynamic 100

# ./scripts/benchmarks/slacken_benchmarking_pipeline.sh standard-224c inSilico bacteriaSmall214 gold
# ./scripts/benchmarks/slacken_benchmarking_pipeline.sh standard-224c inSilico bacteriaSmall214 kraken
# ./scripts/benchmarks/slacken_benchmarking_pipeline.sh standard-224c inSilico bacteriaSmall214 dynamic 1
# ./scripts/benchmarks/slacken_benchmarking_pipeline.sh standard-224c inSilico bacteriaSmall214 dynamic 10
# ./scripts/benchmarks/slacken_benchmarking_pipeline.sh standard-224c inSilico bacteriaSmall214 dynamic 100
########