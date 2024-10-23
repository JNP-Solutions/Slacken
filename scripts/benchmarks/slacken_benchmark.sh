#!/bin/bash
BUCKET=s3://onr-emr/Slacken_nishad
DISCOUNT_HOME=/Users/n-dawg/IdeaProjects/Slacken-SBI
aws --profile sbi s3 cp $DISCOUNT_HOME/target/scala-2.12/Slacken-assembly-0.1.0.jar $BUCKET/

FAMILY=$1
LABEL=$2
SAMPSTART=$3
SAMPEND=$4

./scripts/benchmarks/slacken_benchmarking_pipeline.sh refseq-224pc $1 $2 gold NA $SAMPSTART $SAMPEND
./scripts/benchmarks/slacken_benchmarking_pipeline.sh refseq-224pc $1 $2 kraken NA $SAMPSTART $SAMPEND
./scripts/benchmarks/slacken_benchmarking_pipeline.sh refseq-224pc $1 $2 dynamic 1 $SAMPSTART $SAMPEND
./scripts/benchmarks/slacken_benchmarking_pipeline.sh refseq-224pc $1 $2 dynamic 10 $SAMPSTART $SAMPEND
./scripts/benchmarks/slacken_benchmarking_pipeline.sh refseq-224pc $1 $2 dynamic 100 $SAMPSTART $SAMPEND

./scripts/benchmarks/slacken_benchmarking_pipeline.sh standard-224c $1 $2 gold NA $SAMPSTART $SAMPEND
./scripts/benchmarks/slacken_benchmarking_pipeline.sh standard-224c $1 $2 kraken NA $SAMPSTART $SAMPEND
./scripts/benchmarks/slacken_benchmarking_pipeline.sh standard-224c $1 $2 dynamic 1 $SAMPSTART $SAMPEND
./scripts/benchmarks/slacken_benchmarking_pipeline.sh standard-224c $1 $2 dynamic 10 $SAMPSTART $SAMPEND
./scripts/benchmarks/slacken_benchmarking_pipeline.sh standard-224c $1 $2 dynamic 100 $SAMPSTART $SAMPEND



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