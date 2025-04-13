#!/bin/bash


function compare1 {
  MIN=$1
  shift
  SAMPLES=$1
  shift
  FAMILY=$1
  shift
  DIR=$1
  shift

for ((i=0; i <=$SAMPLES; i++))
do
scala compare_kraken_kraken.scala ~/SBIShared/Kraken_Benchmark/kraken2_35_31_s7/${FAMILY}_v2/kraken2_35_31_s7_c0.15_classified/S${i}_kreport.txt ~/SBIShared/$DIR/std_1-step-${MIN}--${SAMPLES}_35_31_s7/${FAMILY}_v2/std_1-step-${MIN}--${SAMPLES}_35_31_s7_c0.15_classified/S${i}_kreport.txt
done

}

function compare2 {
  SAMPLES=$1
  shift
  FAMILY=$1
  shift
  DIR=$1
  shift

for ((i=0; i <=$SAMPLES; i++))
do
scala compare_kraken_kraken.scala ~/SBIShared/Kraken_Benchmark/kraken2_35_31_s7/${FAMILY}_v2/kraken2_35_31_s7_c0.15_classified/S${i}_kreport.txt ~/SBIShared/$DIR/std_1-step_35_31_s7/${FAMILY}_v2/std_1-step_35_31_s7_c0.15_classified/S${i}_kreport.txt
done

}

compare1 0 50 strain Slacken_Strain0--50_Benchmark
compare2 9 Assorted_Genomes_225  Slacken_Benchmark_plant_inSilico_marine
compare2 9 Assorted_Genomes_Perfect_225 Slacken_Benchmark_plant_inSilico_marine
compare2 9 Assorted_Genomes_mbarc_225 Slacken_Benchmark_plant_inSilico_marine

compare1 0 9 marine Slacken_Benchmark_marine

compare2 19 plant_associated Slacken_Benchmark_plant_inSilico_marine


