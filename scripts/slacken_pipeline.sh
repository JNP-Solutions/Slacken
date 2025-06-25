#!/bin/bash

. scripts/slacken_steps_lib.sh

#confidence thresholds.

#In this script, please always use two decimal points, e.g. 0.10, not 0.1
#CS=(0.05 0.10)
CS=(0.00 0.05 0.10 0.15)

#strain marine plant_associated
LABEL=strain
FAMILY=cami2/$LABEL
SPATH=$ROOT/$FAMILY
SAMPLES=()

for ((i = 0; i <= 9; i++))
do
  SAMPLES+=($SPATH/sample$i/anonymous_reads.part_001.f.fq $SPATH/sample$i/anonymous_reads.part_002.f.fq)
done


#build rspc 35 31 7 30000

#build std 35 31 7 2000

#histogram std_35_31_s7
#report std_35_31_s7

#classify rspc_35_31_s7 rspc_35_31_s7
classifyGS rspc_35_31_s7 rspc_gold_35_31_s7
#classify std_35_31_s7 std_35_31_s7

compare rspc_gold_35_31_s7

#brackenBuild std_35_31_s7
