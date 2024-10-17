#!/bin/bash

. scripts/slacken_steps_lib.sh

BUCKET=s3://onr-emr/slacken_johan
DISCOUNT_HOME=/home/johan/ws/jnps/Hypercut-git

aws --profile sbi s3 cp $DISCOUNT_HOME/target/scala-2.12/Slacken-assembly-0.1.0.jar $BUCKET/

#In this script, please always use two decimal points, e.g. 0.10, not 0.1
#CS=(0.05 0.10)
CS=(0.00 0.05 0.10 0.15)

#airskinurogenital strain marine plant_associated
LABEL=strain
FAMILY=cami2/$LABEL
SPATH=$ROOT/$FAMILY
SAMPLES=()

for ((i = 0; i <= 9; i++))
do
  SAMPLES+=($SPATH/sample$i/anonymous_reads.part_001.f.fq $SPATH/sample$i/anonymous_reads.part_002.f.fq)
done


#build rspc 35 31 7 30000

#respace rs_45_41_s7 12
#histogram rs_45_41_s12

#build s2_2023 45 41 7 2000

#histogram std_35_31_s7
#report s2_2023_i_35_31_s7
#report rsc_35_31_s7 35
#report std_35_31_s7

#classifyGS s2_2023_i_35_31_s7 s2_2023_gold_35_31_s7
#classify rspc_35_31_s7 rspc_35_31_s7
classifyGS rspc_35_31_s7 rspc_gold_35_31_s7
#classify std_35_31_s7 std_35_31_s7
#classify rs_45_41_s7
#classify rs_45_41_s12

for ((s = 0; s <= 9; s++))
do
  STEP=$(compare $s rspc_gold_35_31_s7)
  echo "Waiting for $STEP"
  waitForStep $STEP
done

#brackenWeights rsc_35_31_s7
