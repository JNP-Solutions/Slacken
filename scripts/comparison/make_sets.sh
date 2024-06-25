#!/bin/bash

#Create gold sets

function compute {
  FILE=$1

  #exclude taxon 1
  cat $FILE | scala check_report.scala 100 | cut -f5 | grep -v -w 1
}

NORMALIZE="taxonkit filter -N -L genus"
OUTDIR=~/SBIshared/slackenTaxonSets
compute strain/rsc_gold_35_31_s7_c0.15_classified/S0_kreport.txt | $NORMALIZE  > $OUTDIR/strain_0.txt

compute plant_associated_v2/rsc_s_gold_35_31_s7_c0.15_classified/S0_kreport.txt | $NORMALIZE > $OUTDIR/plant_0.txt

compute marine_v2/rsc_gold_35_31_s7_c0.15_classified/S0_kreport.txt | $NORMALIZE > $OUTDIR/marine_0.txt

