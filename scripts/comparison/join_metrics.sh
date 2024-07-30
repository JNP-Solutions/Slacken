#!/bin/bash

#Very simple script to combine the various sampleN_metrics.tsv files produced
#when we compare classifications with a reference.
#Usage: join_metrics.sh sample1_metrics.tsv sample2_metrics.tsv ... > samples_metrics.tsv

head -1 $1
tail -q -n +2 $*
