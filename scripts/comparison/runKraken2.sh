#!/bin/bash

#Script for running Kraken 2 and producing slacken-compatible outputs that can be used with the compare command.

CS="0.00 0.05 0.10 0.15"
FAMILY=strain
THREADS=32

for C in $CS
do
        for ((i = 0; i < 10; i++))
        do
                DEST=scratch/$FAMILY/kraken2_35_31_s7_c${C}_classified
                mkdir -p $DEST/sample=S$i
                time kraken2 --memory-mapping --db std_16/ --output $DEST/sample=S$i/classified \
                  --report $DEST/S${i}_kreport.txt --paired --confidence $C --threads $THREADS  $FAMILY/sampleS$i/anonymous_reads_part_00*.fq
        done
        find . -name "*classified" | xargs -n 1 -P $THREADS gzip
done