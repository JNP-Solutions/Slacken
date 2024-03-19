#!/bin/bash

BUCKET=s3://jnp-bio-eu/cami2
PROJECT=frl:6425518
GROUP=airskinurogenital
#groups=airskinurogenital gastrooral per_bodysite

#PROJECT=frl:6425521
#groups=marine plant_associated strain

DEST=$GROUP
OPTS="--create-dir --output-dir $DEST -LO"
for ((s = 0; s <= 28; s++))
do
    curl $OPTS https://frl.publisso.de/data/$PROJECT/$GROUP/sample_$s.tar.gz
    SAMPLE=./$DEST/sample_$s.tar.gz
    tar xzf $SAMPLE
    gzip -d *sample_$s/reads/anonymous_reads.fq.gz
    gzip -d *sample_$s/reads/reads_mapping.tsv.gz
    #unpack inner files
    seqkit split2 -p 2 -O sample$s *sample_$s/reads/anonymous_reads.fq
    mv *sample_$s/reads/reads_mapping.tsv sample$s
    aws s3 sync sample$s $BUCKET/$GROUP/sample$s
    rm -r $SAMPLE *sample_$s sample$s
done
curl $OPTS https://frl.publisso.de/data/$PROJECT/$GROUP/setup.tar.gz

