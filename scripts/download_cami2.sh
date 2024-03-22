#!/bin/bash

BUCKET=s3://jnp-bio-eu/cami2

function samplePrefix {
  case $1 in
  marine)
    echo "marmg"
    ;;
  strain)
    echo "strmg"
    ;;
  plant_associated)
    echo "rhimg"
    ;;
  esac
}

function getSamples {
  PROJECT=$1
  GROUP=$2
  FROM=$3
  TO=$4

  DEST=$GROUP
  OPTS="--create-dir --output-dir $DEST -LO -C -"
  for ((s = $FROM; s <= $TO; s++))
  do
    case $GROUP in
      strain | plant_associated | marine)
        DIR=short_read
        URL=https://frl.publisso.de/data/$PROJECT/$GROUP/short_read/$(samplePrefix $GROUP)CAMI2_sample_${s}_reads.tar.gz
      curl $OPTS https://frl.publisso.de/data/$PROJECT/$GROUP/short_read/$(samplePrefix $GROUP)CAMI2_setup.tar.gz
        ;;
      *)
        DIR=.
        URL=https://frl.publisso.de/data/$PROJECT/$GROUP/sample_$s.tar.gz
        curl $OPTS https://frl.publisso.de/data/$PROJECT/$GROUP/setup.tar.gz || exit 1
        ;;
      esac

    echo $URL
    curl $OPTS $URL || exit 1
    SAMPLE=./$DEST/*sample_$s*.tar.gz
    tar xzf $SAMPLE
    gzip -d $DIR/*sample_$s/reads/anonymous_reads.fq.gz
    gzip -d $DIR/*sample_$s/reads/reads_mapping.tsv.gz
    #unpack inner files.
    seqkit split2 -p 2 -O sample$s $DIR/*sample_$s/reads/anonymous_reads.fq
    mv $DIR/*sample_$s/reads/reads_mapping.tsv sample$s
    aws s3 sync sample$s $BUCKET/$GROUP/sample$s || exit 1
    rm -r $SAMPLE $DIR/*sample_$s sample$s
  done
}

#PROJECT=frl:6425518
#groups=airskinurogenital gastrooral per_bodysite
#airskinurogenital samples 0-28
#gastrooral samples 0-19


#PROJECT=frl:6425521
#marine samples 0-9
#plant_associated samples 0-20
#strain samples 0-99

getSamples frl:6425521 strain 0 9
