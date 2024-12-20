#!/bin/bash
#This file is part of Slacken.
#Script to download and pre-process nucleotide sequences for the refseq prefer complete library.
#Reference: standard_installation.sh in kraken2

#See README.txt in this directory for guidance.

#Example environment:
#KRAKEN2_DB_NAME=(destination)
#KRAKEN2_DIR=(wherever these scripts are located)

export KRAKEN2_FILTER_LEVEL=prefer_complete
export KRAKEN2_PROTEIN_DB=
export KRAKEN2_SKIP_MAPS=
export KRAKEN2_USE_FTP=
export KRAKEN2_MASK_LC=true

download_taxonomy.sh

download_genomic_library.sh refseq
download_genomic_library.sh plasmid
download_genomic_library.sh UniVec_Core

build_kraken2_db.sh
