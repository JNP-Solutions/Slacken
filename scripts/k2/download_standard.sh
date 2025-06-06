#!/bin/bash
#This file is part of Slacken.
#Script to download and pre-process nucleotide sequences for the Kraken 2 standard library for use in Slacken.
#Reference: standard_installation.sh in kraken2

#See README.txt in this directory for guidance.

#Example environment:
#KRAKEN2_DB_NAME=(destination)
#KRAKEN2_DIR=(wherever these scripts are located)

export KRAKEN2_FILTER_LEVEL=complete
export KRAKEN2_PROTEIN_DB=
export KRAKEN2_SKIP_MAPS=
export KRAKEN2_USE_FTP=
export KRAKEN2_MASK_LC=true

download_taxonomy.sh
download_genomic_library.sh archaea
download_genomic_library.sh bacteria
download_genomic_library.sh viral

download_genomic_library.sh plasmid
download_genomic_library.sh UniVec_Core

#The human genome is not low complexity masked
export KRAKEN2_MASK_LC=""
download_genomic_library.sh human

build_kraken2_db.sh
