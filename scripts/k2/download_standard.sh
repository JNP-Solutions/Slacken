#!/bin/bash
#This file is part of Slacken.
#Script to download and pre-process sequences for the Kraken 2 standard library (nucleotide form only, not protein).
#Reference: standard_installation.sh in kraken2

#Example environment:
#KRAKEN2_DB_NAME= (wherever the database is)
#KRAKEN2_DIR= (wherever these scripts are located)

export KRAKEN2_INCOMPLETE=
export KRAKEN2_PROTEIN_DB=
export KRAKEN2_SKIP_MAPS=
export KRAKEN2_USE_FTP=
export KRAKEN2_MASK_LC=true

download_taxonomy.sh
download_genomic_library.sh archaea
download_genomic_library.sh bacteria
download_genomic_library.sh viral

#Complete refseq that includes archaea, bacteria, viral, human as well as other sections like plants
#(but not plasmid, which gets special treatment).
#If this is downloaded, everything else except taxonomy, UniVec_Core and plasmid can be commented out.
#download_genomic_library.sh refseq

download_genomic_library.sh plasmid
download_genomic_library.sh UniVec_Core

#The human genome is not low-complexity masked
export KRAKEN2_MASK_LC=""
download_genomic_library.sh human

build_kraken2_db.sh
