These scripts are originally from the Kraken 2 project by Derrick Wood et al, under the MIT license.
In some cases they have been modified for use with Slacken.

See: https://github.com/DerrickWood/kraken2

We use them in Slacken to help download taxonomies and genomes, and to mask low-complexity
regions in genomes.

For the download process to work, the kraken2 binaries must be installed and available in the path (in particular,
k2mask). Other dependencies: seqkit (https://bioinf.shenwei.me/seqkit/download/), curl.

To download e.g. contigs and scaffolds, please export KRAKEN2_INCOMPLETE=1. This will
download incomplete as well as complete genomes (default is only complete and chromosomes), resulting in a larger
library.

Example environment:
KRAKEN2_DB_NAME= (wherever the database is)
KRAKEN2_DIR= (wherever these scripts are located)
KRAKEN2_INCOMPLETE=
KRAKEN2_PROTEIN_DB=
KRAKEN2_SKIP_MAPS=
KRAKEN2_USE_FTP=
KRAKEN2_MASK_LC=true
