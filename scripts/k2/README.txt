These genome downloading and preprocessing scripts are originally from the Kraken 2 project by Derrick Wood et al,
under the MIT license. (See: https://github.com/DerrickWood/kraken2)

They have been modified for use with Slacken.
The following changes were made:
 * Downloading using curl instead of wget
 * Parallel downloads
 * Running seqkit after download to generate .fai files for genomes
 * Multiple filter levels supported (instead of just complete/incomplete genomes)
 * Performance improvements

Slacken does not yet support protein (AA) databases. These scripts can only download nucleotide sequences.

Dependencies that must be available in $PATH:

* the kraken2 binaries (in particular, k2mask)
* seqkit (https://bioinf.shenwei.me/seqkit/download/)
* curl
* Scala 2.12 (https://www.scala-lang.org/download/all.html)
* perl

To use these scripts, it is first necessary to configure some environment variables.
The directory $KRAKEN2_DB_NAME/library will be created and genomes will be stored there.
The taxonomy will be downloaded to $KRAKEN2_DB_NAME/taxonomy.

Example environment:

export KRAKEN2_DB_NAME=(destination directory)
export KRAKEN2_DIR=(wherever these scripts are located)

After configuring these variables, to download the standard database, use download_standard.sh.
To download the RefSeq prefer-complete database, use download_rspc.sh.
