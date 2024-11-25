These genome downloading and preprocessing scripts are originally from the Kraken 2 project by Derrick Wood et al,
under the MIT license. (See: https://github.com/DerrickWood/kraken2)

They have been modified for use with Slacken.
The following changes were made:
 * Downloading using curl instead of wget
 * Parallel downloads
 * Running seqkit after download to generate .fai files for genomes
 * Multiple filter levels supported (instead of just complete/incomplete genomes)
 * Performance improvements

Slacken does not yet support protein (AA) databases.

For the download process to work, the kraken2 binaries must be installed and available in the path (in particular,
k2mask). Other dependencies: seqkit (https://bioinf.shenwei.me/seqkit/download/), curl, Scala 2.12, perl.

The directory $KRAKEN2_DB_NAME/library will be created and genomes will be stored there.
The taxonomy will be downloaded to $KRAKEN2_DB_NAME/taxonomy.

Example environment:

export KRAKEN2_DB_NAME=(destination directory)
export KRAKEN2_DIR=(wherever these scripts are located)
export KRAKEN2_PROTEIN_DB=
export KRAKEN2_SKIP_MAPS=
export KRAKEN2_USE_FTP=
export KRAKEN2_MASK_LC=true

#Choices are: complete, prefer_complete, all
#Complete selects only complete genomes ("chromosome" or "complete genome")
#All selects both complete and incomplete genomes.
#Prefer_complete selects complete genomes for those taxa that have them, and for other taxa selects incomplete ones.

export KRAKEN2_FILTER_LEVEL=prefer_complete