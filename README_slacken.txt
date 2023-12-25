Slacken is an implementation of Kraken on Spark, provided as-is for evaluation purposes only.
Copyright (c) Johan Nyström-Persson 2019-2023.

First, configure by editing submit-slacken.sh.
To run locally, the Spark distribution (3.0 or later) needs to be available and its location must be configured. The location of scratch space (temporary shuffle files) should also be configured.

To get help:

./submit-slacken.sh --help

First, a database (taxonomic index) must be built once, after which it may be used multiple times to classify reads.


1) Example command to build a database:

./submit-slacken.sh taxonIndex -l /fast/scratch/s1test -t /fast/scratch/kraken2/taxonomy  build -l /fast/scratch/kraken2/seqid2taxid.map   /fast/scratch/kraken2/library/fungi/library.fna

Where: --maxlen is the maximum length of input genomes,
-l /fast/scratch/s1test is the location where the index will be stored,
-b is the number of buckets to use for partitioning,
-t /fast/scratch/kraken2/taxonomy is a directory containing NCBI-style nodes.dmp and names.dmp,
-l (second time) is the file mapping sequence IDs to taxa,
library.fna is a multi-fasta file containing genomes to be indexed. Multiple files can be specified.

It is convenient to first build a standard kraken or kraken2 database using the scripts included in those tools. This will generate names.dmp, nodes.dmp and seqid2taxid.map.



2) Example command to classify reads:

./submit-slacken.sh taxonIndex -t /fast/scratch/kraken2/taxonomy  -l /fast/scratch/s1test classify /fast/scratch/SRR094926/10M.fasta -o /fast/scratch/s1_10M

Where:
-t is as above,
-l is the location of the index that was built using the command above,
-b is as above,
10M.fasta is a file with reads to be classified (fasta/fastq, uncompressed, multiple files may be specified),
-o is the location where outputs should be saved.


3) Example for running on AWS:

This assumes that the aws CLI tools are installed and correctly set up.
First, edit slacken-aws.sh and set the region (us-east-1 is the default).
Create a cluster in AWS EMR. Machines benefit from having fast physical SSDs. A good machine type is for example m6gd.4xlarge, which has 16 vCPU, 64 GB RAM, 1 TB SSD.
Create a writable S3 bucket and configure this in the script as well.
Take note of the cluster ID. For example, if it is j-1PAZEEY38FDAR, then the classify command could be as follows:

./slacken-aws.sh j-1PAZEEY38FDAR taxonIndex -t s3://mybucket/taxonomy  -l s3://mybucket/s1test classify s3://mybucket/10M.fasta -o s3://mybucket/s1_10M


