## Slacken

[![Build and test](https://github.com/jtnystrom/discount/actions/workflows/ci.yml/badge.svg)](https://github.com/jtnystrom/Discount/actions/workflows/ci.yml)
[![Docker Pulls](https://badgen.net/docker/pulls/jtnystrom/slacken?icon=docker&label=pulls)](https://hub.docker.com/r/jtnystrom/slacken/)
![GitHub License](https://img.shields.io/github/license/jnp-solutions/slacken)

Slacken is a metagenomic profiler that classifies genomic sequences based on k-mers and minimizers. It implements the
[Kraken 2](https://github.com/DerrickWood/kraken2)[1] algorithm, while also supporting a wider parameter space and additional 
algorithms. In particular, it supports sample-tailored libraries, where the minimizer library is built on the fly as part 
of read classification.

For more motivation and details, please see our [preprint](https://www.biorxiv.org/content/10.1101/2024.12.22.629657v1) on BioRXiv.

Copyright (c) Johan Nyström-Persson 2019-2025.

## Contents
1. [Basics](#basics)
  - [Introduction](#introduction)
  - [Running Slacken](#running-slacken)
  - [Pre-built genomic libraries](#obtaining-a-pre-built-genomic-library)
  - [Classifying reads (1-step)](#classifying-reads-1-step)
  - [Multi-sample mode](#multi-sample-mode)
  - [Use with Bracken](#use-with-bracken)
  - [Classifying reads (2-step/dynamic)](#classifying-reads-using-a-dynamic-index-2-step-method)
  - [Running on AWS Elastic MapReduce or large clusters](#running-on-aws-elastic-mapreduce-or-large-clusters)
2. [Technical details](#technical-details)
  - [Building a library](#building-a-library)
  - [Discrepancies between Slacken and Kraken 2](#discrepancies-between-slacken-and-kraken-2)
  - [Compiling](#compiling)
  - [Citation](#citation)
3. [References](#references)


## Basics

### Introduction

Slacken classifies metagenomic sequences (reads) according to k-mers and minimizers using the same algorithm as
Kraken 2. The end result is a classification for each read, as well as a summary report that shows the number of reads
and the fraction of reads assigned to each taxon. However, Slacken is based on Apache Spark and is thus a distributed application,
rather than run on a single machine as Kraken 2 does. Slacken can run on a single machine but it can
also scale to a cluster with hundreds or thousands of machines. It also does not keep all data in RAM but 
uses a combination of RAM and disk.

Slacken does not currently support translated mode (protein/AA sequence classification) but only nucleotide sequences.

Slacken has its own database format and can not use pre-built Kraken 2 databases as they are.

### Running Slacken

Minimal single-machine prerequisites: 
* [Spark](https://spark.apache.org/downloads.html) 3.5.0 or later (pre-built, for Scala 2.12. The Scala 2.13 version is not compatible.) It is sufficient 
to download and extract the Spark distribution somewhere.
* 16 GB or more of RAM (32 GB or more recommended).
* A fast SSD drive for temporary space is very helpful. The amount of space required depends on the size of the 
libraries and samples.
* As of December 2024, Spark supports Java 8/11/17. Java 23 is unsupported. Please refer to the Spark documentation.

#### Download

The latest precompiled Slacken may be downloaded as a .zip from the 
[Releases](https://github.com/JNP-Solutions/Slacken/releases). This is the easiest way to obtain Slacken.

A [Docker image](https://hub.docker.com/r/jtnystrom/slacken) is also available. If you are using the Docker image,
please refer to the instructions on that page.

Set up the environment:

```commandline
#path to the Spark distribution (where you extracted your Spark download)
export SPARK_HOME=SPARK_HOME=/usr/local/spark-3.5.1-bin-hadoop3  

#a location for scratch space on a fast hard drive
export SLACKEN_TMP=/tmp

#Optional memory limit. The default is 16g, which we recommend as the minimum 
#for single-machine use. More is better.
export SLACKEN_MEMORY=32g
```

Check that it works: 
`./slacken.sh --help`

These options may also be permanently configured by editing `slacken.sh`.

While Slacken is running, the Spark UI may be inspected at [http://localhost:4040](http://localhost:4040) if the process is running
locally. We refer users to the Spark documentation for more details.

### Obtaining a pre-built genomic library

We provide a pre-built library in a public S3 bucket at s3://slacken-sbi. The current version is based on
RefSeq release 224.

* Compressed bundle with everything (164 GB): https://s3.amazonaws.com/slacken-sbi/library/standard-224c.tar.gz
* Slacken index: s3://slacken-sbi/library/standard-224c/std_35_31_s7/
* Bracken weights: s3://slacken-sbi/library/standard-224c/std_35_31_s7_bracken/
* Library location for dynamic mode: s3://slacken-sbi/library/standard-224c/
* Taxonomy: s3://slacken-sbi/library/standard-224c/std_35_31_s7_taxonomy/

The libraries are hosted in the us-east-1 region of AWS, and when running Slacken on AWS EMR in that region,
these libraries may also be accessed directly from the public S3 bucket without downloading them.

### Classifying reads (1-step)

The "1-step" classification corresponds to the standard Kraken 2 method. It classifies reads based on the pre-built library only.

```
./slacken.sh taxonIndex mySlackenLib classify testData/SRR094926_10k.fasta \
  -o test_class
```

Where

* `mySlackenLib` is the location where the library was built. For the pre-built library, this would 
be `standard-224c/std_35_31_s7`.
* `SRR094926_10k.fasta` is the file with reads to be classified. Any number of files may be supplied.
* `test_class` is the directory where the output will be stored. Individual read classifications and a file 
`test_class_kreport.txt` will be created.

To classify mate pairs, the `-p` flag may be used. Input files are then expected to be paired up in sequence:

```
./slacken.sh taxonIndex mySlackenLib classify -p sample01.1.fq sample01.2.fq \
  sample02.1.fq sample02.2.fq -o test_class
```

The accepted input formats for samples are fasta and fastq. Compressed files (gzip, bzip2) are not supported.

### Multi-sample mode

With multi-sample mode, Slacken can classify multiple samples at once. Separate outputs will be generated for each
distinct sample identified. This mode is more efficient than single sample classification and recommended whenever possible.

Samples are identified by means of a regular expression that needs to match each sequence (read) ID.

For example:

```
./slacken.sh taxonIndex mySlackenLib classify -p sample01.1.fq sample01.2.fq \
  sample02.1.fq sample02.2.fq --sample-regex "(S[0-9]+)" -o test_class
```

With this regular expression, a read with the ID `@S0R10/1` in a fastq file would be assigned to sample `S0`,
the ID `@S10R10/1` would be assigned to sample `S10`, and so on. The first group in the regex, indicated by `(...)`, 
identifies the sample.

All reads from all files are pooled together during classification. The regex is the
sole way that reads are mapped to samples and the file of origin for each read is ignored.

### Use with Bracken

Slacken can produce [Bracken](https://github.com/jenniferlu717/Bracken)[2] weights, like those produced by `bracken-build`. 
They can be used directly with Bracken to re-estimate taxon abundances in a taxon profiles and correct for database bias.
(Bracken is an external tool developed by Jennifer Lu et al. For more details, please refer to their paper and GitHub site.)

For example:

```
./slacken.sh taxonIndex mySlackenLib brackenWeights --read-len 150
```

This will generate the file `mySlackenLib_bracken/database150mers.kmer_distrib`. Bracken can now
simply be invoked with `bracken -d mySlackenLib_bracken -r 150 ...`. 

### Classifying reads using a dynamic index (2-step method)

Slacken has the ability to build a dynamic minimizer library on the fly, which is tailored specifically to the samples being classified.
This "two-step method" usually leads to more precise classifications.

For this method, first, a static minimizer library must be built as usual, following the instructions above. This library is used
to sketch the taxon set in the sample/samples being classified by using a user-specified heuristic. During classification, a second minimizer library is built on the fly
and used to classify the reads for the final result. 

For example (100 reads heuristic, multi-sample mode):

```
./slacken.sh taxonIndex mySlackenLib classify -p \
 --sample-regex "(S[0-9]+)" -o test_class \
  dynamic --reads 100 -l k2 --bracken-length 150 \
  sample01.1.fq sample01.2.fq sample02.1.fq sample02.2.fq
```

Where:

* --sample-regex is the multisample regular expression (see above)
* --reads 100 is the taxon heuristic (see below)
* --bracken-length is the optional read length to use for building bracken weights for the dynamic library. 
If omitted, no weights will be built.
* k2 is a directory that contains library/ with the genomes that were used to build the static minimizer index. A subset 
of these will be used to build the dynamic index. 

The same example with the pre-built library:

```
./slacken.sh taxonIndex standard-224c/std_35_31_s7 classify -p \
 --sample-regex "(S[0-9]+)" -o test_class \
  dynamic --reads 100 -l standard-224c --bracken-length 150 \
  sample01.1.fq sample01.2.fq sample02.1.fq sample02.2.fq
```

#### Heuristics

Several different taxon selection heuristics are supported. If a taxon satisfied the given criterion 
(only one can be specified) using the static, pre-built index, then that taxon's genomic sequences will be included in 
the dynamic library used for the final classification. 

`--reads N`

This heuristic selects a taxon for inclusion using the regular Kraken 2 classification method. For example, 
with `--reads 100`, at least 100 reads have to classify as a given taxon for it to be included. The confidence score
for this heuristic can be set using `--read-confidence`.

`--min-count N`

This heuristic selects a taxon for inclusion if at least N minimizers from the taxon are present.

`--min-distinct N`

This heuristic selects a taxon for inclusion if at least N distinct minimizers from the taxon are present.

#### Dynamic library using a gold standard taxon set 

If a gold standard taxon set (e.g. from a ground truth mapping for the given samples) is available in `goldSet.txt`, 
a library can be built from those taxa during classification by supplying:

`--classify-with-gold -g goldSet.txt`

For example:

```
./slacken.sh taxonIndex mySlackenLib classify -p \
 --sample-regex "(S[0-9]+)" -o test_class \
  dynamic --classify-with-gold -g goldSet.txt -l k2 --bracken-length 150 \
  sample01.1.fq sample01.2.fq sample02.1.fq sample02.2.fq
```

If `-classify-with-gold` is not given but `-g` is, then the detected taxon set will be compared with the gold set.


### Running on AWS Elastic MapReduce or large clusters

Slacken can run on AWS EMR (Elastic MapReduce) and should also work similarly on other commercial cloud providers 
that support Apache Spark. In this scenario, data can be stored on AWS S3 and the computation can run on a mix of 
on-demand and spot (interruptible) instances. We refer the reader to the AWS EMR documentation for more details.
 
The cluster configuration we generally recommend is 4 GB RAM per CPU (but 2 GB per CPU may be enough for small workloads).
For large workloads, the worker nodes should have fast physical hard drives, such as NVMe. On EMR Spark will automatically use
these drives for temporary space. We have found the m7gd and m6gd machine families to work well. 

To run on AWS EMR, first, install the AWS CLI. 
Copy `slacken-aws.sh.template` to a new file, e.g. `slacken-aws.sh` and edit the file to configure
some settings such as the S3 bucket to use for the Slacken jar. Then, create the AWS EMR cluster. You will receive a 
cluster ID, either from the web GUI or from the CLI. Set the `AWS_EMR_CLUSTER` environment variable to this id:

```export AWS_EMR_CLUSTER=j-abc123...```

`slacken-aws.sh` may then be invoked in the same way as `slacken.sh` in the examples above, with the difference that 
instead of running Slacken locally, the script will create a step on your EMR cluster.

The files [scripts/slacken_pipeline.sh](scripts/slacken_pipeline.sh) and 
[scripts/slacken_steps_lib.sh](scripts/slacken_steps_lib.sh) contain preconfigured AWS pipelines and EMR steps, 
respectively.

If you are running an AWS EMR cluster in the us-east-1 region, you can access the pre-built standard library 
directly at `s3://slacken-sbi/library/standard-224c/std_35_31_s7`. Genomes for 2-step classification are available at
`s3://slacken-sbi/library/standard-224c`. If you are running in a different region, we recommend that you copy
these files to your cluster's region first for better performance.

## Technical details


### Building a library

#### Building from a new or existing Kraken 2 library

Slacken is compatible with the Kraken 2 build process. Genomes downloaded for Kraken 2 can also be used to
build a Slacken database, as long as `.fai` index files have also been generated (see below).

The build scripts from [Kraken 2](https://github.com/DerrickWood/kraken2) can automatically download the taxonomy and
genomes. For example, after installing kraken 2:

`kraken2-build --db k2 --download-taxonomy` downloads the taxonomy into the directory `k2/taxonomy`.

`kraken2-build --db k2 --download-library bacteria` downloads the bacterial library (large). Other libraries, e.g.
archaea, human, fungi, are also available.

For more help, see `kraken2-build --help`.

After the genomes have been downloaded and masked in this way, and `seqid2taxid.map` has been generated, it is necessary 
to generate faidx index files. This can be done using e.g. [seqkit](https://bioinf.shenwei.me/seqkit/):

`seqkit faidx k2/library/bacteria/library.fna`

This generates the index file `library.fna.fai`, which Slacken needs. This step must be repeated for every fasta/fna file
that will be indexed. If you already have a pre-existing Kraken 2 library with genomes, generating the faidx files is 
sufficient preparation.

Unlike with Kraken 2, the sequence files (`.fna`) are needed even after building the index to enable dynamic
classification. Deleting them to save space is not recommended (i.e., avoid running `kraken2-build --clean`).

#### Obtaining genomes with the Slacken build scripts

As a hopefully faster and more efficient alternative to kraken2-build, we have included modified and optimized versions 
of the Kraken 2 build scripts in [scripts/k2](scripts/k2) for downloading genomes and the taxonomy. They also have special
logic for building the "refseq prefer complete" library. Please refer to README.txt in that directory for more details.

#### Building the index

After the input files have been prepared as above, the index may be built:

```
./slacken.sh  -p 2000 -k 35 -m 31 -t k2/taxonomy  taxonIndex mySlackenLib \
  build -l k2 
```

Where:
* `-p 2000` is the number of partitions (should be larger for larger libraries. When tuning this, the aim is 50-100 MB 
per output file)
* `-k 35` is the k-mer (window) size
* `-m 31` is the minimizer size
* `mySlackenLib` is the location where the built library will be stored (a directory will be created or overwritten)
* `-t` is the directory where the NCBI taxonomy is stored (names.dmp, nodes.dmp, and merged.dmp, see above)
* `k2` is a directory containing seqid2taxid.map, which maps sequence IDs to taxa, and the subdirectory
  `library/` which will be scanned recursively for `*.fna` sequence files and their corresponding `*.fna.fai` index files

We have found that 2000 partitions is suitable for the standard library, and 30,000 is reasonable for a large library 
with 1.8 TB of FASTA input sequences.

More help: `./slacken.sh --help`

### Discrepancies between Slacken and Kraken 2

Given the same values of k and m, the same spaced seed mask, and the same genomic library and taxonomy, 
Slacken classifies reads as closely to Kraken 2 as possible. However, there are some sources of potential divergence between the two:

* We have found that Kraken 2 indexes extra minimizers after ambiguous regions in a genome. This means that the true value of k,
for Kraken 2, is between k and (k-1). Such extra minimizers give Kraken 2 a slightly larger minimizer database for the same parameters.
For the K2 standard library, we found that the difference is about 1% of the total minimizer count. If this is a concern, 
using (k-1) instead of k for Slacken is guaranteed to index at least as many minimizers as K2.

* Kraken 2 uses a probabilistic data structure called a compact hash table (CHT) which sometimes can lose information. 
Slacken does not have this and instead stores each record in full. This means that Slacken records, particularly when
the database contains a very large number of taxa, may be more precise.

* From the NCBI taxonomy, Kraken 2 currently reads only names.dmp and nodes.dmp, whereas Slacken also reads merged.dmp 
to correctly handle merged taxa. Canonical taxon IDs will be output in the results.

### Compiling

Prerequisites:

* Java 17 or later
* The [sbt](https://www.scala-sbt.org/) build tool.

To build the jar file: `sbt assembly`. The output will be in `target/scala-2.12/Slacken-assembly-0.1.0.jar`.

To just compile class files: `sbt compile`

To run tests: `sbt test`

### Citation

If you use Slacken in your research, please cite our paper:

Johan Nyström-Persson, Nishad Bapatdhar, and Samik Ghosh:
Precise and scalable metagenomic profiling with sample-tailored minimizer libraries.
bioRxiv 2024.12.22.629657; doi: https://doi.org/10.1101/2024.12.22.629657

## References

1. Wood, D.E., Lu, J. & Langmead, B. Improved metagenomic analysis with Kraken 2. Genome Biol 20, 257 (2019). https://doi.org/10.1186/s13059-019-1891-0
2. Lu J, Breitwieser FP, Thielen P, Salzberg SL. 2017. Bracken: estimating species abundance in metagenomics data. PeerJ Computer Science 3:e104 https://doi.org/10.7717/peerj-cs.104
