## Overview

Slacken implements metagenomic classification based on k-mers and minimizers. It can closely mimic the behaviour of 
[Kraken 2](https://github.com/DerrickWood/kraken2)[1], while also supporting a wider parameter space and additional algorithms. 
In particular, it supports sample-tailored libraries, where the minimizer library is built on the fly as part of read classification.


Copyright (c) Johan Nyström-Persson 2019-2024.

## Contents
1. [Basics](#basics)
  - [How it works](#how-it-works)
  - [Running Slacken](#running-slacken)
  - [Building a library](#building-a-library)
  - [Classifying reads (1-step)](#classifying-reads-1-step)
  - [Bracken weights](#bracken-weights)
  - [Classifying reads (2-step/dynamic)](#classifying-reads-2-stepdynamic-library)
  - [Running on AWS EMR](#running-on-aws-emr)
2. [Technical details](#technical-details)
  - [Differences between Slacken and Kraken 2](#differences-between-slacken-and-kraken-2)
  - [Compiling Slacken](#compiling)
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
* [Spark](https://spark.apache.org/downloads.html) 3.5.0 or later (pre-built, for Scala 2.12. The Scala 2.13 version is not compatible.) 
* 16 GB or more of RAM (32 GB or more recommended).
* A fast SSD drive for temporary space is very helpful. The amount of space required depends on the size of the libraries.

The following environment variables should now be set:

* `SPARK_HOME` should point to the unzipped Spark download
* `SLACKEN_TMP` should point to a location for scratch space on a fast hard drive
* `SLACKEN_MEMORY`, which defaults to `16g`, may optionally be configured.

Check that it works: 
`./slacken.sh --help`

These options may also be permanently configured by editing `slacken.sh`.

### Building a library

#### Obtaining reference genomes

Genomes compatible with the NCBI taxonomy are expected.

The build scripts from [Kraken 2](https://github.com/DerrickWood/kraken2) can automatically download the taxonomy and 
genomes. For example, after installing kraken 2:

`kraken2-build --db k2 --download-taxonomy` downloads the taxonomy into the directory `k2/taxonomy`.

`kraken2-build --db k2 --download-library bacteria` downloads the bacterial library (large). Other libraries, e.g. 
archaea, human, fungi, are also available.

For more help, see `kraken2-build --help`.

After the genomes have been downloaded, it is necessary to generate faidx index files. This can be done using e.g.
[seqkit](https://bioinf.shenwei.me/seqkit/):

`seqkit faidx k2/library/bacteria/library.fna`

This generates the index file `library.fna.fai`, which Slacken needs. This step must be repeated for every fasta/fna file
that will be indexed.

Unlike with Kraken 2, genome sequence files may be needed even after index construction, for example during dynamic 
classification. Deleting them to save space is not recommended.

As an alternative build process, we have included modified and optimised versions of the Kraken 2 build scripts in `scripts/k2`.
Please refer to README.txt in that directory for more details.


### Building the index

```
./slacken.sh  -p 2000 -k 35 -m 31 -t k2/taxonomy  taxonIndex mySlackenLib \
  build -l k2 
```

Where: 
* `-p 2000` is the number of partitions (should be larger for larger libraries, the aim is 50-100 MB per output file)
* `-k 35` is the k-mer (window) size
* `-m 31` is the minimizer size
* `mySlackenLib` is the location where the built library will be stored (a directory will be created or overwritten)
* `-t` is the directory where the taxonomy is stored (names.dmp and nodes.dmp, see above)
* `k2` is a directory containing seqid2taxid.map, which maps sequence IDs to taxa, and the subdirectory 
  `library/` which will be scanned recursively for `*.fna` sequence files

While the build process is running, the Spark UI may be inspected at [http://localhost:4040](http://localhost:4040) if the process is running 
locally.

More help: `./slacken.sh --help`

#### Minimal demo data

For technical testing purposes, as an alternative to using kraken2-build above, a minimal demo library is available in Amazon S3 at `s3://jnp-public/slackenTestLib`.
This is a small random selection of bacterial genomes.

To build the demo library in the location `/tmp/mySlackenLib`, after downloading the genomes to `slackenTestLib`:

```
./slacken.sh -k 35 -m 31 -t slackenTestLib/taxonomy taxonIndex /tmp/mySlackenLib \                                     
    build -l slackenTestLib 
```

### Classifying reads (1-step)

The "1-step" classification corresponds to the standard Kraken 2 method. It classifies reads based on the pre-built library only.

```
./slacken.sh taxonIndex mySlackenLib classify testData/SRR094926_10k.fasta \
  -o test_class
```

Where

* `mySlackenLib` is the location where the library was built in the previous step
* `SRR094926_10k.fasta` is the file with reads to be classified. Any number of files may be supplied.
* `test_class` is the directory where the output will be stored. Individual read classifications and a file 
`test_class_kreport.txt` will be created.

To classify mate pairs, the `-p` flag may be used. Input files are then expected to be in alternating order:

```
./slacken.sh taxonIndex mySlackenLib classify -p sample01.1.fq sample01.2.fq \
  sample02.1.fq sample02.2.fq -o test_class
```

While the process is running, the Spark UI may be inspected at [http://localhost:4040](http://localhost:4040) if the process is running
locally.

More help: `./slacken.sh --help`

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

### Computing bracken weights

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

* --reads 100 is the taxon heuristic (see below)
* --bracken-length is the optional read length to use for building bracken weights for the dynamic library. 
If omitted, no weights will be built.
* k2 is a directory that contains library/ with the genomes that were used to build the static minimizer index. A subset 
of these will be used to build the dynamic index.

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


### Running on AWS EMR or large clusters

Slacken can run on AWS EMR (Elastic MapReduce) and should also work similarly on other commercial cloud providers 
that support Apache Spark. In this scenario, data can be stored on AWS S3 and the computation can run on a mix of 
on-demand and spot (interruptible) instances. We refer the reader to the AWS EMR documentation for more details.
 
The cluster configuration we generally recommend is 4 GB RAM per CPU (but 2 GB per CPU may be enough for small workloads).
For large workloads, the worker nodes should have fast physical hard drives, such as NVMe. On EMR Spark will automatically use
these drives for temporary space. We have found the m7gd and m6gd machine families to work well. 

To run on AWS EMR, first, install the AWS CLI. 
Copy `slacken-aws.sh.template` to a new file, e.g. `slacken-aws.sh` and edit the file to configure
some settings such as the S3 bucket to use for thie Slacken jar. Then, create the AWS EMR cluster, and set its ID using
the `AWS_EMR_CLUSTER` environment variable. `slacken-aws.sh` may then be invoked in the same way as `slacken.sh` in the 
examples above.

The files [scripts/slacken_pipeline.sh](scripts/slacken_pipeline.sh) and 
[scripts/slacken_steps_lib.sh](scripts/slacken_steps_lib.sh) contain preconfigured AWS pipelines and EMR steps, 
respectively.

## Technical details

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

## References

1. Wood, D.E., Lu, J. & Langmead, B. Improved metagenomic analysis with Kraken 2. Genome Biol 20, 257 (2019). https://doi.org/10.1186/s13059-019-1891-0
2. Lu J, Breitwieser FP, Thielen P, Salzberg SL. 2017. Bracken: estimating species abundance in metagenomics data. PeerJ Computer Science 3:e104 https://doi.org/10.7717/peerj-cs.104
