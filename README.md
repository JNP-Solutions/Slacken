## Slacken

[![Build and test](https://github.com/JNP-Solutions/Slacken/actions/workflows/scala.yml/badge.svg)](https://github.com/jtnystrom/Discount/actions/workflows/ci.yml)
[![Docker Pulls](https://badgen.net/docker/pulls/jnpsolutions/slacken?icon=docker&label=pulls)](https://hub.docker.com/r/jnpsolutions/slacken/)
![GitHub License](https://img.shields.io/github/license/jnp-solutions/slacken)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.15469081.svg)](https://doi.org/10.5281/zenodo.15469081)


Slacken is a metagenomic profiler that classifies genomic sequences based on k-mers and minimizers. It implements the
[Kraken 2](https://github.com/DerrickWood/kraken2)[1] algorithm, while also supporting a wider parameter space and additional 
algorithms. In particular, it supports sample-tailored libraries, where the minimizer library is built on the fly as part 
of read classification. The end result is a classification for each read, as well as a summary report that shows the number of reads
and the fraction of reads assigned to each taxon. 

Slacken is based on Apache Spark and is thus a distributed application. It can run on a single machine, but can
also scale to a cluster with hundreds or thousands of machines. It does not keep all data in RAM during processing, but
processes data in batches. On a 16-core PC, Slacken needs only 16 GB of RAM to classify with the genomes from the Kraken 2 standard library.

Unfortunately, Slacken does not currently support translated mode (protein/AA sequence classification) but only nucleotide sequences. Also, 
Slacken has its own database format (Parquet based) and can not use pre-built Kraken 2 databases as they are.

For more motivation and details, please see [our 2025 paper in NAR Genomics and Bioinformatics](https://academic.oup.com/nargab/article/7/2/lqaf076/8158581).

Copyright (c) Johan Nyström-Persson 2019-2025.

## Contents
1. [Quick start using Docker on Linux](#quick-start-using-docker-on-linux)
2. [Common usage](#common-usage)
  - [Introduction](#introduction)
  - [Pre-built genomic libraries](#obtaining-a-pre-built-genomic-library)
  - [Classifying reads (1-step)](#classifying-reads-1-step)
  - [Multi-sample mode](#multi-sample-mode)
  - [Use with Bracken](#use-with-bracken)
  - [Classifying reads (2-step/dynamic)](#classifying-reads-using-a-dynamic-index-2-step-method)
  - [Troubleshooting](#troubleshooting)
3. [Technical details](#technical-details)
  - [Running with a Spark distribution](#running-with-a-spark-distribution)
-   [Running on AWS Elastic MapReduce or large clusters](#running-on-aws-elastic-mapreduce-or-large-clusters)
  - [Building a library](#building-a-library)
  - [Discrepancies between Slacken and Kraken 2](#discrepancies-between-slacken-and-kraken-2)
  - [Compiling](#compiling)
  - [Citation](#citation)
4. [References](#references)


## Quick start using Docker on Linux

Minimal single-machine prerequisites:
* 16 GB or more of free RAM (32 GB or more recommended).
* A fast SSD drive for temporary space. The amount of space required depends on the size of the 
libraries and samples, and the commands you want to run. At least 500 GB of space will be needed for this guide.
* We assume that you are running Linux and have Docker installed. We have tested with Docker v26.1. If you do not use Linux, or if you want to avoid Docker, please refer to
  [Running with a Spark distribution](#running-with-a-spark-distribution) below.
* If you want to run Bracken, we assume that you have it installed.

#### Download

The latest precompiled Slacken may be downloaded as a .zip from the
[Releases](https://github.com/JNP-Solutions/Slacken/releases). This is the easiest way to obtain Slacken. Download Slacken 2.0.0 and unzip the release.

```commandline
curl -LO https://github.com/JNP-Solutions/Slacken/releases/download/v2.0.0/Slacken-2.0.0.zip && \
unzip Slacken-2.0.0.zip
```
Also, pull the Docker image from [DockerHub](https://hub.docker.com/r/jnpsolutions/slacken):

```commandline
docker pull jnpsolutions/slacken:2.0.0
```

Set up the environment:

```commandline
#a writable location for data files and scratch space on a fast physical drive
export SLACKEN_DATA=/host/data/location

#Optional memory limit. The default is 16g, which we recommend as the minimum 
#for single-machine use. More is better.
export SLACKEN_MEMORY=32g
```

In order to extract the library, your data location should have at least 500 GB of free space. Please set the path as appropriate on your own system.

Check that Slacken can successfully run and display a help message: 
`./Slacken/dockerSlacken.sh --help`

These options may also be permanently configured by editing `dockerSlacken.sh`.

It may be helpful to put this script in your `$PATH`, e.g.:

```commandline
  export PATH=$PATH:$(pwd)/Slacken
```

Download the pre-built standard library to the previously specified data location.
After successful extraction, we delete the tar.gz to free up some space.

```commandline
cd /host/data/location
curl -LO https://s3.amazonaws.com/slacken/index/standard-224.tar.gz
tar xzf standard-224.tar.gz && rm standard-224.tar.gz
```

Download a CAMI2 sample for testing (or provide your own sample if you have one):

```commandline
curl -LO https://s3.amazonaws.com/slacken/sample/cami2/sample0/anonymous_reads.part_001.fq
curl -LO https://s3.amazonaws.com/slacken/sample/cami2/sample0/anonymous_reads.part_002.fq
```

Slacken currently does not support compressed (e.g. gz or bz2) input files. 

#### Perform read classification (1-step):

1-step classification corresponds to the regular Kraken 2 method:

```commandline
dockerSlacken.sh classify -i /data/standard-224c/std_35_31_s7 -p \
    -o /data/sample0 -c 0.15 \
    /data/anonymous_reads.part_001.fq /data/anonymous_reads.part_002.fq
```

In this command, `standard-224c/std_35_31_s7` is the location of the pre-built library. `-p` indicates that we wish to 
classify paired-end reads. 0.15 is the confidence threshold for classifying a read, as in Kraken 2.

If you get error messages about files not existing or not being readable, check that you put the files inside the SLACKEN_DATA 
directory that you specified above. Inside Docker, that will show up as /data. Depending on the file's location, symbolic
links might not work.

While Slacken is running, the Spark UI may be inspected at [http://localhost:4040](http://localhost:4040) if the process is running
locally. We refer users to the Spark documentation for more details.

When the command has finished, the following files will be generated:

* `sample0_c0.15_classified/all_kreport.txt` will be a Kraken-style report.
* `sample0_c0.15_classified/sample=all` is a directory with txt.gz files. These contain detailed classifications for each read.

In multi-sample mode (see below), instead of `all` we would see one report and one directory per sample ID.

If we wish, we can now run [Bracken](https://github.com/jenniferlu717/Bracken):

```commandline
bracken -d standard-224c/std_35_31_s7_bracken -r 150 -i sample0_c0.15_classified/all_kreport.txt  -o sample0_c0.15_classified/all_bracken
```

After Slacken has terminated, there will sometimes be some temporary files in `slacken_scratch` 
(in the location you specified as `SLACKEN_DATA`). They can be deleted.


#### Perform dynamic (2-step) classification, optionally with Bracken:

2-step classification first performs an initial classification to identify a taxon set, and then builds a second library 
on the fly. It then classifies all reads again using this second library.

2-step classification creates a lot of temporary files. To run this example, you need to have an additional 1 TB of free 
space in the data directory (or 250 GB without `--bracken-length`).

```commandline
dockerSlacken.sh classify2 -i /data/standard-224c/std_35_31_s7 \
 -o /data/sample0_R100 -c 0.15 -p \
  --reads 100 --bracken-length 150 \
  -l /data/standard-224c \
  /data/anonymous_reads.part_001.fq /data/anonymous_reads.part_002.fq 
```

Here, 

* `--reads 100` is the threshold for including a taxon in the initial set (R100).
* `-l /data/standard-224c` is required, and indicates where genomes for library building may be found.
* `--bracken-length 150` specifies that Bracken weights for the given read length (150) should be generated. That can be slow, and
also requires extra space, so we recommend omitting `--bracken-length` when Bracken is not needed. When generating Bracken weights, we recommend giving Slacken at least 32 GB of RAM.

When the command has finished, the following files will be generated:

* `sample0_R100_c0.15_classified/all_kreport.txt` will be a Kraken-style report.
* `sample0_R100_c0.15_classified/sample=all` is a directory with txt.gz files. These contain detailed classifications 
for each read.
* `sample0_R100_taxonSet.txt` is the set of taxa that was detected by the R100 heuristic and included in the second 
library.
* `sample0_R100/database150mers.kmer_distrib` will be generated if you ran with `--bracken-length 150`.

If we generated database150mers.kmer_distrib, we can now run Bracken using the following command:

```commandline
bracken -d sample0_R100 -r 150 -i sample0_R100_c0.15_classified/all_kreport.txt \
  -o sample0_R100_c0.15_classified/all_bracken
```

This concludes the quick start guide. If you have problems getting Slacken to work, we are happy to answer queries to 
the best of our ability. Feel free to open an issue in this repo, or email us directly (johan@jnpsolutions.io).

## Common usage

Below, we describe the most common commands in more detail. For convenience, the complete list of Slacken commands is
also available on its own [wiki page](https://github.com/JNP-Solutions/Slacken/wiki/Slacken-commands-overview).

In the following examples we are using `slacken.sh` to invoke Slacken. If you prefer to use the Docker version, 
please use `dockerSlacken.sh` instead.

### Obtaining a pre-built genomic library

We provide pre-built libraries in a public S3 bucket at s3://slacken. 
[This wiki page](https://github.com/JNP-Solutions/Slacken/wiki/Pre%E2%80%90built-Slacken-indexes-on-Amazon-S3) describes the available libraries.

For convenience, the standard index may also be downloaded as a compressed bundle from: https://s3.amazonaws.com/slacken/library/standard-224.tar.gz

The libraries are hosted in the us-east-1 region of AWS. When running Slacken on AWS Elastic MapReduce (EMR),
ideally in the same region,
these libraries may also be accessed directly from the public S3 bucket without downloading them. 


### Classifying reads (1-step)

The "1-step" classification corresponds to the standard Kraken 2 method. It classifies reads based on the pre-built library only.

```commandline
./slacken.sh classify -i /data/standard-224c/std_35_31_s7 -o test_class \
 sample1.fasta 
```

Where

* `/data/standard-224c/std_35_31_s7` is the location of a pre-built library.
*  `sample1.fasta` is the file with reads to be classified. Any number of files may be supplied.
* `test_class` is the directory where the output will be stored. Individual read classifications and a file 
`test_class_kreport.txt` will be created.

To classify mate pairs, the `-p` flag may be used. Input files are then expected to be paired up in sequence:

```commandline
./slacken.sh classify -i /data/standard-224c/std_35_31_s7 -p -o test_class \
  sample01.1.fq sample01.2.fq sample02.1.fq sample02.2.fq 
```

The accepted input formats for samples are fasta and fastq. Compressed files (gzip, bzip2) are not supported.

### Multi-sample mode

With multi-sample mode, Slacken can classify multiple samples at once. Separate outputs will be generated for each
distinct sample identified. This mode is more efficient than classifying samples one by one and recommended whenever possible.

Samples are identified by means of a regular expression that extracts the sample ID from each sequence header.

For example:

```commandline
./slacken.sh classify -i /data/standard-224c/std_35_31_s7 -p \
 --sample-regex "(S[0-9]+)" -o test_class \
  sample01.1.fq sample01.2.fq sample02.1.fq sample02.2.fq
```

With this regular expression, a read with the ID `@S0R5/1` in a fastq file would be assigned to sample `S0`,
the ID `@S2R5/1` would be assigned to sample `S2`, and so on. 

As another example, samples from the NCBI Sequencing Read Archive (SRA) might have headers of the form 
`@ERR234359.1`. Here, a regex like 

`--sample-regex "(.+)\."` 

would do the job. In this case the sample ID is `ERR234359`.

### Use with Bracken

Slacken can produce [Bracken](https://github.com/jenniferlu717/Bracken)[2] weights, like those produced by `bracken-build`. 
They can be used directly with Bracken to re-estimate taxon abundances in a taxon profiles and correct for database bias.
(Bracken is an external tool developed by Jennifer Lu et al. For more details, please refer to their paper and GitHub site.)

For example, for read length 150:

```commandline
./slacken.sh bracken-build -i /data/standard-224c/std_35_31_s7 \
 -l /data/standard-224c --read-len 150
```

Here, `-l` indicates the directory containing `library/` with the genomes that were used to build the index.

This will generate the file `/data/standard-224c/std_35_31_s7_bracken/database150mers.kmer_distrib`. Bracken can now
simply be invoked with `bracken -d /data/standard-224c/std_35_31_s7_bracken -r 150 ...`. 

### Classifying reads using a dynamic index (2-step method)

Slacken has the ability to build a dynamic minimizer library on the fly, which is tailored specifically to the samples being classified.
This "two-step method" usually leads to more precise classifications.

For this method, first, a static minimizer library must be built as usual, following the instructions above. This library is used
to sketch the taxon set in the sample/samples being classified by using a user-specified heuristic. During classification, a second minimizer library is built on the fly
and used to classify the reads for the final result. 

For example (100 reads heuristic):

```commandline
./slacken.sh classify2 -i /data/standard-224c/std_35_31_s7 -p \
  -o test_class \
  --reads 100 --library /data/standard-224c --bracken-length 150 \
  sample01.1.fq sample01.2.fq sample02.1.fq sample02.2.fq
```

Where:

* `--library` is required to specify the location of genomes. Here, standard-224c is a directory that contains library/ with the 
genomes that were used to build the static minimizer index. A subset of these will be used to build the dynamic index.
* `--reads 100` is the taxon heuristic (see below)
* `--bracken-length` is the optional read length to use for building bracken weights for the dynamic library. 
This is slow and requires a lot of temporary space. If omitted, no weights will be built (and can not be built later).

Dynamic classification also accepts other flags like `--sample-regex` (see above)

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

```commandline
./slacken.sh classify2 -i /data/standard-224c/std_35_31_s7 -p \
 -o test_class \
  --classify-with-gold -g goldSet.txt --library /data/standard-224c --bracken-length 150 \
  sample01.1.fq sample01.2.fq sample02.1.fq sample02.2.fq
```

If `-classify-with-gold` is not given but `-g` is, then the detected taxon set will be compared with the gold set.

### Troubleshooting

When getting error messages from Slacken/Spark/Java, it is helpful to locate the first error message that occurred. 
Often the root cause can be found there.

#### java.lang.ArrayIndexOutOfBoundsException: Index 3080012 out of bounds for length 3080008

Messages of this type may indicate that the taxonomy used is not compatible with either the library or the taxon set being 
used. Make sure you are not mixing two different taxonomies.

####  java.lang.OutOfMemoryError: Java heap space

Not enough memory. Increase the amount of memory available to Slacken via the SLACKEN_MEMORY variable (if you are 
running on a single machine. Otherwise, use the method appropriate to your cluster.)

#### java.io.IOException: Input path does not exist

If you are running Slacken with Docker, this error can occur when you are trying to access files outside the `/data`
directory. The Docker container can only see volumes that you have mounted, and also can only write to these locations. 
It is possible to supply multiple directories by passing additional `-v` arguments to Docker.

## Technical details


### Running with a Spark Distribution

Running with your own Spark distribution, without the Docker image, provides additional flexibility of configuration for
advanced users. This works on Linux, Mac and Windows but requires a little more configuration.
Unlike with the Docker image, Slacken would be able to access all files that you have access to, not just Docker mounted
volumes like `/data`.

Software dependencies:
* [Spark](https://spark.apache.org/downloads.html) 3.5.0 or later (pre-built, for Scala 2.12. The Scala 2.13 version is not compatible.) It is sufficient
  to download and extract the Spark distribution somewhere.
* As of December 2024, Spark supports Java 8/11/17. Java 21 is unsupported. Please refer to the Spark documentation.

Set up the environment:

```commandline
#path to the Spark distribution (where you extracted your Spark download)
export SPARK_HOME=/usr/local/spark-3.5.1-bin-hadoop3  

#a location for scratch space on a fast hard drive
export SLACKEN_TMP=/tmp

#Optional memory limit. The default is 16g, which we recommend as the minimum 
#for single-machine use. More is better.
export SLACKEN_MEMORY=32g
```

Check that it works:
`./slacken.sh --help`. 

These options may also be permanently configured by editing `slacken.sh`.



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

```commandline
export AWS_EMR_CLUSTER=j-abc123...
```

`slacken-aws.sh` may then be invoked in the same way as `slacken.sh` in the examples above, with the difference that
instead of running Slacken locally, the script will create a step on your EMR cluster.

The files [scripts/slacken_pipeline.sh](scripts/slacken_pipeline.sh) and
[scripts/slacken_steps_lib.sh](scripts/slacken_steps_lib.sh) contain preconfigured AWS pipelines and EMR steps,
respectively.


If you are running an AWS EMR cluster, you can access the pre-built standard library
directly at `s3://slacken/index/standard-224/idx_35_31_s7`. Genomes for 2-step classification are available at
`s3://slacken/index/standard-224`. If you are running in a different region than us-east-1, we recommend that you copy
these files to your cluster's region first for better performance.


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

```commandline
./slacken.sh -p 2000 build -i mySlackenLib -k 35 -m 31 -t k2/taxonomy \
  -l k2 
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

More help: `./slacken.sh --help` (or see the equivalent [wiki page](https://github.com/JNP-Solutions/Slacken/wiki/Slacken-commands-overview)).


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

We are happy to provide the code that we used for debugging Kraken 2 minimizer detection on request. 

### Compiling

Prerequisites:

* Java 17 or later
* The [sbt](https://www.scala-sbt.org/) build tool.

To build the jar file: `sbt assembly`. The resulting jar will be in `target/scala-2.12/Slacken-assembly-x.x.x.jar` for 
Slacken version x.x.x.

To just compile class files: `sbt compile`

To run tests: `sbt test`

To build the Docker image: `docker build -t jnpsolutions/slacken:latest`. 
(You would have to edit the `dockerSlacken.sh` script to run this image using the given tag.)

### Citation

If you use Slacken in your research, please cite our paper:

Johan Nyström-Persson, Nishad Bapatdhar, and Samik Ghosh:
Precise and scalable metagenomic profiling with sample-tailored minimizer libraries.
NAR Genomics and Bioinformatics, Volume 7, Issue 2, June 2025, lqaf076, https://doi.org/10.1093/nargab/lqaf076


## References

1. Wood, D.E., Lu, J. & Langmead, B. Improved metagenomic analysis with Kraken 2. Genome Biol 20, 257 (2019). https://doi.org/10.1186/s13059-019-1891-0
2. Lu J, Breitwieser FP, Thielen P, Salzberg SL. 2017. Bracken: estimating species abundance in metagenomics data. PeerJ Computer Science 3:e104 https://doi.org/10.7717/peerj-cs.104
