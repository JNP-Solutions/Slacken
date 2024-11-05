## Overview

Slacken implements metagenomic classification based on k-mers and minimizers. It can emulate the behaviour of 
[Kraken 2](https://github.com/DerrickWood/kraken2), while also supporting a wider parameter space and additional algorithms. In particular,
it supports sample-tailored libraries, where the minimizer library is built on the fly as part of read classification.


Copyright (c) Johan Nystrom-Persson 2019-2024.

## Contents
1. [Basics](#basics)
  - [How it works](#how-it-works)
  - [Running Slacken](#running-slacken)
  - [Building a library](#building-a-library)
  - [Classifying reads (1-step)](#classifying-reads-1-step)
  - [Bracken weights](#bracken-weights)
  - [Classifying reads (2-step/dynamic)](#classifying-reads-2-stepdynamic-library)
  - [License and support](#license-and-support)
2. [Technical details](#technical-details)
  - [Differences between Kraken 2 and Slacken](#differences)
  - [Compiling Discount](#compiling)
  - [Citation](#citation)
3. [References](#references)


## Basics

### How it works

Slacken basically classifies sequences according to k-mers and minimizers using the same algorithm as
Kraken 2. However, Slacken is based on Apache Spark and is thus a distributed application,
rather than run on a single machine as Kraken 2 does. Slacken can run on a single machine but it can
also scale to a cluster with hundreds or thousands of machines. It also does not keep all data in RAM but 
uses a combination of RAM and disk.

### Running Slacken

Prerequisites: 
* [Spark](https://spark.apache.org/downloads.html) 3.5.0 or later (pre-built, for Scala 2.12 (i.e. not the Scala 2.13 version).) 
* 16 GB or more of RAM (32 GB or more recommended).
* A fast SSD drive is very helpful if running locally. The amount of space required depends on the size of the libraries.

Copy `submit-slacken.sh.template` to `submit-slacken2.sh`. Edit this file. Set the path to the unzipped Spark installation.
Set the path to temporary disk space (e.g. an SSD drive and the maximum allowed memory). 
Change any other flags that may be necessary.

Check that it works: 
`./submit-slacken2.sh --help`

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

### Building the index

```
./submit-slacken2.sh  -p 2000 -k 35 -m 31 -t k2/taxonomy  taxonIndex mySlackenLib \
  build -l k2 
```

Where: 
* `-p 2000` is the number of partitions (should be larger for larger libraries)
* `-k 35` is the k-mer (window) size
* `-m 31` is the minimizer size
* `mySlackenLib` is the location where the built library will be stored (a directory will be created or overwritten)
* `-t` is the directory where the taxonomy is stored (names.dmp and nodes.dmp, see above)
* `k2` is a directory containing seqid2taxid.map, which maps sequence IDs to taxa, and the subdirectory 
  `library/` which will be scanned recursively for `*.fna` sequence files

While the build process is running, the Spark UI may be inspected at [http://localhost:4040](http://localhost:4040) if the process is running 
locally.

More help: `./submit-slacken2.sh --help`

#### Minimal demo data

For testing purposes, as an alternative to using kraken2-build above, a minimal demo library is available in Amazon S3 at `s3://jnp-public/slackenTestLib`.
This is a small random selection of bacterial genomes.

To build the demo library in the location `/tmp/library`:

```
./submit-slacken2.sh -k 35 -m 31 -t slackenTestLib/taxonomy taxonIndex /tmp/library \                                     
    build -l slackenTestLib 
```

### Classifying reads (1-step)


```
./submit-slacken2.sh taxonIndex mySlackenLib classify testData/SRR094926_10k.fasta \
  -o test_class
```

Where

* `mySlackenLib` is the location where the library was built in the previous step
* `SRR094926_10k.fasta` is the file with reads to be classified. Any number of files may be supplied.
* `-o test_class` is the directory where the output will be stored. A file `test_class_kreport.txt` will also be created.

To classify mate pairs, the `-p` flag may be used. Input files are then expected to be in alternating order:

```
./submit-slacken2.sh taxonIndex mySlackenLib classify -p sample01.1.fq sample01.2.fq \
  sample02.1.fq sample02.2.fq -o test_class
```

While the process is running, the Spark UI may be inspected at [http://localhost:4040](http://localhost:4040) if the process is running
locally.

More help: `./submit-slacken2.sh --help`

### Multi-sample mode

With multi-sample mode, Slacken can classify multiple samples at once. Separate outputs will be generated for each
distinct sample identified. This mode is more efficient than single sample classification and recommended whenever possible.

Samples are identified by means of a regular expression that needs to match each sequence (read) ID.

For example:

`--sample-regex "(S[0-9]+)"`

With this regular expression, a read with the ID `@S0R10/1` in a fastq file would be assigned to sample `S0`,
the ID `@S10R10/1` would be assigned to sample `S10`, and so on.

File structure is ignored as all reads from all files are pooled together during classification. The regular expression is the
only way that reads are mapped to samples.



### Computing bracken weights

Slacken can produce [Bracken](https://github.com/jenniferlu717/Bracken) weights, like those produced by `bracken-build`. 
They can be used directly with Bracken. (Bracken is an external tool developed by Jennifer Lu et al.)


### Classifying reads (2-step/dynamic library)

### Heuristics

#### Bracken weights

### License and support

For any inquiries, please contact JNP Solutions at [info@jnpsolutions.io](mailto:info@jnpsolutions.io). We will do our
best to help. Alternatively, feel free to open issues and/or PRs in the GitHub repo if you find a problem.

Discount is currently released under a dual GPL/commercial license. For a commercial license, custom development, or
commercial support please contact us at the email above.

## Technical details

### Compiling

Prerequisites:

* Java 17 or later
* The [sbt](https://www.scala-sbt.org/) build tool.

To build the jar file: `sbt assembly`. The output will be in `target/scala-2.12/Slacken-assembly-0.1.0.jar`.

To just compile class files: `sbt compile`

To run tests: `sbt test`

## References