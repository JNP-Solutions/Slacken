## Overview

Slacken implements metagenomic classification based on k-mers and minimizers. It can closely mimic the behaviour of 
[Kraken 2](https://github.com/DerrickWood/kraken2), while also supporting a wider parameter space and additional algorithms. 
In particular, it supports sample-tailored libraries, where the minimizer library is built on the fly as part of read classification.


Copyright (c) Johan Nystrom-Persson 2019-2024.

## Contents
1. [Basics](#basics)
  - [How it works](#how-it-works)
  - [Running Slacken](#running-slacken)
  - [Building a library](#building-a-library)
  - [Classifying reads (1-step)](#classifying-reads-1-step)
  - [Bracken weights](#bracken-weights)
  - [Classifying reads (2-step/dynamic)](#classifying-reads-2-stepdynamic-library)
  - [Running on AWS EMR](#running-on-aws-emr)
  - [License and support](#license-and-support)
2. [Technical details](#technical-details)
  - [Differences between Slacken and Kraken 2](#differences-between-slacken-and-kraken-2)
  - [Compiling Slacken](#compiling)
  - [Citation](#citation)
3. [References](#references)


## Basics

### How it works

Slacken classifies sequences (reads) according to k-mers and minimizers using the same algorithm as
Kraken 2. The end result is a classification for each read, as well as a summary report that shows the number of reads
and the fraction of reads assigned to each taxon. However, Slacken is based on Apache Spark and is thus a distributed application,
rather than run on a single machine as Kraken 2 does. Slacken can run on a single machine but it can
also scale to a cluster with hundreds or thousands of machines. It also does not keep all data in RAM but 
uses a combination of RAM and disk.

### Running Slacken

Minimal single-machine prerequisites: 
* [Spark](https://spark.apache.org/downloads.html) 3.5.0 or later (pre-built, for Scala 2.12 (i.e. not the Scala 2.13 version).) 
* 16 GB or more of RAM (32 GB or more recommended).
* A fast SSD drive is very helpful if running locally. The amount of space required depends on the size of the libraries.

The following environment variables should now be set:

* `SPARK_HOME` should point to the unzipped Spark download
* `SLACKEN_TMP` should point to a location for scratch space on a fast hard drive (SSD drives are essential to get good performance). Optionally,
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

In dynamic mode, first, a static minimizer library must be built, following the instructions above. This library is used
to sketch the taxon set in the sample/samples being classified. Next, a second minimizer library is built on the fly
and used to classify the reads for the final result. Using a library specifically tailored for the samples in this way
usually leads to more precise classifications.

### Heuristics

`--reads N`

This heuristic selects a taxon for inclusion using the regular Kraken 2 classification method. For example, 
with `--reads 100`, at least 100 reads have to classify as a given taxon for it to be included. The confidence score
for this heuristic can be set using `--read-confidence`.

`--min-count N`

This heuristic selects a taxon for inclusion if at least N minimizers from the taxon are present.

`--min-distinct N`

This heuristic selects a taxon for inclusion if at least N distinct minimizers from the taxon are present.


#### Bracken weights

Bracken weights for a dynamic library are automatically computed when the `--bracken-length` (read length) parameter is added.

### License and support

For any inquiries, please contact JNP Solutions at [info@jnpsolutions.io](mailto:info@jnpsolutions.io). We will do our
best to help. Alternatively, feel free to open issues and/or PRs in the GitHub repo if you find a problem.

Discount is currently released under a dual GPL/commercial license. For a commercial license, custom development, or
commercial support please contact us at the email above.

### Running on AWS EMR or large clusters

Slacken can run on AWS EMR (Elastic MapReduce) and should also work similarly on other commercial cloud providers 
that support Apache Spark. In this scenario, data can be stored on AWS S3 and the computation can run on a mix of 
on-demand and spot (interruptible) instances. We refer the reader to the AWS EMR documentation for more details.
 
The cluster configuration we generally recommend is 4 GB RAM per CPU (but 2 GB per CPU may be enough for small workloads).
For large workloads, the worker nodes should have fast physical hard drives, such as NVMe. On EMR Spark will automatically use
these drives for temporary space. Suitable machine types may be e.g. the m7gd and m6gd families. We recommend at least 16 CPUs
per machine.

To run on AWS EMR, first, install the AWS CLI. 
Copy `slacken-aws.sh.template` to a new file, e.g. `slacken-aws.sh` and edit the file to configure
some settings such as the S3 bucket to use for the Slacken jar. Then, create the AWS EMR cluster, and set its ID using
the `AWS_EMR_CLUSTER` environment variable. `slacken-aws.sh` may then be invoked in the same way as `slacken.sh` in the 
examples above.

The files [scripts/slacken_pipeline.sh](scripts/slacken_pipeline.sh) and 
[scripts/slacken_steps_lib.sh](scripts/slacken_steps_lib.sh) contain preconfigured AWS pipelines and EMR steps, 
respectively.

## Technical details

### Discrepancies between Slacken and Kraken 2

Given the same values of k and m, and given the same genomic library and taxonomy, Slacken classifies reads identically 
to Kraken 2. However, there are two sources of potential divergence between the two:

* We have found that Kraken 2 indexes extra minimizers after ambiguous regions in a genome. This means that the true value of k,
for Kraken 2, is between k and (k-1). Such extra minimizers give Kraken 2 a slightly larger minimizer database for the same parameters.
For the K2 standard library, we found that the difference is about 1% of the total minimizer count. If this is a concern, 
using (k-1) instead of k for Slacken is guaranteed to index at least as many minimizers as K2.

* Kraken 2 uses a probabilistic data structure called a compact hash table (CHT) which sometimes can lose information. 
Slacken does not have this and instead stores each record in full. This means that Slacken records, particularly when
the database contains a very large number of taxa, may be more precise.

### Compiling

Prerequisites:

* Java 17 or later
* The [sbt](https://www.scala-sbt.org/) build tool.

To build the jar file: `sbt assembly`. The output will be in `target/scala-2.12/Slacken-assembly-0.1.0.jar`.

To just compile class files: `sbt compile`

To run tests: `sbt test`

### Citation

## References