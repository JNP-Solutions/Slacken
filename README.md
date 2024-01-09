## Overview

Slacken implements metagenomic classification based on k-mers and minimizers. It can emulate the behaviour of 
Kraken 1 and Kraken 2, while also supporting a wider parameter space and additional algorithms.

Copyright (c) Johan Nystrom-Persson 2019-2024.

## Compiling

Prerequisites:

* Java 8 or later (if using Java 8, `build.sbt` may need to be edited if tests won't run)
* The [sbt](https://www.scala-sbt.org/) build tool.

To build the jar file: `sbt assembly`. The output will be in `target/scala-2.12/Slacken-assembly-0.1.0.jar`.

To just compile class files: `sbt compile`

To run tests: `sbt test`

Also useful: `sbt clean`

As a development environment, [IntelliJ](https://www.jetbrains.com/idea/) community edition with Scala support is 
recommended (but any decent Scala IDE should do).

## Running

Prerequisites: 
* [Spark](https://spark.apache.org/downloads.html) 3.3.0 or later (pre-built, for Scala 2.12 (i.e. not the Scala 2.13 version).) 
* 16 GB or more of RAM (32 GB or more recommended).
* A fast SSD drive is very helpful if running locally. The amount of space required depends on the size of the libraries.

Copy `submit-slacken.sh.template` to `submit-slacken2.sh`. Edit this file. Set the path to the unzipped Spark installation.
Set the path to temporary disk space (e.g. an SSD drive and the maximum allowed memory). 
Change any other flags that may be necessary.

Check that it works: 
`./submit-slacken2.sh --help`

## Building a library

### Obtaining reference genomes

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
./submit-slacken2.sh  -p 2000 -k 35 -m 31  taxonIndex -l mySlackenLib \
  -t k2/taxonomy  build -l k2/seqid2taxid.map  k2/library/bacteria/library.fna
```

Where: 
* `-p 2000` is the number of partitions (should be larger for larger libraries)
* `-k 35` is the k-mer (window) size
* `-m 31` is the minimizer size
* `-l mySlackenLib` is the location where the built library will be stored (a directory will be created or overwritten)
* `-t` is the directory where the taxonomy is stored (names.dmp and nodes.dmp, see above)
* `-l` is the label file mapping sequence IDs to taxa (obtained by the download step above)
* `...library.fna` is the file to be indexed (any number of files may be supplied, separated by space)


While the build process is running, the Spark UI may be inspected at [http://localhost:4040](http://localhost:4040) if the process is running 
locally.

More help: `./submit-slacken2.sh --help`
## Classifying reads


```
./submit-slacken2.sh taxonIndex -l mySlackenLib -t k2/taxonomy classify testData/SRR094926_10k.fasta \
  -o test_class
```

Where

* `-l mySlackenLib` is the location where the library was built in the previous step
* `-t k2/taxonomy` is the directory where the taxonomy is stored
* `SRR094926_10k.fasta` is the file with reads to be classified. Any number of files may be supplied.
* `-o test_class` is the directory where the output will be stored. A file `test_class_kreport.txt` will also be created.

To classify mate pairs, the `-p` flag may be used. Input files are then expected to be in alternating order:

```
./submit-slacken2.sh taxonIndex -l mySlackenLib -t k2/taxonomy classify -p sample01.1.fq sample01.2.fq \
  sample02.1.fq sample02.2.fq -o test_class
```

While the process is running, the Spark UI may be inspected at [http://localhost:4040](http://localhost:4040) if the process is running
locally.

More help: `./submit-slacken2.sh --help`