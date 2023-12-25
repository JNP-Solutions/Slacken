#!/bin/bash

ROOT=/home/johan/ws/jnps/Hypercut-git
BM=$ROOT/benchmarks
CHECK=$ROOT/benchmarks/check.sh

function run_small {
  $CHECK "" /fast/scratch/tara/ERR599052/10M.fastq $BM/tara_10M.fastq.expected || exit 1
  $CHECK "--method simple" /fast/scratch/SRR094926/10M.fasta $BM/cowrumen_10M.fasta.expected || exit 1
  $CHECK "--method pregrouped" /fast/scratch/SRR094926/10M.fasta $BM/cowrumen_10M.fasta.expected || exit 1
  $CHECK "--method simple --normalize"  /fast/scratch/SRR094926/10M.fasta $BM/cowrumen_10M.fasta.normalized.expected || exit 1
  $CHECK "--method pregrouped --normalize" /fast/scratch/SRR094926/10M.fasta $BM/cowrumen_10M.fasta.normalized.expected || exit 1
  $CHECK "" /fast/scratch/eDAL/Akashinriki_10M.fasta $BM/Akashinriki_10M.fasta.expected || exit 1
}

function run_large {
  #KMC3 disagrees (very slightly) with jellyfish about the following file.
  #Discount agrees with jellyfish,
  $CHECK "" /shiba/scratch/wheat/GCA_902810645.1.fasta $BM/GCA_902810645.1.fasta.expected || exit 1
  $CHECK "" /shiba/scratch/improver/ERR1620255.sra_1.fasta $BM/ERR1620255.sra_1.fasta.expected || exit 1
}

time case $1 in 
  small)
    run_small
    ;;
  large)
    run_large
    ;;
  *)
    run_small
    run_large
    ;;
esac

echo All OK
