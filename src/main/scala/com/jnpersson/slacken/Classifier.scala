/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */

package com.jnpersson.slacken

import com.jnpersson.kmers.minimizer.InputFragment
import com.jnpersson.kmers.{HDFSUtil, Inputs, SeqTitle}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{count, desc}

import java.util


/** A classified read.
 *
 * @param sampleId     The ID of the sample (if available)
 * @param classified   Could the read be classified?
 * @param title        Sequence title/ID
 * @param taxon        The assigned taxon
 * @param hits         The taxon hits (minimizers)
 * @param lengthString Length of the classified sequence
 * @param hitDetails   Human-readable details for the hits
 */
final case class ClassifiedRead(sampleId: String, classified: Boolean, title: SeqTitle, taxon: Taxon,
                                hits: Array[TaxonHit], lengthString: String, hitDetails: String) {
  def classifyFlag: String = if (!classified) "U" else "C"

  //Imitate the Kraken output format
  def outputLine: String = s"$classifyFlag\t$title\t$taxon\t$lengthString\t$hitDetails"
}

/** Parameters for classification of reads
 *
 * @param minHitGroups     min number of hit groups
 * @param withUnclassified whether to include unclassified reads in the output
 * @param thresholds       min. confidence scores (fraction of k-mers/minimizers that must be in the classified
 *                         taxon's clade)
 * @param sampleRegex      regular expression that identifies the sample ID of each read (for multi-sample mode).
 *                         e.g. ".*\\|(.*)\\|.*"
 *                         If none is specified, then single-sample mode is assumed.
 */
final case class ClassifyParams(minHitGroups: Int, withUnclassified: Boolean, thresholds: List[Double] = List(0.0),
                                sampleRegex: Option[String] = None)

/** Routines for classifying reads using a taxonomic k-mer LCA index.
 * @param index Minimizer-LCA index
 */
class Classifier(index: KeyValueIndex)(implicit spark: SparkSession) {
  import spark.sqlContext.implicits._

  /** Classify subject sequences using the index stored at the default location, optionally for multiple samples,
   * writing the results to a designated output location
   *
   * @param inputs         sequences to be classified
   * @param outputLocation (directory, if multi-sample or prefix, if single sample) to write output
   * @param cpar           classification parameters

   */
  def classifyAndWrite(inputs: Inputs, outputLocation: String, cpar: ClassifyParams): Unit = {
    val subjects = inputs.getInputFragments(withRC = false, withAmbiguous = true)
    val hits = index.collectHitsBySequence(subjects)
    classifyHitsAndWrite(hits, outputLocation, cpar)
  }

  def classify(subjects: Dataset[InputFragment], cpar: ClassifyParams, threshold: Double): Dataset[ClassifiedRead] = {
    val hits = index.collectHitsBySequence(subjects)
    classifyHits(hits, cpar, threshold)
  }

  /** Classify input sequence-hit dataset for a single sample and single confidence threshold value */
  def classifyHits(subjectsHits: Dataset[(SeqTitle, Array[TaxonHit])],
                   cpar: ClassifyParams, threshold: Double): Dataset[ClassifiedRead] = {
    val bcTax = index.bcTaxonomy
    val k = index.params.k
    val sre = cpar.sampleRegex.map(_.r)
    subjectsHits.map({ case (title, hits) =>
      val sortedHits = hits.sortBy(_.ordinal)

      val sample = sre match {
        case Some(re) => re.findFirstMatchIn(title).
          map(_.group(1)).getOrElse("other")
        case _ => "all"
      }

      Classifier.classify(bcTax.value, sample, title, sortedHits, threshold, k, cpar)
    })
  }

  /** Classify subject sequences using the given index, optionally for multiple samples,
   * writing the results to a designated output location
   *
   * @param subjectsHits       sequences to be classified
   * @param outputLocation location (directory, if multi-sample or prefix, if single sample) to write output
   * @param cpar           classification parameters
   */
  def classifyHitsAndWrite(subjectsHits: Dataset[(SeqTitle, Array[TaxonHit])], outputLocation: String,
                           cpar: ClassifyParams): Unit = {
    if (cpar.thresholds.size > 1) {
      subjectsHits.cache()
    }

    try {
      for {t <- cpar.thresholds} {
        val classified = classifyHits(subjectsHits, cpar, t)
        writeForSamples(classified, outputLocation, t, cpar)
      }
    } finally {
      subjectsHits.unpersist()
    }
  }

  /**
   * For each sample in the classified reads, write classified reads to a directory, with the _classified suffix,
   * as well as a kraken-style kreport.txt
   *
   * @param reads          classified reads
   * @param outputLocation directory/prefix to write to
   * @param threshold      the confidence threshold that was used in this classification
   * @param cpar           parameters for classification
   */
  private def writeForSamples(reads: Dataset[ClassifiedRead], outputLocation: String, threshold: Double, cpar: ClassifyParams): Unit = {
    val thresholds = cpar.thresholds
    // find the maximum number of digits after the decimal point for values in the threshold list
    // to enable proper sorting of file names with threshold values
    val maxDecimalLength = thresholds.map(num => num.toString.split("\\.")(1).length).max
    val thresholdStr = s"%.${maxDecimalLength}f".format(threshold)
    val location = outputLocation + "_c" + thresholdStr

    val keepLines = if (cpar.withUnclassified) {
      reads
    } else {
      reads.where($"classified" === true)
    }
    val outputRows = keepLines.map(r => (r.outputLine, r.sampleId)).
      toDF("classification", "sample")

    //These tables will be relatively small and we coalesce to avoid generating a lot of small files
    //in the case of an index with many partitions
    outputRows.coalesce(1000).write.mode(SaveMode.Overwrite).
      partitionBy("sample").
      option("compression", "gzip").
      text(s"${location}_classified")
    makeReportsFromClassifications(s"${location}_classified")
  }

  /** For each subdirectory (corresponding to a sample), read back written classifications
   * and produce a KrakenReport. */
  private def makeReportsFromClassifications(location: String): Unit = {
    //At this point we don't have the sample IDs, so we have to explicitly traverse the filesystem
    //and look for the data that we wrote in the previous step
    for { d <- HDFSUtil.subdirectories(location) } {
      val loc = s"$location/$d"
      println(s"Generating Kraken report for $loc")
      val report = reportFromWrittenClassifications(loc)
      val sampleId = d.replaceFirst("sample=", "")
      HDFSUtil.usingWriter(s"$location/${sampleId}_kreport.txt", wr => report.print(wr))
    }
  }

  /** Read back written classifications from writeOutput to produce a KrakenReport. */
  private def reportFromWrittenClassifications(location: String): KrakenReport = {
    val countByTaxon = spark.read.option("sep", "\t").csv(location).
      map(x => x.getString(2).toInt).toDF("taxon").
      groupBy("taxon").agg(count("*").as("count")).
      sort(desc("count")).as[(Taxon, Long)].collect()
    new KrakenReport(index.bcTaxonomy.value, countByTaxon)
  }

}

object Classifier {

  /** Classify a read.
   * @param taxonomy Parent map for taxa
   * @param title Sequence title/ID
   * @param sortedHits Taxon hits (minimizers) in order
   * @param confidenceThreshold Minimum fraction of k-mers/minimizers that must be in the match (KeyValueIndex only)
   * @param k Length of k-mers
   * @param cpar Classify parameters
   */
  def classify(taxonomy: Taxonomy, sampleId: String, title: SeqTitle, sortedHits: Array[TaxonHit],
               confidenceThreshold: Double, k: Int, cpar: ClassifyParams): ClassifiedRead = {
    val lca = new LowestCommonAncestor(taxonomy)

    val totalSummary = TaxonCounts.concatenate(sortedHits.map(_.summary))

    val taxon = lca.resolveTree(totalSummary, confidenceThreshold)
    val classified = taxon != Taxonomy.NONE && sufficientHitGroups(sortedHits, cpar.minHitGroups)

    val reportTaxon = if (classified) taxon else Taxonomy.NONE
    ClassifiedRead(sampleId, classified, title, reportTaxon, sortedHits,
      totalSummary.lengthString(k), totalSummary.pairsInOrderString)
  }

  /** For the given set of sorted hits, was there a sufficient number of hit groups wrt the given minimum? */
  def sufficientHitGroups(sortedHits: Seq[TaxonHit], minimum: Int): Boolean = {
    var hitCount = 0
    var lastMin = sortedHits(0).minimizer

    //count separate hit groups (adjacent but with different minimizers) for each sequence, imitating kraken2 classify.cc
    for { hit <- sortedHits } {
      if (hit.taxon != AMBIGUOUS_SPAN && hit.taxon != Taxonomy.NONE && hit.taxon != MATE_PAIR_BORDER &&
        (hitCount == 0 || !util.Arrays.equals(hit.minimizer, lastMin))) {
        hitCount += 1
      }
      lastMin = hit.minimizer
    }
    hitCount >= minimum
  }
}