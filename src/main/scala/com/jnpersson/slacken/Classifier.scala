/*
 * This file is part of Slacken. Copyright (c) 2019-2025 Johan Nyström-Persson.
 *
 * Slacken is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 *  Slacken is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 * along with Slacken.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.jnpersson.slacken

import com.jnpersson.kmers.minimizer.InputFragment
import com.jnpersson.kmers.{HDFSUtil, Inputs, SeqTitle}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{collect_list, count, desc, struct}

import scala.collection.JavaConverters._


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
 *                         If none is specified, then single-sample mode is assumed. The first parenthesis group
 *                         in the regex is used to extract the sample ID.
 * @param perReadOutput    whether to output classification results, including hit groups, for every read. If false,
 *                         only reports are output.
 */
final case class ClassifyParams(minHitGroups: Int, withUnclassified: Boolean, thresholds: List[Double] = List(0.0),
                                sampleRegex: Option[String] = None, perReadOutput: Boolean = true)

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
    val subjects = inputs.getInputFragments(withAmbiguous = true)
    val hits = index.collectHitsBySequence(subjects, cpar.perReadOutput)
    classifyHitsAndWrite(hits, outputLocation, cpar)
  }

  def classify(subjects: Dataset[InputFragment], cpar: ClassifyParams, threshold: Double): Dataset[ClassifiedRead] = {
    val hits = index.collectHitsBySequence(subjects, cpar.perReadOutput)
    classifyHits(hits, cpar, threshold)
  }

  /** Classify input sequence-hit dataset for a single sample and single confidence threshold value */
  def classifyHits(subjectsHits: Dataset[(SeqTitle, Array[TaxonHit])],
                   cpar: ClassifyParams, threshold: Double): Dataset[ClassifiedRead] = {
    val bcTax = index.bcTaxonomy
    val k = index.params.k
    val sre = cpar.sampleRegex.map(_.r)

    subjectsHits.map({ case (title, hits) =>
      if (cpar.perReadOutput) {
        //The ordering of hits is not needed if we are not generating per read output
        java.util.Arrays.sort(hits, Classifier.hitsComparator)
      }

      val sample = sre match {
        case Some(re) => re.findFirstMatchIn(title).
          map(_.group(1)).getOrElse("other")
        case _ => "all"
      }

      Classifier.classify(bcTax.value, sample, title, hits, threshold, k, cpar)
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

    val classOutputLoc = s"${location}_classified"
    if (cpar.perReadOutput) {
      //Write the classification of every read along with hit details
      val outputRows = keepLines.map(r => (r.outputLine, r.sampleId)).
        toDF("classification", "sample")

      //These tables will be relatively small. We coalesce to avoid generating a lot of small files
      //in the case of an index with many partitions
      outputRows.coalesce(1000).write.mode(SaveMode.Overwrite).
        partitionBy("sample").
        option("compression", "gzip").
        text(classOutputLoc)
      makeReportsFromClassifications(classOutputLoc)
    } else {
      //Write aggregate classification reports only
      for {
        (sample, countByTaxon) <- keepLines.groupBy("sampleId", "taxon").agg(count("*").as("count"))
        .groupBy("sampleId").agg(collect_list(struct($"taxon".as("_1"), $"count".as("_2")))).
        as[(String,Array[(Taxon, Long)])].toLocalIterator().asScala
            } {
        val report = new KrakenReport(index.taxonomy, countByTaxon)
        val loc = s"$classOutputLoc/${sample}_kreport.txt"
        HDFSUtil.usingWriter(loc, wr => report.print(wr))
      }
    }
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
      select($"_c2".cast("int").as("taxon")).as[Taxon].
      groupBy("taxon").agg(count("*").as("count")).
      sort(desc("count")).as[(Taxon, Long)].collect()
    new KrakenReport(index.taxonomy, countByTaxon)
  }
}

object Classifier {

  /** Classify a read.
   * @param taxonomy Parent map for taxa
   * @param title Sequence title/ID
   * @param sortedHits Taxon hits (minimizers) in order
   * @param confidenceThreshold Minimum fraction of k-mers/minimizers that must be in the match
   * @param k Length of k-mers
   * @param cpar Classify parameters
   */
  def classify(taxonomy: Taxonomy, sampleId: String, title: SeqTitle, sortedHits: Array[TaxonHit],
               confidenceThreshold: Double, k: Int, cpar: ClassifyParams): ClassifiedRead = {
    val lca = new LowestCommonAncestor(taxonomy)

    val totalSummary = TaxonCounts.fromHits(sortedHits)

    val taxon = lca.resolveTree(totalSummary, confidenceThreshold)
    val classified = taxon != Taxonomy.NONE && sufficientHitGroups(sortedHits, cpar.minHitGroups)

    val reportTaxon = if (classified) taxon else Taxonomy.NONE
    if (cpar.perReadOutput) {
      ClassifiedRead(sampleId, classified, title, reportTaxon, sortedHits,
        totalSummary.lengthString(k), totalSummary.pairsInOrderString)
    } else {
      ClassifiedRead(sampleId, classified, "", reportTaxon, Array.empty, "", "")
    }
  }

  /** For the given set of sorted hits, was there a sufficient number of hit groups wrt the given minimum? */
  def sufficientHitGroups(hits: Array[TaxonHit], minimum: Int): Boolean = {
    var hitCount = 0

    //count separate hit groups (adjacent but with different minimizers) for each sequence, imitating kraken2 classify.cc
    var h = 0
    while (h < hits.length && hitCount < minimum) {
      val hit = hits(h)
      if (hit.taxon != Taxonomy.NONE && hit.distinct) {
        hitCount += 1
      }
      h += 1
    }
    hitCount >= minimum
  }

  val hitsComparator = java.util.Comparator.comparingInt((hit: TaxonHit) => hit.ordinal)

}