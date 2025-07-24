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

import com.jnpersson.kmers.input.FileInputs
import com.jnpersson.kmers.minimizer.InputFragment
import com.jnpersson.kmers.{HDFSUtil, SeqTitle}
import it.unimi.dsi.fastutil.ints.{Int2IntArrayMap, Int2IntMap}
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
  def classifyAndWrite(inputs: Dataset[InputFragment], outputLocation: String, cpar: ClassifyParams): Unit = {
    if (!cpar.perReadOutput) {
      new SQLClassifier(index).classifyAndWrite(inputs, outputLocation, cpar)
    } else {
      val hits = index.collectHitsBySequence(inputs, cpar.perReadOutput)
      classifyHitsAndWrite(hits, outputLocation, cpar)
    }
  }

  def classify(subjects: Dataset[InputFragment], cpar: ClassifyParams, threshold: Double): Dataset[ClassifiedRead] = {
    if (!cpar.perReadOutput) {
      new SQLClassifier(index).classify(subjects, cpar, threshold)
    } else {
      val hits = index.collectHitsBySequence(subjects, cpar.perReadOutput)
      classifyHits(hits, cpar, threshold)
    }
  }

  /** Classify input sequence-hit dataset for a single sample and single confidence threshold value */
  def classifyHits(subjectsHits: Dataset[(SeqTitle, Array[TaxonHit], Long)],
                   cpar: ClassifyParams, threshold: Double): Dataset[ClassifiedRead] = {
    val bcTax = index.bcTaxonomy
    val k = index.params.k
    val sre = cpar.sampleRegex.map(_.r)

    subjectsHits.mapPartitions(part => {
      val lca = new LowestCommonAncestor(bcTax.value)

      part.map { case (title, hits, distinct) =>
        if (cpar.perReadOutput) {
          //The ordering of hits is not needed if we are not generating per read output
          java.util.Arrays.sort(hits, Classifier.hitsComparator)
        }

        val sample = sre match {
          case Some(re) => re.findFirstMatchIn(title).
            map(_.group(1)).getOrElse("other")
          case _ => "all"
        }

        Classifier.classify(lca, sample, title, hits, distinct, threshold, k, cpar)
      }
    })
  }

  /** Classify subject sequences using the given index, optionally for multiple samples,
   * writing the results to a designated output location
   *
   * @param subjectsHits       sequences to be classified
   * @param outputLocation location (directory, if multi-sample or prefix, if single sample) to write output
   * @param cpar           classification parameters
   */
  def classifyHitsAndWrite(subjectsHits: Dataset[(SeqTitle, Array[TaxonHit], Long)], outputLocation: String,
                           cpar: ClassifyParams): Unit = {
    if (cpar.thresholds.size > 1) {
      subjectsHits.cache()
    }

    try {
      for {t <- cpar.thresholds} {
        val classified = classifyHits(subjectsHits, cpar, t)
        writePerSampleOutput(classified, outputLocation, t, cpar)
      }
    } finally {
      subjectsHits.unpersist()
    }
  }

  /**
   * For each sample in the classified reads, write outputs.
   * If cpar.perReadOutput is true, classified reads with hit details will be written in a separate directory for each sample,
   * as well as a kraken-style report in kreport.txt.
   * If cpar.perReadOutput is false, only reports will be written.
   *
   * @param reads          classified reads
   * @param outputLocation directory/prefix to write to
   * @param threshold      the confidence threshold that was used in this classification
   * @param cpar           parameters for classification
   * @return               the sample IDs that were discovered and processed, or "all" if not using multi-sample mode
   */
  def writePerSampleOutput(reads: Dataset[ClassifiedRead], outputLocation: String, threshold: Double,
                           cpar: ClassifyParams): Iterable[String] = {
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

    if (cpar.perReadOutput) {
      //Write the classification of every read along with hit details
      val outputRows = keepLines.map(r => (r.outputLine, r.sampleId)).
        toDF("classification", "sample")

      //These tables will be relatively small. We coalesce to avoid generating a lot of small files
      //in the case of an index with many partitions
      outputRows.coalesce(1000).write.mode(SaveMode.Overwrite).
        partitionBy("sample").
        option("compression", "gzip").
        text(location)
      makeReportsFromClassifications(location)
    } else {
      //Write aggregate classification reports only

      val sampleIterator = keepLines.groupBy("sampleId", "taxon").agg(count("*").as("count"))
        .groupBy("sampleId").agg(collect_list(struct($"taxon".as("_1"), $"count".as("_2")))).
        as[(String, Array[(Taxon, Long)])].
        toLocalIterator().asScala

      val samples = for {
        (sample, countByTaxon) <- sampleIterator
        report = new KrakenReport(index.taxonomy, countByTaxon)
        loc = Classifier.reportOutputLocation(location, sample)
        _ = HDFSUtil.usingWriter(loc, wr => report.print(wr))
      } yield sample
      samples.toList
    }
  }

  /** For each subdirectory (corresponding to a sample), read back written classifications
   * and produce a KrakenReport.
   * @return A list of processed sample IDs
   */
  private def makeReportsFromClassifications(location: String): Iterable[String] =
    //At this point we don't have the sample IDs, so we have to explicitly traverse the filesystem
    //and look for the data that we wrote in the previous step
    for { d <- HDFSUtil.subdirectories(location)
      loc = s"$location/$d"
      _ = println(s"Generating report for $loc")
      report = reportFromWrittenClassifications(loc)
      sampleId = d.replaceFirst("sample=", "")
      _ = HDFSUtil.usingWriter(Classifier.reportOutputLocation(location, sampleId), wr => report.print(wr))
    } yield sampleId

  /** Read back written classifications from writeOutput to produce a KrakenReport. */
  private def reportFromWrittenClassifications(location: String): KrakenReport = {
    val countByTaxon = spark.read.option("sep", "\t").csv(location).
      select($"_c2".cast("int").as("taxon")).as[Taxon].
      groupBy("taxon").agg(count("*").as("count")).
      sort(desc("count")).as[(Taxon, Long)].collect()
    new KrakenReport(index.taxonomy, countByTaxon)
  }
}

/** Routines for classifying reads using a taxonomic k-mer LCA index.
 * This is an alternative implementation that prefers spark SQL over UDFs where possible.
 * Per-read output is not supported (only reports).
 * @param index Minimizer-LCA index
 */
class SQLClassifier(index: KeyValueIndex)(implicit spark: SparkSession) {
  import spark.implicits._

  def classify(subjects: Dataset[InputFragment], cpar: ClassifyParams, threshold: Double): Dataset[ClassifiedRead] = {
    val spans = index.getSpans(subjects, withTitle = true)
    val hits = index.spansToGroupedHitsCounted(spans)
    classifyHits(hits, cpar, threshold)
  }

  def classifyAndWrite(inputs: Dataset[InputFragment], outputLocation: String, cpar: ClassifyParams): Unit = {
    val spans = index.getSpans(inputs, withTitle = true)
    val hits = index.spansToGroupedHitsCounted(spans)
    classifyHitsAndWrite(hits, outputLocation, cpar)
  }

  /** Classify subject sequences using the given index, optionally for multiple samples,
   * writing the results to a designated output location
   *
   * @param subjectsHits       sequences to be classified
   * @param outputLocation location (directory, if multi-sample or prefix, if single sample) to write output
   * @param cpar           classification parameters
   */
  def classifyHitsAndWrite(subjectsHits: Dataset[(SeqTitle, Int, Array[(Taxon, Int)])], outputLocation: String,
                           cpar: ClassifyParams): Unit = {
    if (cpar.thresholds.size > 1) {
      subjectsHits.cache()
    }

    try {
      for {t <- cpar.thresholds} {
        val classified = classifyHits(subjectsHits, cpar, t)
        new Classifier(index).writePerSampleOutput(classified, outputLocation, t, cpar)
      }
    } finally {
      subjectsHits.unpersist()
    }
  }

  /** Classify input sequence-hit dataset for a single sample and single confidence threshold value */
  def classifyHits(subjectsHits: Dataset[(SeqTitle, Int, Array[(Taxon, Int)])],
                   cpar: ClassifyParams, threshold: Double): Dataset[ClassifiedRead] = {
    val bcTax = index.bcTaxonomy
    val k = index.params.k
    val sre = cpar.sampleRegex.map(_.r)
    assert(!cpar.perReadOutput) //not yet supported

    subjectsHits.mapPartitions(part => {
      val lca = new LowestCommonAncestor(bcTax.value)

      part.map { case (title, totalDistinct, hits) =>

        val sample = sre match {
          case Some(re) => re.findFirstMatchIn(title).
            map(_.group(1)).getOrElse("other")
          case _ => "all"
        }

        val hitMap = new Int2IntArrayMap(hits.length)
        var i = 0
        var totalKmers = 0
        while (i < hits.length) {
          val taxon = hits(i)._1
          val count = hits(i)._2
          if (taxon != MATE_PAIR_BORDER && taxon != AMBIGUOUS_SPAN)
            hitMap.put(taxon, count)
          if (taxon != MATE_PAIR_BORDER)
            totalKmers += count
          i += 1
        }

        Classifier.classifySimple(lca, sample, hitMap, totalKmers, totalDistinct, threshold, cpar)
      }
    })
  }

}

object Classifier {

  /** Location where per-read outputs are written for a given sample. The per-sample directories
   * are created when we partition the output dataframe by the sample column.
   */
  def perReadOutputsLocation(baseLocation: String, sampleId: String) =
    s"$baseLocation/sample=$sampleId"

  /** Location where the report is written for a given sample. */
  def reportOutputLocation(baseLocation: String, sampleId: String) =
    s"$baseLocation/${sampleId}_kreport.txt"

  /** Location where per-read outputs are written for a given report. */
  def perReadOutputsFromReportFile(reportName: String) = {
    val part2 = reportName.split("/")(1)
    val sample = part2.substring(0, part2.length - "_kreport.txt".length)
    perReadOutputsLocation(reportName.split("/")(0), sample)
  }

  /** Classify a read.
   * @param lca LCA calculator
   * @param title Sequence title/ID
   * @param sortedHits Taxon hits (minimizers) in order
   * @param confidenceThreshold Minimum fraction of k-mers/minimizers that must be in the match
   * @param k Length of k-mers
   * @param cpar Classify parameters
   */
  def classify(lca: LowestCommonAncestor, sampleId: String, title: SeqTitle, sortedHits: Array[TaxonHit],
               distinctHits: Long,
               confidenceThreshold: Double, k: Int, cpar: ClassifyParams): ClassifiedRead = {
    val totalSummary = TaxonCounts.fromHits(sortedHits)

    val taxon = lca.resolveTree(totalSummary, confidenceThreshold)
    val classified = taxon != Taxonomy.NONE && distinctHits >= cpar.minHitGroups

    val reportTaxon = if (classified) taxon else Taxonomy.NONE
    if (cpar.perReadOutput) {
      ClassifiedRead(sampleId, classified, title, reportTaxon, sortedHits,
        totalSummary.lengthString(k), totalSummary.pairsInOrderString)
    } else {
      ClassifiedRead(sampleId, classified, "", reportTaxon, Array.empty, "", "")
    }
  }

  //Simpler version that does not support per-read classification
  def classifySimple(lca: LowestCommonAncestor, sampleId: String, hitCounts: Int2IntMap,
                     totalKmers: Int,
               distinctHits: Int,
               confidenceThreshold: Double, cpar: ClassifyParams): ClassifiedRead = {
    val requiredScore = Math.ceil(confidenceThreshold * totalKmers)
    val taxon = lca.resolveTree(hitCounts, requiredScore)
    val classified = taxon != Taxonomy.NONE && distinctHits >= cpar.minHitGroups

    val reportTaxon = if (classified) taxon else Taxonomy.NONE
    ClassifiedRead(sampleId, classified, "", reportTaxon, Array.empty, "", "")
  }
  
  val hitsComparator = java.util.Comparator.comparingInt((hit: TaxonHit) => hit.ordinal)

}