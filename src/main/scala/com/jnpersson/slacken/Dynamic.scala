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

import com.jnpersson.kmers.minimizer._
import com.jnpersson.kmers.Helpers.formatPerc
import com.jnpersson.kmers.{HDFSUtil, Inputs}
import com.jnpersson.slacken.Taxonomy.Rank
import org.apache.spark.sql.functions.{count, udf, concat_ws}
import org.apache.spark.sql.{DataFrame, SaveMode, Dataset, RelationalGroupedDataset, SparkSession, functions}

import scala.collection.mutable

sealed trait TaxonCriteria

/** Criterion that includes taxa having a minimum number of total minimizer hits in the sample. */
final case class MinimizerTotalCount(threshold: Int) extends TaxonCriteria

/** Criterion that includes taxa having a minimum number of distinct minimizer hits in the sample. */
final case class MinimizerDistinctCount(threshold: Int) extends TaxonCriteria

/** Criterion that includes taxa having a minimum number of classified reads in the sample, using the given
 * confidence threshold. */
final case class ClassifiedReadCount(threshold: Int, confidence: Double) extends TaxonCriteria

final case class MinimizerFraction(threshold: Double) extends TaxonCriteria

/** Helper for timing tasks */
final case class Timer(task: String, start: Long) {
  def finish(): Unit = {
    val elapsed = System.currentTimeMillis() - start
    val s = elapsed / 1000
    val min = s / 60
    val rem = s % 60
    println(s"Finish task: $task [$min min $rem s]")
  }
}

/** Parameters for a user-supplied dynamic classification gold taxon set.
 * @param taxonFile Path to the text file to read taxa from (one per line)
 * @param promoteRank if supplied, taxa from the gold set that have no minimizers in the database will be
 * promoted to this rank at the highest, e.g. Genus
 * @param classifyWith whether to classify using the gold set library, or just compare a detected taxon set with it
 */
final case class GoldSetOptions(taxonFile: String, promoteRank: Option[Rank], classifyWith: Boolean)

/** Two-step classification of reads with dynamically generated indexes,
 * starting from a base index.
 * First, a set of taxa will be identified from the sample (reads). Then these taxa will be used
 * to construct a second (dynamic) taxonomic index for classifying the reads.
 * A bracken-style weights file describing the dynamic index will optionally also be generated.
 *
 * @param base                     initial index for identifying taxa by minimizer
 * @param genomes                  genomic library for construction of new indexes on the fly
 * @param reclassifyRank           rank for the initial classification. Taxa at this level will be used to construct the second index
 * @param taxonCriteria            criteria for selecting taxa for inclusion in the dynamic index
 * @param cpar                     parameters for classification
 * @param goldSetOpts              parameters for deciding whether to get stats or classify wrt gold standard
 * @param outputLocation           prefix location for output files
 */
class Dynamic(base: KeyValueIndex, genomes: GenomeLibrary,
              reclassifyRank: Rank,
              taxonCriteria: TaxonCriteria,
              cpar: ClassifyParams,
              goldSetOpts: Option[GoldSetOptions],
              outputLocation: String)(implicit spark: SparkSession) {

  import spark.sqlContext.implicits._

  def taxonomy: Taxonomy = base.taxonomy

  /** Report the time taken by subtasks. */
  def startTimer(task: String): Timer = {
    println(s"Start task: $task")
    Timer(task, System.currentTimeMillis())
  }

  private def minimizersInSubjects(subjects: Dataset[InputFragment]): RelationalGroupedDataset = {
    val hits = base.findHitsWithMinimizers(subjects)

    val bcTax = base.bcTaxonomy
    val rank = reclassifyRank

     hits.flatMap { case (hit, min) =>
       for {t <- hit.trueTaxon
            if bcTax.value.depth(t) >= rank.depth
            } yield (t, min)
     }.
      toDF("taxon", "minimizer").groupBy("taxon")
  }

  /** Counting method that counts the number of distinct minimizers per taxon in the sample,
   * to aid taxon set filtering */
  def distinctMinimizersPerTaxon(subjects: Dataset[InputFragment]): Array[(Taxon, Long)] =
    minimizersInSubjects(subjects).agg(functions.count_distinct($"minimizer").as("count")).
      as[(Taxon, Long)].collect()

  def totalMinimizersPerTaxon(subjects: Dataset[InputFragment]): Array[(Taxon, Long)] =
    minimizersInSubjects(subjects).agg(functions.count($"minimizer").as("count")).
      as[(Taxon, Long)].collect()

  /** Counting method that counts the fraction of distinct minimizers per taxon seen in the sample,
   * to aid taxon set filtering. */
  def minimizerFractionPerTaxon(subjects: Dataset[InputFragment]): Array[(Taxon, Double)] = {
    val inSample = distinctMinimizersPerTaxon(subjects).
      toMap
    val inRecords = base.distinctMinimizersPerTaxon(inSample.keys.toSeq).
      toMap

    inSample.keys.toArray.map(t => (t, inSample(t).toDouble / inRecords(t).toDouble))
  }

  /** Counting method that counts the number of minimizers per taxon, in the records, to aid taxon set filtering */
  def minimizersPerTaxon(taxa: Seq[Taxon]): Array[(Taxon, Long)] =
    base.distinctMinimizersPerTaxon(taxa)

  /** Counting method that counts the number of reads classified per taxon to aid taxon set filtering */
  def classifiedReadsPerTaxon(subjects: Dataset[InputFragment], confidenceThreshold: Double): Array[(Taxon, Long)] = {
    val cls = new Classifier(base)
    val classified = cls.classify(subjects, cpar, confidenceThreshold)
    classified.where($"classified" === true).
      select("taxon").
      groupBy("taxon").agg(count("*")).as[(Taxon, Long)].
      collect()
  }

  /** Calculate per-taxon statistics for the index support report
   *
   * @param subjects reads to collect stats from
   * @return For each taxon, a set of aggregated statistics:
   *         1. total k-mer and minimizer counts,
   *         2. classified read counts,
   *         3. taxon minimizer depth distribution stats
   */
  private def multiStatsPerTaxon(subjects: Dataset[InputFragment]): (Dataset[(Taxon, Long, Long, Long)],
    Dataset[(Taxon, Long)], Dataset[(Taxon, String, String)]) = {
    val initThreshold = 0.0
    val indexStats = new IndexStatistics(base)
    val coveragePerTaxon = indexStats.showTaxonFullCoverageStats(genomes)

    val foundHits = base.findHits(subjects)
    val cls = new Classifier(base)
    val classified = cls.classify(subjects, cpar, initThreshold)
      .where($"classified" === true)
      .groupBy("taxon").agg(count("*").as("classifiedReadCount")).as[(Taxon, Long)]

    val bcTax = base.bcTaxonomy
    val rank = reclassifyRank

    val passDepth = udf((t: Taxon) =>
      t != AMBIGUOUS_SPAN && t != MATE_PAIR_BORDER &&
        bcTax.value.depth(t) >= rank.depth
    )
    val grouped = foundHits.where(passDepth($"taxon")).select($"taxon", $"count", $"minimizer")
      .toDF("taxon", "kmerCount", "distinctMinimizer").groupBy("taxon")

    (grouped.agg(functions.sum($"kmerCount").as("totalKmerCount")
      , functions.countDistinct($"distinctMinimizer").as("distinctMinimizerCount")
      , functions.count($"*").as("totalMinimizerCount"))
      .select("taxon","totalKmerCount", "distinctMinimizerCount", "totalMinimizerCount")
      .as[(Taxon, Long, Long, Long)].cache(), classified,
      coveragePerTaxon)
  }

  /** A method for identifying a taxon set in a set of reads. */
  trait TaxonSetFinder {

    /** The identified taxa */
    def taxa: mutable.BitSet
  }

  /** Simple count filter that finds a taxon set by capping at a minimum count threshold.
   */
  class CountFilter(counts: Array[(Taxon, Long)], threshold: Int) extends TaxonSetFinder {
    val hitMinimizers = new TreeAggregator(taxonomy, counts)

    def taxa: mutable.BitSet =
      mutable.BitSet.empty ++
        (for {taxon <- hitMinimizers.keys
              if taxonomy.depth(taxon) >= reclassifyRank.depth
              if hitMinimizers.cladeTotals(taxon) >= threshold
              }
        yield taxon)
  }

  /**
   * For a given set of reads, report various per-taxon statistics (which would be used to support
   * taxon selection for dynamic index construction).
   * Reports are written to various files and directories prefixed by the output location.
   * Slow.
   * @param subjects reads
   */
  def reportDynamicIndexSupport(subjects: Dataset[InputFragment]): Unit = {
    val statCollection = multiStatsPerTaxon(subjects)
    val totalKmerCounter = new KrakenReport(taxonomy,statCollection._1
      .select("taxon","totalKmerCount").as[(Taxon,Long)].collect())
    val distinctMinimizerCounter = new KrakenReport(taxonomy, statCollection._1
      .select("taxon","distinctMinimizerCount").as[(Taxon,Long)].collect())
    val totalMinimizerCounter = new KrakenReport(taxonomy, statCollection._1
      .select("taxon","totalMinimizerCount").as[(Taxon,Long)].collect())
    val classifiedReadCounter = new KrakenReport(taxonomy, statCollection._2
      .select("taxon","classifiedReadCount").as[(Taxon,Long)].collect())

    HDFSUtil.usingWriter(outputLocation + "_support_report_totalKmerCount.txt",
      wr => totalKmerCounter.print(wr))
    HDFSUtil.usingWriter(outputLocation + "_support_report_distinctMinimizerCount.txt",
      wr => distinctMinimizerCounter.print(wr))
    HDFSUtil.usingWriter(outputLocation + "_support_report_totalMinimizerCount.txt",
      wr => totalMinimizerCounter.print(wr))
    HDFSUtil.usingWriter(outputLocation + "_support_report_classifiedReadCount.txt",
      wr => classifiedReadCounter.print(wr))

    val minimizerCoverage = statCollection._3.cache()

    try {
      minimizerCoverage
        .select(concat_ws("  ", $"taxon".cast("string"), $"minimizerCoverage"))
        .write.format("text").mode(SaveMode.Overwrite)
        .save(outputLocation + "_support_report_minimizerCoverage")

      minimizerCoverage
        .select(concat_ws("  ", $"taxon".cast("string"), $"distinctMinimizerCoverage"))
        .write.format("text").mode(SaveMode.Overwrite)
        .save(outputLocation + "_support_report_minimizerDistinctCoverage")
    } finally {
      minimizerCoverage.unpersist()
    }
  }

  /** Find an estimated taxon set in the given reads (to be classified),
   * emphasising recall over precision.
   */
  def findTaxonSet(subjects: Dataset[InputFragment], writeLocation: Option[String]): mutable.BitSet = {
    val t = startTimer("Find taxon set in subjects")

    val finder = taxonCriteria match {
      case MinimizerTotalCount(threshold) => new CountFilter(totalMinimizersPerTaxon(subjects), threshold)
      case MinimizerFraction(threshold) => ???
      case ClassifiedReadCount(threshold, confidence) => new CountFilter(classifiedReadsPerTaxon(subjects, confidence), threshold)
      case MinimizerDistinctCount(threshold) => new CountFilter(distinctMinimizersPerTaxon(subjects), threshold)
    }

    val keepTaxa = finder.taxa

    for {loc <- writeLocation}
      HDFSUtil.writeTextLines(loc, keepTaxa.iterator.map(_.toString))

    for { gs <- goldSetOpts} {
        val goldSet = readGoldSet(gs)
        val tp = keepTaxa.intersect(goldSet).size
        val fp = (keepTaxa -- keepTaxa.intersect(goldSet)).size
        val fn = (goldSet -- keepTaxa.intersect(goldSet)).size
        val precision = tp.toDouble / (tp + fp)
        val recall = tp.toDouble / goldSet.size
        println(s"Comparing detected set with supplied gold set. True Positives: $tp, False Positives: $fp, False Negatives: $fn, " +
          s"Precision: ${formatPerc(precision)}, Recall: ${formatPerc(recall)}")
    }

    val withDescendants = taxonomy.taxaWithDescendants(keepTaxa)
    t.finish()
    println(s"Detected set: Initial scan (criterion $taxonCriteria) produced ${keepTaxa.size} taxa at rank $reclassifyRank, expanded with descendants to ${withDescendants.size}")
    withDescendants
  }

  private lazy val taxonSetInLibrary = genomes.taxonSet(taxonomy)

  def readGoldSet(goldSetOpts: GoldSetOptions): mutable.BitSet = {
    val bcTax = base.bcTaxonomy
    val goldSet = mutable.BitSet.empty ++
      spark.read.csv(goldSetOpts.taxonFile).map(x => bcTax.value.primary(x.getString(0).toInt)).collect()

    println(s"Gold set contained ${goldSet.size} taxa")
    val notFound = goldSet -- taxonSetInLibrary

    val promoted = notFound.flatMap(t => {
      val path = taxonomy.pathToRoot(t).filter(taxonSetInLibrary.contains)
      if (path.hasNext) Some(path.next()) else None
    })
    println(s"${notFound.size} taxa from gold set not found in library, promoted to ${promoted.size} taxa.")
    val levelCounts = promoted.toSeq.map(t => taxonomy.depth(t)).groupBy(x => x).map(x =>
      (Taxonomy.rankForDepth(x._1).orNull, x._2.size)).toSeq.sorted
    println(s"Promoted to levels: $levelCounts")

    val keptPromoted = goldSetOpts.promoteRank match {
      case Some(r) =>
        val kept = promoted.filter(t => taxonomy.depth(t) >= r.depth)
        println(s"Keeping ${kept.size} taxa at rank $r and below from promoted set")
        kept
      case None => List()
    }
    val total = goldSet ++ promoted
    val filtered = total.filter(taxonomy.depth(_) >= reclassifyRank.depth) ++ keptPromoted
    println(s"Initial adjusted gold set size ${total.size}, filtered at $reclassifyRank to ${filtered.size}")
    filtered
  }

  /** Perform two-step classification, writing the final results to a location.
   *
   * @param inputs         Subjects to classify (reads)
   * @param partitions     Number of partitions for the dynamically generated index in step 2
   * @param dynamicReports whether to generate reports describing the dynamic index
   * @param dynamicBrackenReadLength read length for generating bracken weights for the second index (if any)
   */
  def twoStepClassifyAndWrite(inputs: Inputs, partitions: Int, dynamicReports: Boolean,
                              dynamicBrackenReadLength: Option[Int]): Unit = {
    val reads = inputs.getInputFragments(withAmbiguous = true).
      coalesce(partitions)
    val (records, usedTaxa) = makeRecords(reads, Some(outputLocation + "_taxonSet.txt"))
    if (dynamicReports || dynamicBrackenReadLength.nonEmpty) {
      records.cache()
    }

    val dynamicIndex = base.withRecords(records)

    try {
      //Write genome and minimizer reports for the dynamic index
      if (dynamicReports) {
        reportDynamicIndexSupport(reads)
        dynamicIndex.report(None, outputLocation + "_dynamic")
      }

      for {brackenLength <- dynamicBrackenReadLength} {
        val t = startTimer("Build library and Bracken weights")
        new BrackenWeights(dynamicIndex, brackenLength).
          buildAndWriteWeights(genomes, usedTaxa, outputLocation + s"/database${brackenLength}mers.kmer_distrib")
        t.finish()
      }

      val t = startTimer("Classify reads")
      val hits = dynamicIndex.collectHitsBySequence(reads, cpar.perReadOutput)
      val cls = new Classifier(dynamicIndex)
      cls.classifyHitsAndWrite(hits, outputLocation, cpar)
      t.finish()
    } finally {
      records.unpersist()
    }
  }

  /** Build a dynamic index from a taxon set, which can be either supplied (a gold standard set)
   * or detected using a heuristic.
   *
   * @param subjects         reads for detecting a taxon set
   * @param setWriteLocation location to write the detected taxon set (optionally) for later inspection
   */
  def makeRecords(subjects: Dataset[InputFragment], setWriteLocation: Option[String]): (DataFrame, mutable.BitSet) = {

    val taxonSet = goldSetOpts match {
      case Some(dgs@GoldSetOptions(_, _, true)) =>
        val goldSet = readGoldSet(dgs)
        taxonomy.taxaWithDescendants(goldSet)
      case _ =>
        findTaxonSet(subjects, setWriteLocation)
    }

    //Dynamically create a new index containing only the identified taxa
    (base.makeRecords(genomes, Some(taxonSet)), taxonSet)
  }

}
