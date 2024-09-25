package com.jnpersson.slacken

import com.jnpersson.kmers.minimizer._
import com.jnpersson.kmers.Output.formatPerc
import com.jnpersson.kmers.{HDFSUtil, Inputs, Output}
import com.jnpersson.slacken.Taxonomy.Rank
import org.apache.spark.sql.functions.{approx_count_distinct, count}
import org.apache.spark.sql.{DataFrame, Dataset, RelationalGroupedDataset, SparkSession, functions}

import scala.collection.mutable

sealed trait TaxonCriteria
case class MinimizerTotalCount(threshold: Int) extends TaxonCriteria
case class ClassifiedReadCount(threshold: Int, confidence: Double) extends TaxonCriteria
case class MinimizerFraction(threshold: Double) extends TaxonCriteria
case class MinimizerDistinctCount(threshold: Int) extends TaxonCriteria

/** Two-step classification of reads with dynamically generated indexes,
 * starting from a base index.
 * First, a set of taxa will be identified from the sample (reads). Then these taxa will be used
 * to construct a taxonomic index for classifying the reads.
 * A bracken-style weights file describing the second index will optionally also be generated.
 *
 * @param base Initial index for identifying taxa by minimizer
 * @param genomes genomic library for construction of new indexes on the fly
 * @param reclassifyRank rank for the initial classification. Taxa at this level will be used to construct the second index
 * @param taxonMinFraction minimum distinct minimizers to keep a taxon in the first pass
 * @param cpar parameters for classification
 * @param dynamicBrackenReadLength read length for generating bracken weights for the second index (if any)
 * @param goldStandardTaxonSet parameters for deciding whether to get stats or classify wrt gold standard
 * @param reportDynamicIndex whether to generate reports describing the second index
 * @param outputLocation prefix location for output files
 */
class Dynamic(base: KeyValueIndex, genomes: GenomeLibrary,
              reclassifyRank: Rank,
              taxonCriteria: TaxonCriteria,
              cpar: ClassifyParams,
              dynamicBrackenReadLength: Option[Int],
              goldStandardTaxonSet: Option[(String, Boolean)],
              reportDynamicIndex: Boolean,
              outputLocation: String)(implicit spark: SparkSession) {

  import spark.sqlContext.implicits._

  def taxonomy = base.taxonomy

  private def minimizersInSubjects(subjects: Dataset[InputFragment]): RelationalGroupedDataset = {
    val hits = base.findHits(base.loadBuckets(), subjects)

    val bcTax = base.bcTaxonomy
    val rank = reclassifyRank

    hits.flatMap(h =>
        for { t <- h.trueTaxon
              if bcTax.value.depth(t) >= rank.depth
              } yield (t, h.minimizer)
      ).
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
    val inBuckets = base.distinctMinimizersPerTaxon(base.loadBuckets(), inSample.map(_._1).toSeq).
      toMap

    inSample.keys.toArray.map(t => (t, inSample(t).toDouble / inBuckets(t).toDouble))
  }

  /** Counting method that counts the number of minimizers per taxon, in the buckets, to aid taxon set filtering */
  def minimizersPerTaxon(taxa: Seq[Taxon]): Array[(Taxon, Long)] =
    base.distinctMinimizersPerTaxon(base.loadBuckets(), taxa)

  /** Counting method that counts the number of reads classified per taxon to aid taxon set filtering */
  def classifiedReadsPerTaxon(subjects: Dataset[InputFragment], confidenceThreshold: Double): Array[(Taxon, Long)] = {
    val hits = base.classify(base.loadBuckets(), subjects)
    val classified = base.classifyHits(hits, cpar, confidenceThreshold)
    classified.where($"classified" === true).
      select("taxon").
      groupBy("taxon").agg(count("*")).as[(Taxon, Long)].
      collect()
  }

  /** Counting method that counts the number of reads classified per taxon, as well as
   * distinct minimizers, to aid taxon set filtering */
  def classifiedReadsPerTaxonWithDistinctMinimizers(subjects: Dataset[InputFragment]): Array[(Taxon, Long, Long)] = {
    val initThreshold = 0.0
    val hits = base.classify(base.loadBuckets(), subjects)
    val classified = base.classifyHits(hits, cpar, initThreshold)
    classified.where($"classified" === true).
      flatMap(r => r.hits.map(hit => (r.taxon, hit.minimizer, r.title))).toDF("taxon", "minimizer", "title").
      groupBy("taxon").agg(approx_count_distinct("title"), approx_count_distinct("minimizer")).as[(Taxon, Long, Long)].
      collect()
  }

  /** A method for identifying a taxon set in a set of reads. */
  trait TaxonSetFinder {

    /** The identified taxa */
    def taxa: mutable.BitSet

    /** A report with all taxa, even non-included, and supporting information that was used to select the set */
    def report: KrakenReport
  }

  /** Simple count filter that finds a taxon set by capping at a minimum count threshold.
   */
  class CountFilter(counts: Array[(Taxon, Long)], threshold: Int) extends TaxonSetFinder {
    val hitMinimizers = new TreeAggregator(taxonomy, counts)

    def report: KrakenReport =
      new KrakenReport(taxonomy, counts)

    def taxa: mutable.BitSet =
      mutable.BitSet.empty ++
        (for {taxon <- hitMinimizers.keys
              if taxonomy.depth(taxon) >= reclassifyRank.depth
              if hitMinimizers.cladeTotals(taxon) >= threshold
              }
        yield taxon)
  }

  /** Find an estimated taxon set in the given reads (to be classified),
   * emphasising recall over precision.
   */
  def findTaxonSet(subjects: Dataset[InputFragment], writeLocation: Option[String]): mutable.BitSet = {
    val finder = taxonCriteria match {
      case MinimizerTotalCount(threshold) => new CountFilter(totalMinimizersPerTaxon(subjects), threshold)
      case MinimizerFraction(threshold) => ???
      case ClassifiedReadCount(threshold, confidence) => new CountFilter(classifiedReadsPerTaxon(subjects, confidence), threshold)
      case MinimizerDistinctCount(threshold) => new CountFilter(distinctMinimizersPerTaxon(subjects), threshold)
    }

    if (reportDynamicIndex)
      HDFSUtil.usingWriter(outputLocation + "_support_report.txt", wr => finder.report.print(wr))

    val keepTaxa = finder.taxa

    for { loc <- writeLocation }
      HDFSUtil.writeTextLines(loc, keepTaxa.iterator.map(_.toString))

    goldStandardTaxonSet match {
      case Some((path, _)) =>
        val goldSet = readGoldSet(path)
        val tp = keepTaxa.intersect(goldSet).size
        val fp = (keepTaxa -- keepTaxa.intersect(goldSet)).size
        val fn = (goldSet -- keepTaxa.intersect(goldSet)).size
        val precision = tp.toDouble/(tp+fp)
        val recall = tp.toDouble/goldSet.size
        println(s"True Positives: $tp, False Positives: $fp, False Negatives: $fn, " +
          s"Precision: ${formatPerc(precision)}, Recall: ${formatPerc(recall)}")
      case _ =>
    }

    val withDescendants = taxonomy.taxaWithDescendants(keepTaxa)
    println(s"Initial scan (criterion $taxonCriteria) produced ${keepTaxa.size} taxa at rank $reclassifyRank, expanded with descendants to ${withDescendants.size}")
    withDescendants
  }

  lazy val taxonSetInLibrary = genomes.taxonSet(taxonomy)

  def readGoldSet(path: String): mutable.BitSet = {
    val bcTax = base.bcTaxonomy
    val goldSet = mutable.BitSet.empty ++
      spark.read.csv(path).map(x => bcTax.value.primary(x.getString(0).toInt)).collect()

    println(s"Gold set contained ${goldSet.size} taxa")
    val notFound = goldSet -- taxonSetInLibrary

    val elevated = notFound.flatMap(t => {
      val path = taxonomy.pathToRoot(t).filter(taxonSetInLibrary.contains)
      if (path.hasNext) Some(path.next) else None
    })
    println(s"${notFound.size} taxa from gold set not found in library, elevated to ${elevated.size} taxa.")
    val levelCounts = elevated.toSeq.map(t => taxonomy.depth(t)).groupBy(x => x).map(x =>
      (Taxonomy.rankForDepth(x._1).get, x._2.size)).toSeq.sorted
    println(s"Elevated to levels: $levelCounts")
    val total = goldSet ++ elevated
    val filtered = total.filter(taxonomy.depth(_) >= reclassifyRank.depth)
    println(s"Total adjusted gold set size ${total.size}, filtered at $reclassifyRank to ${filtered.size}")
    filtered
  }

  /** Perform two-step classification, writing the final results to a location.
   * @param inputs Subjects to classify (reads)
   * @param outputLocation Directory to write reports and classifications in
   * @param partitions Number of partitions for the dynamically generated index in step 2
   */
  def twoStepClassifyAndWrite(inputs: Inputs, partitions: Int): Unit = {
    val reads = inputs.getInputFragments(withRC = false, withAmbiguous = true).
      coalesce(partitions)
    val (buckets, usedTaxa) = makeBuckets(reads, Some(outputLocation + "_taxonSet.txt"))
    if (reportDynamicIndex || dynamicBrackenReadLength.nonEmpty) {
      buckets.cache()
    }

    try {
      //Write genome and minimizer reports for the dynamic index
      //Inefficient but simple (could be caching buckets), intended for debugging purposes
      if (reportDynamicIndex)
        base.report(buckets, None, outputLocation + "_dynamic")

      for { brackenLength <- dynamicBrackenReadLength } {
        new BrackenWeights(buckets, base, brackenLength).
          buildAndWriteWeights(genomes, usedTaxa, outputLocation + s"/database${brackenLength}mers.kmer_distrib")
      }
      val hits = base.classify(buckets, reads)
      base.classifyHitsAndWrite(hits, outputLocation, cpar)
    } finally {
      buckets.unpersist()
    }
  }

  /** Build a dynamic index from a taxon set, which can be either supplied (a gold standard set)
   * or detected using a heuristic.
   *
   * @param subjects reads for detecting a taxon set
   * @param setWriteLocation location to write the detected taxon set (optionally) for later inspection
   */
  def makeBuckets(subjects: Dataset[InputFragment], setWriteLocation: Option[String]): (DataFrame, mutable.BitSet) = {

    val taxonSet = goldStandardTaxonSet match {
      case Some((path, true)) =>
        val goldSet = readGoldSet(path)
        taxonomy.taxaWithDescendants(goldSet)
      case _ =>
        findTaxonSet(subjects, setWriteLocation)
    }

    //Dynamically create a new index containing only the identified taxa
    (base.makeBuckets(genomes, addRC = false, Some(taxonSet)), taxonSet)
  }

}
