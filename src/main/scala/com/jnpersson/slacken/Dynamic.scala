package com.jnpersson.slacken

import com.jnpersson.discount.hash.InputFragment
import com.jnpersson.discount.spark.Output.formatPerc
import com.jnpersson.discount.spark.{HDFSUtil, Inputs, Output}
import com.jnpersson.slacken.Taxonomy.Rank
import org.apache.spark.sql.{Dataset, SparkSession, functions}

import scala.collection.mutable


/** Two-step classification of reads with dynamically generated indexes,
 * starting from a base index.
 *
 * @param base Initial index for identifying taxa by minimizer
 * @param genomes genomic library for construction of new indexes on the fly
 * @param reclassifyRank rank for the initial classification. Taxa at this level will be used to construct the second index
 * @param taxonMinFraction minimum distinct minimizers to keep a taxon in the first pass
 * @param cpar parameters for classification
 * @param goldStandardTaxonSet parameters for deciding whether to get stats or classify wrt gold standard
 */
class Dynamic[Record](base: TaxonomicIndex[Record], genomes: GenomeLibrary,
                      reclassifyRank: Rank, taxonMinFraction: Double,
                      cpar: ClassifyParams,
                      goldStandardTaxonSet: Option[(String,Boolean)],
                      reportDynamicIndexLocation: Option[String])(implicit spark: SparkSession) {
  import spark.sqlContext.implicits._

  def taxonomy = base.taxonomy

  def step1AggregateTaxa(subjects: Dataset[InputFragment]): Array[(Taxon, Long)] = {
    val hits = base.findHits(base.loadBuckets(), subjects)

    val bcTax = base.bcTaxonomy
    val rank = reclassifyRank

    //approx_count_distinct uses hyperLogLogPlusPlus to estimate the number of distinct values
    //https://en.wikipedia.org/wiki/HyperLogLog#HLL++
    hits.flatMap(h =>
      for { t <- h.trueTaxon
        if bcTax.value.depth(t) >= rank.depth
        } yield (t, h.minimizer)
      ).
      toDF("taxon", "minimizer").groupBy("taxon").
      agg(functions.count_distinct($"minimizer").as("count")).
      as[(Taxon, Long)].collect()
  }

  def minimizersPerTaxon(taxa: Seq[Taxon]): Array[(Taxon, Long)] =
    base.distinctMinimizersPerTaxa(base.loadBuckets(), taxa)

  /** Perform two-step classification, writing the final results to a location.
   * @param inputs Subjects to classify (reads)
   * @param outputLocation Directory to write reports and classifications in
   * @param partitions Number of partitions for the dynamically generated index in step 2
   */
  def twoStepClassifyAndWrite(inputs: Inputs, outputLocation: String, partitions: Int): Unit = {
    val reads = inputs.getInputFragments(withRC = false, withAmbiguous = true).
      coalesce(partitions)
    val buckets = makeBuckets(reads, Some(outputLocation + "_taxonSet.txt"))

    try {
      //Write genome and minimizer reports for the dynamic index
      //Inefficient but simple (could be caching buckets), intended for debugging purposes
      for {location <- reportDynamicIndexLocation} {
        buckets.cache()
        base.report(buckets, None, location)
      }
      val hits = base.classify(buckets, reads)
      base.classifyHitsAndWrite(hits, outputLocation, cpar)
    } finally {
      buckets.unpersist()
    }
  }


  /** Find an estimated taxon set in the given reads (to be classified),
   * emphasising recall over precision.
   */
  def findTaxonSet(subjects: Dataset[InputFragment], writeLocation: Option[String]): mutable.BitSet = {
    val agg = step1AggregateTaxa(subjects)
    val hitMinimizers = new TreeAggregator(taxonomy, agg)
    val atRank = hitMinimizers.keys.filter(taxon => taxonomy.depth(taxon) >= reclassifyRank.depth)
    val withDescendants = taxonomy.taxaWithDescendants(atRank)
    val totalMinimizers = new TreeAggregator(taxonomy, minimizersPerTaxon(withDescendants.toSeq))

    /** Fraction of distinct minimizers in a taxon (including the entire clade)
     * that were seen
     */
    def hitFraction(t: Taxon) = {
      if (totalMinimizers.cladeTotals(t) == 0) 0.0 else
        hitMinimizers.cladeTotals(t).toDouble / totalMinimizers.cladeTotals(t)
    }

    for { loc <- reportDynamicIndexLocation } {
      val report = new KrakenReport(taxonomy, agg) {
        override def dataColumns(taxid: Taxon): String = {
          s"${super.dataColumns(taxid)}\t${totalMinimizers.cladeTotals(taxid)}\t${totalMinimizers.taxonCounts(taxid)}\t${"%.3f".format(hitFraction(taxid))}"
        }
      }
      HDFSUtil.usingWriter(loc + "_support_report.txt", wr =>
        report.print(wr)
      )
    }

    val keepTaxa = mutable.BitSet.empty ++
      (for {(taxon, count) <- hitMinimizers.cladeTotals
            if taxonomy.depth(taxon) >= reclassifyRank.depth
            if hitFraction(taxon) >= taxonMinFraction
            }
      yield taxon)

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

    //include descendants (leaf genomes) if they have enough unique minimizers
    val allowedTaxa =
      keepTaxa ++
        taxonomy.taxaWithDescendants(keepTaxa).filter(t => hitFraction(t) >= taxonMinFraction)
    println(s"Initial scan (cutoff $taxonMinFraction) produced ${keepTaxa.size} taxa at rank $reclassifyRank, " +
      s"expanded with descendants (min fraction $taxonMinFraction) to ${allowedTaxa.size}")

    allowedTaxa
  }

  def readGoldSet(path: String): mutable.BitSet = {
    val goldSet = mutable.BitSet.empty ++
      spark.read.csv(path).map(x => x.getString(0).toInt).collect()
    val taxaAtRank = goldSet.filter(t => taxonomy.depth(t) >= reclassifyRank.depth)
    println(s"Gold set contained ${goldSet.size} taxa, filtered at $reclassifyRank to ${taxaAtRank.size} taxa")
    taxaAtRank
  }

  /** Build a dynamic index from a taxon set, which can be either supplied (a gold standard set)
   * or detected using a heuristic.
   *
   * @param subjects reads for detecting a taxon set
   * @param setWriteLocation location to write the detected taxon set (optionally) for later inspection
   */
  def makeBuckets(subjects: Dataset[InputFragment], setWriteLocation: Option[String]): Dataset[Record] = {

    val taxonSet = goldStandardTaxonSet match {
      case Some((path, true)) =>
        val goldSet = readGoldSet(path)
        taxonomy.taxaWithDescendants(goldSet)
      case _ =>
        findTaxonSet(subjects, setWriteLocation)
    }

    //Dynamically create a new index containing only the identified taxa and their descendants
    base.makeBuckets(genomes, addRC = false, Some(taxonSet))
  }

}
