package com.jnpersson.slacken

import com.jnpersson.discount.SeqTitle
import com.jnpersson.discount.hash.InputFragment
import com.jnpersson.discount.spark.{HDFSUtil, Inputs}
import com.jnpersson.slacken.Taxonomy.{ROOT, Rank}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}

import scala.collection.mutable


/** Two-step classification of reads with dynamically generated indexes,
 * starting from a base index.
 *
 * @param base Initial index for identifying taxa by minimizer
 * @param genomes genomic library for construction of new indexes on the fly
 * @param reclassifyRank rank for the initial classification. Taxa at this level will be used to construct the second index
 * @param taxonMinCount minimum distinct minimizers to keep a taxon in the first pass
 * @param cpar parameters for classification
 * @param goldStandardTaxonSet parameters for deciding whether to get stats or classify wrt gold standard
 */
class Dynamic[Record](base: TaxonomicIndex[Record], genomes: GenomeLibrary,
                      reclassifyRank: Rank, taxonMinCount: Int, cpar: ClassifyParams,
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
          //move hits up to given ancestor level e.g. species, so that we can aggregate counts from strains and
          //subspecies. However, sometimes not every level is present, in which case we use t as it was.
          atLevel = bcTax.value.ancestorAtLevelStrict(t, rank)
          mappedTaxon = atLevel.getOrElse(t)
        } yield (mappedTaxon, h.minimizer)
      ).
      toDF("taxon", "minimizer").groupBy("taxon").
      agg(functions.approx_count_distinct("minimizer").as("count")).
      as[(Taxon, Long)].collect()
  }

  /** Perform two-step classification, writing the final results to a location.
   * @param inputs Subjects to classify (reads)
   * @param outputLocation Directory to write reports and classifications in
   * @param partitions Number of partitions for the dynamically generated index in step 2
   */
  def twoStepClassifyAndWrite(inputs: Inputs, outputLocation: String, partitions: Int): Unit = {
    val reads = inputs.getInputFragments(withRC = false, withAmbiguous = true).
      coalesce(partitions)
    val hits = twoStepClassify(reads, Some(outputLocation + "_taxonSet.txt"))
    base.classifyHitsAndWrite(hits, outputLocation, cpar)
  }

  /** Find an estimated taxon set in the given reads (to be classified),
   * emphasising recall over precision.
   */
  def findTaxonSet(subjects: Dataset[InputFragment], writeLocation: Option[String]): mutable.BitSet = {
    val taxonCounts = step1AggregateTaxa(subjects)
    val taxonTotal = taxonCounts.iterator.map(_._2.toDouble).sum

    val keepTaxa = mutable.BitSet.empty ++ taxonCounts.iterator.filter(_._2 >= taxonMinCount).map(_._1)

    println(s"Initial scan produced ${keepTaxa.size} taxa at rank $reclassifyRank")
    for { loc <- writeLocation }
      HDFSUtil.writeTextLines(loc, keepTaxa.iterator.map(_.toString))
    keepTaxa
  }

  def readGoldSet(path: String): mutable.BitSet = {
    val goldSet = mutable.BitSet.empty ++
      spark.read.csv(path).map(x => x.getString(0).toInt).collect()
    val taxaAtRank = goldSet.filter(t => taxonomy.depth(t) >= reclassifyRank.depth)
    println(s"Gold set contained ${goldSet.size} taxa, filtered at $reclassifyRank to ${taxaAtRank.size} taxa")
    taxaAtRank
  }

  /** Reclassify reads using a dynamic index. This produces the final result.
   * The dynamic index is built from a taxon set, which can be either supplied (a gold standard set)
   * or detected using a heuristic.
   *
   * @param subjects reads to classify
   * @param setWriteLocation location to write the detected taxon set (optionally) for later inspection
   */
  def twoStepClassify(subjects: Dataset[InputFragment], setWriteLocation: Option[String]): Dataset[(SeqTitle, Array[TaxonHit])] = {

    val taxonSet = goldStandardTaxonSet match {
      case Some((path,classifyWithGoldSet)) =>
        val goldSet = readGoldSet(path)
        if(classifyWithGoldSet) {
          goldSet
        } else {
          import com.jnpersson.discount.spark.Output.formatPerc
          //Only compare the gold standard set to the detected set, use the latter for classification
          val taxaAtRank = findTaxonSet(subjects, setWriteLocation)
          val tp = taxaAtRank.intersect(goldSet).size
          val fp = (taxaAtRank -- taxaAtRank.intersect(goldSet)).size
          val fn = (goldSet -- taxaAtRank.intersect(goldSet)).size
          val precision = tp.toDouble/(tp+fp)
          val recall = tp.toDouble/goldSet.size
          println(s"True Positives: $tp, False Positives: $fp, False Negatives: $fn, " +
            s"Precision: ${formatPerc(precision)}, Recall: ${formatPerc(recall)}")
          taxaAtRank
        }

      case None =>
        findTaxonSet(subjects, setWriteLocation)
    }

    //Dynamically create a new index containing only the identified taxa and their descendants
    val buckets = base.makeBuckets(genomes, false, Some(taxonSet))

    //Write genome and minimizer reports for the dynamic index
    //Inefficient but simple (could be caching buckets), intended for debugging purposes
    for { location <- reportDynamicIndexLocation } {
      base.report(buckets, None, location)
    }

    base.classify(buckets, subjects)
  }
}
