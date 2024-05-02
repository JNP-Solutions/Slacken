package com.jnpersson.slacken

import com.jnpersson.discount.SeqTitle
import com.jnpersson.discount.hash.InputFragment
import com.jnpersson.discount.spark.Inputs
import com.jnpersson.slacken.TaxonomicIndex.ClassifiedRead
import com.jnpersson.slacken.Taxonomy.{ROOT, Rank}
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}

import scala.collection.mutable

/** Two-step classification of reads with dynamically generated indexes,
 * starting from a base index.
 *
 * @param base Initial index for identifying taxa by minimizer
 * @param genomes location of all input genome sequences, for construction of new indexes on the fly
 * @param taxonLabels location of taxonomic label file, for the genome library
 * @param reclassifyRank rank for the initial classification. Taxa at this level will be used to construct the second index
 * @param taxonMinCount minimum k-mer abundance to keep a taxon in the first pass
 * @param cpar parameters for classification
 */
class Dynamic[Record](base: TaxonomicIndex[Record], genomes: Inputs, taxonLabels: String,
                        reclassifyRank: Rank, taxonMinCount: Int, cpar: ClassifyParams)(implicit spark: SparkSession) {
  import spark.sqlContext.implicits._

  def taxonomy = base.taxonomy

  def step1AggregateTaxa(subjects: Dataset[InputFragment]): Array[(Taxon, Long)] = {
    val hits = base.findHits(base.loadBuckets(), subjects)
    hits.flatMap(h => h.trueTaxon.map(t => (t, h.count.toLong))).
      toDF("taxon", "count").groupBy("taxon").agg(functions.sum("count").as("count")).
      as[(Taxon, Long)].collect()
  }

  def twoStepClassify(subjects: Dataset[InputFragment]): Dataset[(SeqTitle, Array[TaxonHit])] =
    reclassify(subjects, step1AggregateTaxa(subjects))

  /** Perform two-step classification,
   * writing the final results to a location.
   */
  def twoStepClassifyAndWrite(inputs: Inputs, outputLocation: String): Unit = {
    val hits = twoStepClassify(inputs.getInputFragments(withRC = false, withAmbiguous = true).
      coalesce(base.numIndexBuckets))
    base.classifyHitsAndWrite(hits, outputLocation, cpar)
  }

  /** Reclassify reads with a dynamic index. This produces the final result.
   */
  def reclassify(subjects: Dataset[InputFragment], taxonCounts: Array[(Taxon, Long)]): Dataset[(SeqTitle, Array[TaxonHit])] = {
    val keepTaxa = mutable.BitSet.empty ++ taxonCounts.iterator.filter(_._2 >= taxonMinCount).map(_._1)

    val taxaAtRank = keepTaxa.
      filter(t => taxonomy.depth(t) >= reclassifyRank.depth)
//    flatMap(t => List(t, taxonomy.ancestorAtLevel(t, reclassifyRank))).

    println(s"Initial scan produced ${keepTaxa.size} taxa, filtered at $reclassifyRank to ${taxaAtRank.size} taxa")

    //Dynamically create a new index containing only the identified taxa and their descendants
    val buckets = base.makeBuckets(genomes, taxonLabels, false, Some(taxaAtRank))
    base.classify(buckets, subjects)
  }
}
