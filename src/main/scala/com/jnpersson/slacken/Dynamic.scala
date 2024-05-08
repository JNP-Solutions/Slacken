package com.jnpersson.slacken

import com.jnpersson.discount.SeqTitle
import com.jnpersson.discount.hash.InputFragment
import com.jnpersson.discount.spark.Inputs


import com.jnpersson.slacken.Taxonomy.{ROOT, Rank}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}

import scala.collection.mutable


/** Two-step classification of reads with dynamically generated indexes,
 * starting from a base index.
 *
 * @param base Initial index for identifying taxa by minimizer
 * @param genomes genomic library for construction of new indexes on the fly
 * @param reclassifyRank rank for the initial classification. Taxa at this level will be used to construct the second index
 * @param taxonMinFraction minimum k-mer fraction to keep a taxon in the first pass
 * @param cpar parameters for classification
 * @param goldStandardTaxonSet parameters for deciding whether to get stats or classify wrt gold standard
 */
class Dynamic[Record](base: TaxonomicIndex[Record], genomes: GenomeLibrary,
                      reclassifyRank: Rank, taxonMinFraction: Double, cpar: ClassifyParams,
                      goldStandardTaxonSet: Option[(String,Boolean)])(implicit spark: SparkSession) {
  import spark.sqlContext.implicits._

  def taxonomy = base.taxonomy

  def step1AggregateTaxa(subjects: Dataset[InputFragment]): Array[(Taxon, Long)] = {
    val hits = base.findHits(base.loadBuckets(), subjects)
    hits.flatMap(h => h.trueTaxon.map(t => (t, h.count.toLong))).
      toDF("taxon", "count").groupBy("taxon").agg(functions.sum("count").as("count")).
      as[(Taxon, Long)].collect()
  }

  /** Perform two-step classification, writing the final results to a location.
   * @param inputs Subjects to classify (reads)
   * @param outputLocation Directory to write reports and classifications in
   * @param partitions Number of partitions for the dynamically generated index in step 2
   */
  def twoStepClassifyAndWrite(inputs: Inputs, outputLocation: String, partitions: Int): Unit = {
    val hits = twoStepClassify(inputs.getInputFragments(withRC = false, withAmbiguous = true).
      coalesce(partitions))
    base.classifyHitsAndWrite(hits, outputLocation, cpar)
  }

  /** Find an estimated taxon set in the given reads (to be classified),
   * emphasising recall over precision.
   */
  def findTaxonSet(subjects: Dataset[InputFragment]): mutable.BitSet = {
    val taxonCounts = step1AggregateTaxa(subjects)
    val taxonTotal = taxonCounts.iterator.map(_._2.toDouble).sum

    val keepTaxa = mutable.BitSet.empty ++ taxonCounts.iterator.filter(_._2/taxonTotal >= taxonMinFraction).map(_._1)

    val taxaAtRank = keepTaxa.
      filter(t => taxonomy.depth(t) >= reclassifyRank.depth)
    //    flatMap(t => List(t, taxonomy.ancestorAtLevel(t, reclassifyRank))).

    println(s"Initial scan produced ${keepTaxa.size} taxa, filtered at $reclassifyRank to ${taxaAtRank.size} taxa")
    taxaAtRank
  }

  /** Reclassify reads using a dynamic index. This produces the final result.
   * The dynamic index is built from a taxon set, which can be either supplied (a gold standard set)
   * or detected using a heuristic.
   */
  def twoStepClassify(subjects: Dataset[InputFragment]): Dataset[(SeqTitle, Array[TaxonHit])] = {

    val taxonSet = goldStandardTaxonSet match {
      case Some((path,classifyWithGoldSet)) =>
        val goldSet = mutable.BitSet.empty ++ spark.read.csv(path).map(x => x.getString(0).toInt).collect()
        if(classifyWithGoldSet) {
          goldSet
        } else {
          import com.jnpersson.discount.spark.Output.formatPerc
          //Only compare the gold standard set to the detected set, use the latter for classification
          val taxaAtRank = findTaxonSet(subjects)
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
        findTaxonSet(subjects)
    }

    //Dynamically create a new index containing only the identified taxa and their descendants
    val buckets = base.makeBuckets(genomes, false, Some(taxonSet))
    base.classify(buckets, subjects)
  }
}
