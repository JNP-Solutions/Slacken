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
 * @param genomes location of all input genome sequences, for construction of new indexes on the fly
 * @param taxonLabels location of taxonomic label file, for the genome library
 * @param reclassifyRank rank for the initial classification. Taxa at this level will be used to construct the second index
 * @param taxonMinCount minimum k-mer abundance to keep a taxon in the first pass
 * @param cpar parameters for classification
 * @param goldStandardTaxonSet parameters for deciding whether to get stats or classify wrt gold standard
 */
class Dynamic[Record](base: TaxonomicIndex[Record], genomes: Inputs, taxonLabels: String,
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

  /** Possibly add a boolean variable that decides if step1AggregateTaxa is run on the TaxaAtRank set or the goldStandardTaxaSet
  * */
  def twoStepClassify(subjects: Dataset[InputFragment]): Dataset[(SeqTitle, Array[TaxonHit])] =
    reclassify(subjects, step1AggregateTaxa(subjects))

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

  /** Reclassify reads with a dynamic index. This produces the final result.
   */
  def reclassify(subjects: Dataset[InputFragment], taxonCounts: Array[(Taxon, Long)]): Dataset[(SeqTitle, Array[TaxonHit])] = {
    val taxonTotal = taxonCounts.iterator.map(_._2.toDouble).sum

    val keepTaxa = mutable.BitSet.empty ++ taxonCounts.iterator.filter(_._2/taxonTotal >= taxonMinFraction).map(_._1)

    val taxaAtRank = keepTaxa.
      filter(t => taxonomy.depth(t) >= reclassifyRank.depth)
//    flatMap(t => List(t, taxonomy.ancestorAtLevel(t, reclassifyRank))).

    println(s"Initial scan produced ${keepTaxa.size} taxa, filtered at $reclassifyRank to ${taxaAtRank.size} taxa")

    // can make this more efficient by not computing the taxaAtRank if the classification with the GoldSet has been
    // requested @nishad
    val taxonSet = goldStandardTaxonSet match {
      case Some((path,classifyWithGoldset)) =>
        val goldSet = mutable.BitSet.empty ++ spark.read.csv(path).map(x => x.getString(0).toInt).collect()
        val tp = taxaAtRank.intersect(goldSet).size
        val fp = (taxaAtRank -- taxaAtRank.intersect(goldSet)).size
        val fn = (goldSet -- taxaAtRank.intersect(goldSet)).size
        val precision = tp.toDouble/(tp+fp)
        val recall = tp.toDouble/goldSet.size
        println(s"True Positives: $tp, False Positives: $fp, False Negatives: $fn, Precision: $precision, Recall: $recall")
        if(classifyWithGoldset) goldSet else taxaAtRank

      case None =>
        taxaAtRank
    }

    //Dynamically create a new index containing only the identified taxa and their descendants
    val buckets = base.makeBuckets(genomes, taxonLabels, false, Some(taxonSet))
    base.classify(buckets, subjects)
  }
}
