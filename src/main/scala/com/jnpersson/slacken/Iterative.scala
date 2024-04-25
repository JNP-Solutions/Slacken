package com.jnpersson.slacken

import com.jnpersson.discount.SeqTitle
import com.jnpersson.discount.hash.InputFragment
import com.jnpersson.discount.spark.Inputs
import com.jnpersson.slacken.TaxonomicIndex.ClassifiedRead
import com.jnpersson.slacken.Taxonomy.{ROOT, Rank}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable

/** Iterative reclassification of reads with dynamically generated indexes,
 * starting from a base index.
 *
 * @param base Initial index for the first classification
 * @param genomes location of all input genome sequences, for construction of new indexes on the fly
 * @param taxonLabels location of taxonomic label file, for the genome library
 * @param reclassifyRank rank for the initial classification. Taxa at this level will be used to construct the second index
 * @param cpar parameters for classification
 */
class Iterative[Record](base: TaxonomicIndex[Record], genomes: Inputs, taxonLabels: String,
                        reclassifyRank: Rank,
                        cpar: ClassifyParams)(implicit spark: SparkSession) {
  import spark.sqlContext.implicits._

  def taxonomy = base.taxonomy

  /** Perform an initial classification with parameters suitable to generate
   * a taxon set with high sensitivity.
   */
  def step1Classify(subjects: Dataset[InputFragment]): Dataset[ClassifiedRead]  = {
    val hits1 = base.classify(base.loadBuckets(), subjects)

    val initThreshold = 0 //prioritise high sensitivity, some false positives are acceptable
    base.classifyHits(hits1, cpar, initThreshold)
  }

  /** Reclassify reads using a dynamic index.
   * It is recommended to cache the initial reads first.
   */
  def step2ClassifyThreshold(reads: Dataset[ClassifiedRead], threshold: Double): Dataset[ClassifiedRead] = {
    val hits = reclassify(reads)
    base.classifyHits(hits, cpar, threshold)
  }

  /** Perform two-step classification, with intermediate caching and release,
   * writing the final results to a location.
   */
  def twoStepClassifyAndWrite(inputs: Inputs, outputLocation: String): Unit = {
    val subjects = inputs.getInputFragments(withRC = false, withAmbiguous = true)
    val reads = step1Classify(subjects).cache
    try {
      val hits = reclassify(reads)
      base.classifyHitsAndWrite(hits, outputLocation, cpar)
    } finally {
      reads.unpersist()
    }
  }

  /** Reclassify reads with a dynamic index.
   * It is recommended to cache the initial reads first.
   */
  def reclassify(initial: Dataset[ClassifiedRead]): Dataset[(SeqTitle, Array[TaxonHit])] = {
    //collect taxa from the first classification
    val taxa = initial.filter(_.classified).select("taxon").distinct().as[Taxon].collect()
    val taxaAtRank = mutable.BitSet.empty ++ taxa.
      filter(t => taxonomy.depth(t) >= reclassifyRank.depth)

//    flatMap(t => List(t, taxonomy.ancestorAtLevel(t, reclassifyRank))).

    println(s"Initial classification produced ${taxaAtRank.size} taxa")

    //Dynamically create a new index containing only the identified taxa and their descendants
    val buckets = base.makeBuckets(genomes, taxonLabels, false, Some(taxaAtRank))
    val spans = initial.flatMap(r => r.hits.map(_.toSpan(r.title)))
    base.classifySpans(buckets, spans)
  }
}
