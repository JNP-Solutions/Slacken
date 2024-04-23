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

  def twoStepClassify(subjects: Dataset[InputFragment]): Dataset[(SeqTitle, Array[TaxonHit])] = {
    val hits1 = base.classify(base.loadBuckets(), subjects)

    val initThreshold = 0 //prioritise high sensitivity, some false positives are acceptable
    val classified = base.classifyHits(hits1, cpar, initThreshold)
    reclassify(classified)
  }

  def twoStepClassifyThreshold(subjects: Dataset[InputFragment], threshold: Double): Dataset[ClassifiedRead] = {
    val hits = twoStepClassify(subjects)
    base.classifyHits(hits, cpar, threshold)
  }

  def twoStepClassifyAndWrite(inputs: Inputs, outputLocation: String): Unit = {
    val subjects = inputs.getInputFragments(withRC = false, withAmbiguous = true)
    twoStepClassifyAndWrite(subjects, outputLocation)
  }

  def twoStepClassifyAndWrite(subjects: Dataset[InputFragment], outputLocation: String): Unit = {
    val hits = twoStepClassify(subjects)
    base.classifyHitsAndWrite(hits, outputLocation, cpar)
  }

  def reclassify(initial: Dataset[ClassifiedRead]): Dataset[(SeqTitle, Array[TaxonHit])] = {
    try {
      initial.cache()
      //collect taxa from the first classification
      val taxa = initial.select("taxon").distinct().as[Taxon].collect()
      val taxaAtRank = mutable.BitSet.empty ++ taxa.map(t => taxonomy.ancestorAtLevel(t, reclassifyRank)).
        filter(_ != ROOT)
      println(s"Initial classification produced ${taxaAtRank.size} taxa at level $reclassifyRank")

      //Dynamically create a new index containing only the identified taxa and their descendants
      val buckets = base.makeBuckets(genomes, taxonLabels, false, Some(taxaAtRank))
      val spans = initial.flatMap(r =>
        r.hits.map(_.toSpan(r.title)))
      base.classifySpans(buckets, spans)
    } finally {
      initial.unpersist()
    }
  }
}
