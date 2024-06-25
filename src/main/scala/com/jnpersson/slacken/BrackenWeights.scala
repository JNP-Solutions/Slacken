package com.jnpersson.slacken

import com.jnpersson.{discount, slacken}
import com.jnpersson.discount.{NTSeq, SeqLocation, SeqTitle}
import com.jnpersson.discount.hash.InputFragment
import com.jnpersson.discount.spark.AnyMinSplitter
import com.jnpersson.discount.util.NTBitArray
import com.jnpersson.slacken.TaxonomicIndex.{classify, getTaxonLabels}
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}

import scala.collection.{BitSet, mutable}


object TaxonFragment {

  //  def fromSeqTaxon(fragment: InputFragment, taxon: Taxon) =
  //    TaxonFragment(taxon, fragment.nucleotides, fragment.header + fragment.location)


}

/**
 *
 * @param taxon
 * @param nucleotides
 * @param id the fragment id
 */
final case class TaxonFragment(taxon: Taxon, nucleotides: NTSeq, id: String) {

  /**
   * Returns all distinct minimizers in the nucleotide sequence
   *
   * @param splitter
   * @return
   */
  def distinctMinimizers(splitter: AnyMinSplitter) = {
    splitter.superkmerPositions(nucleotides, addRC = false).map(_._2).toArray.distinct.iterator.map(_.data)
  }

  def generateReads(seq: NTSeq, readLen: Int): Iterator[NTSeq] = {

    for {
      i <- Iterator.range(0, seq.length - readLen + 1)
      read = seq.substring(i, i + readLen)
    } yield read

  }

  /**
   * Generate reads from the fragment then classify them according to the lca's.
   *
   * @param minimizers
   * @param lcas
   * @return
   */
  def readClassifications(taxonomy: Taxonomy, minimizers: Array[Array[Long]], lcas: Array[Taxon],
                          splitter: AnyMinSplitter, readLen: Int): Iterator[(Taxon, Long)] = {


    val encodedMinimizers = minimizers.map(m => NTBitArray(m, splitter.priorities.width))
    // this map will contain a subset of the lca index that supports random access
    val lcaLookup = mutable.Map.empty ++ encodedMinimizers.iterator.zip(lcas.iterator)
    val reads = generateReads(nucleotides, readLen)
    val k = splitter.k
    val bogusOrdinal = 0
    // confidence threshold is irrelevant for this purpose
    val confidenceThreshold = 0.0
    val cpar = ClassifyParams(2, withUnclassified = false, List.empty, None)
    // true ordinal not needed for this use case
    val classifications = reads.flatMap { r =>
      val taxonHits = splitter.superkmerPositions(r, addRC = false).map(s =>
        TaxonHit(s._2.data, bogusOrdinal, lcaLookup(s._2), s._3 - (k - 1)))

      TaxonomicIndex.classify(taxonomy, taxonHits.toArray, confidenceThreshold, k, cpar)
    }

    val counted = mutable.Map.empty[Taxon, Long].withDefaultValue(0)
    for {c <- classifications} counted(c) += 1
    counted.iterator
  }

}

/**
 *
 * @param buckets       minimizer LCAs to classify genomes against.
 * @param keyValueIndex used to collect minimizer parameters, taxonomy, splitter
 * @param spark
 */
class BrackenWeights(buckets: DataFrame, keyValueIndex: KeyValueIndex, readLen: Int)(implicit val spark: SparkSession) {

  import spark.sqlContext.implicits._

  def brackenReport = ???

  def buildWeights(library: GenomeLibrary, taxa: BitSet) = {

    val titlesTaxa = getTaxonLabels(library.labelFile).toDF("header", "taxon")
    val idSeqDF = library.inputs.getInputFragments(withRC = false)
    val fragments = idSeqDF.join(titlesTaxa, idSeqDF("header") === titlesTaxa("header")).
      select("taxon", "nucleotides", "location", "header").as[(Taxon, NTSeq, SeqLocation, SeqTitle)].
      map(x => TaxonFragment(x._1, x._2, x._4 + x._3))
    val bcSplit = keyValueIndex.bcSplit
    val bcTaxonomy = keyValueIndex.bcTaxonomy
    val withMins = fragments.flatMap { x =>
        x.distinctMinimizers(bcSplit.value).map(m => (x, m))
      }.toDF("fragment", "minimizer").
      join(buckets, keyValueIndex.idColumnNames).
      groupBy("fragment.id").agg(functions.first("fragment"), collect_list("minimizer"), collect_list("taxon")).
      as[(String, slacken.TaxonFragment, Array[Array[Long]], Array[Taxon])].
      flatMap { case (id, f, m, t) =>
        f.readClassifications(bcTaxonomy.value, m, t, bcSplit.value, readLen)
      }

  }
}

