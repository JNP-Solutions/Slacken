package com.jnpersson.slacken

import com.jnpersson.slacken
import com.jnpersson.discount.{NTSeq, SeqLocation, SeqTitle}
import com.jnpersson.discount.spark.AnyMinSplitter
import com.jnpersson.discount.util.NTBitArray
import com.jnpersson.slacken.TaxonomicIndex.getTaxonLabels
import org.apache.spark.sql.functions.{collect_list, udf}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import scala.collection.{BitSet, mutable}

object TaxonFragment {

  //  def fromSeqTaxon(fragment: InputFragment, taxon: Taxon) =
  //    TaxonFragment(taxon, fragment.nucleotides, fragment.header + fragment.location)


}

/**
 * A fragment of a genome.
 * @param taxon The taxon that this fragment came from
 * @param nucleotides The nucleotide sequence
 * @param id Unique ID of this fragment (for grouping)
 */
final case class TaxonFragment(taxon: Taxon, nucleotides: NTSeq, id: String) {

  /**
   * Returns all distinct minimizers in the nucleotide sequence
   *
   * @param splitter the minimizer scheme
   * @return
   */
  def distinctMinimizers(splitter: AnyMinSplitter): Iterator[Array[SeqLocation]] =
    splitter.superkmerPositions(nucleotides, addRC = false).map(_._2).toArray.distinct.iterator.map(_.data)


/** Generate all reads of a given length */
  def generateReads(seq: NTSeq, readLen: Int): Iterator[NTSeq] =
    for {
      i <- Iterator.range(0, seq.length - readLen + 1)
      read = seq.substring(i, i + readLen)
    } yield read

  /**
   * Generate reads from the fragment then classify them according to the LCAs.
   *
   * @param taxonomy   the taxonomy
   * @param minimizers all minimizers encountered in this fragment (to be paired with LCAs)
   * @param lcas       all LCAs of minimizers encountered in this fragment, in the same order as minimizers
   * @param splitter   the minimizer scheme
   * @param readLen    length of reads to be generated
   * @return an iterator of (taxon, number of reads classified to that taxon)
   */
  def readClassifications(taxonomy: Taxonomy, minimizers: Array[Array[Long]], lcas: Array[Taxon],
                          splitter: AnyMinSplitter, readLen: Int): Iterator[(Taxon, Long)] = {

    //TODO add one more column with srcTaxon (currently we are computing destTaxon)

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
 * Generate bracken-style weights (self-classifying all reads of genomes in a library against the library).
 * This is intended to be fully compatible with Bracken for abundance reestimation.
 * See: https://github.com/jenniferlu717/Bracken
 *
 * @param buckets       minimizer LCAs to classify genomes against.
 * @param keyValueIndex used to collect minimizer parameters, taxonomy, splitter
 * @param readLen       length of reads to be generated and classified
 * @param spark
 */
class BrackenWeights(buckets: DataFrame, keyValueIndex: KeyValueIndex, readLen: Int)(implicit val spark: SparkSession) {

  import spark.sqlContext.implicits._

  def brackenReport = ???

  /**
   * Build Bracken weights for a given library.
   *
   * @param library        The genomes to simulate reads from
   * @param taxa           A taxon filter for the genomes (only included taxa will be simulated)
   * @param outputLocation File to write the results to
   */
  def buildWeights(library: GenomeLibrary, taxa: BitSet, outputLocation: String) = {

    val titlesTaxa = getTaxonLabels(library.labelFile).toDF("header", "taxon")

    val idSeqDF = library.inputs.getInputFragments(withRC = false)
    val presentTaxon = udf((x: Taxon) => taxa.contains(x))

    //Find all fragments of genomes
    val fragments = idSeqDF.join(titlesTaxa, List("header")).
      select("taxon", "nucleotides", "location", "header").
      where(presentTaxon($"taxon")).
      as[(Taxon, NTSeq, SeqLocation, SeqTitle)].
      map(x => TaxonFragment(x._1, x._2, x._4 + x._3)) //x._4 + x._3 becomes a unique ID

    val bcSplit = keyValueIndex.bcSplit
    val bcTaxonomy = keyValueIndex.bcTaxonomy

    val withMins = fragments.flatMap { x =>
        x.distinctMinimizers(bcSplit.value).map(m => (x, m))
      }.toDF("fragment", "minimizer").
      select(keyValueIndex.idColumnsFromMinimizer :+ $"fragment" :_*).
      join(buckets, keyValueIndex.idColumnNames).
      groupBy("fragment.id").agg(
        functions.first("fragment"),
        collect_list(keyValueIndex.minimizerColumnFromIdColumns),
        collect_list("taxon")
      ).
      as[(String, slacken.TaxonFragment, Array[Array[Long]], Array[Taxon])].
      flatMap { case (id, f, m, t) =>
        f.readClassifications(bcTaxonomy.value, m, t, bcSplit.value, readLen)
      }

    //For testing
    withMins.show()

    //TODO group by source taxon and count
    //TODO group by dest taxon and aggregate
    //TODO collect and output TSV
  }
}

