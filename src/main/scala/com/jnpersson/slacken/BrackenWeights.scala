package com.jnpersson.slacken

import com.jnpersson.discount.{NTSeq, SeqLocation, SeqTitle}
import com.jnpersson.discount.spark.AnyMinSplitter
import com.jnpersson.discount.util.NTBitArray
import com.jnpersson.slacken.TaxonomicIndex.{getTaxonLabels, sufficientHitGroups}
import org.apache.spark.sql.functions.{collect_list, min, udf}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import scala.collection.{BitSet, mutable}
import scala.collection.{Map => CMap}

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
    splitter.superkmerPositions(nucleotides, addRC = false).map(_._2).
      toArray.distinct.iterator.map(_.data)

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
    val k = splitter.k

    val lca = new LowestCommonAncestor(taxonomy)

    val minsInFragment = splitter.superkmerPositions(nucleotides, addRC = false)
    var remainingHits = minsInFragment.map(m =>
      //Overloading the second argument (ordinal) to mean the absolute position in the fragment in this case
      TaxonHit(m._2.data, m._1, lcaLookup(m._2), m._3 - (k - 1))
    ).toList

    //For each window corresponding to a read (start and end),
    //classify the corresponding minimizers.
    val classifications = Iterator.range(0, nucleotides.length - readLen + 1).flatMap(start => { // inclusive
      val end = start + readLen //non inclusive
      val hitCountSummary = mutable.Map.empty[Taxon, Int].withDefaultValue(0)
      remainingHits = remainingHits.dropWhile(_.ordinal < start)
      val inRead = remainingHits.takeWhile(_.ordinal + splitter.priorities.width <= end)
      hitCountSummary.clear()
      for { hit <- inRead } hitCountSummary(hit.taxon) += hit.count

      classify(lca, inRead, hitCountSummary)
    })

    val counted = mutable.Map.empty[Taxon, Long].withDefaultValue(0)
    for {c <- classifications} counted(c) += 1
    counted.iterator
  }

  /** Classify a single read efficiently */
  def classify(lca: LowestCommonAncestor, sortedHits: List[TaxonHit], summary: CMap[Taxon, Int]): Option[Taxon] = {
    // confidence threshold is irrelevant for this purpose
    val confidenceThreshold = 0.0
    val minHitGroups = 2
    val taxon = lca.resolveTree(summary, confidenceThreshold)
    val classified = taxon != Taxonomy.NONE && sufficientHitGroups(sortedHits, minHitGroups)
    if (classified) Some(taxon) else None
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
    def fragments = idSeqDF.join(titlesTaxa, List("header")).
      select("taxon", "nucleotides", "location", "header").
      where(presentTaxon($"taxon")).
      as[(Taxon, NTSeq, SeqLocation, SeqTitle)].
      map(x => TaxonFragment(x._1, x._2, x._4 + x._3)) //x._4 + x._3 becomes a unique ID

    val bcSplit = keyValueIndex.bcSplit
    val bcTaxonomy = keyValueIndex.bcTaxonomy

    val idMins = fragments.flatMap { x =>
        x.distinctMinimizers(bcSplit.value).map(m => (x.id, m))
      }.toDF("id", "minimizer").
      select(keyValueIndex.idColumnsFromMinimizer :+ $"id" :_*).
      join(buckets, keyValueIndex.idColumnNames).
      groupBy("id").agg(
        collect_list(keyValueIndex.minimizerColumnFromIdColumns),
        collect_list("taxon")
      ).
      toDF("id", "minimizers", "taxa")

    val readLen = this.readLen
    val withSequence = idMins.join(fragments, List("id")).
      select("id", "taxon", "nucleotides", "minimizers", "taxa").
      as[(String, Taxon, NTSeq, Array[Array[Long]], Array[Taxon])].
      flatMap { case (id, taxon, nts, ms, ts) =>
        val f = TaxonFragment(taxon, nts, id)
        f.readClassifications(bcTaxonomy.value, ms, ts, bcSplit.value, readLen)
      }

    //For testing

    withSequence.show()

    //TODO group by source taxon and count
    //TODO group by dest taxon and aggregate
    //TODO collect and output TSV
  }
}

