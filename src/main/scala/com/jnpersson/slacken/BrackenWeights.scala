package com.jnpersson.slacken

import com.jnpersson.discount.{NTSeq, SeqLocation, SeqTitle}
import com.jnpersson.discount.spark.{AnyMinSplitter, HDFSUtil}
import com.jnpersson.discount.util.NTBitArray
import com.jnpersson.slacken.TaxonomicIndex.{getTaxonLabels, sufficientHitGroups}
import it.unimi.dsi.fastutil.ints.Int2IntMap
import org.apache.spark.sql.functions.{collect_list, min, sum, udf}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import java.io.PrintWriter
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
                          splitter: AnyMinSplitter, readLen: Int): Iterator[(Taxon,Taxon, Long)] = {

    //TODO add one more column with srcTaxon (currently we are computing destTaxon)

    val encodedMinimizers = minimizers.map(m => NTBitArray(m, splitter.priorities.width))
    // this map will contain a subset of the lca index that supports random access
    val lcaLookup = mutable.Map.empty[NTBitArray, Int]
    lcaLookup.sizeHint(minimizers.size)
    lcaLookup ++= encodedMinimizers.iterator.zip(lcas.iterator)

    val k = splitter.k

    val lca = new LowestCommonAncestor(taxonomy)

    val minsInFragment = splitter.superkmerPositions(nucleotides, addRC = false)
    var remainingHits = minsInFragment.map(m =>
      //Overloading the second argument (ordinal) to mean the absolute position in the fragment in this case
      TaxonHit(m._2.data, m._1, lcaLookup(m._2), m._3 - (k - 1))
    ).toList

    //For each window corresponding to a read (start and end),
    //classify the corresponding minimizers.
    Iterator.range(0, nucleotides.length - readLen + 1).flatMap(start => { // inclusive
      val end = start + readLen //non inclusive
      val countSummary = new it.unimi.dsi.fastutil.ints.Int2IntArrayMap(16) //specialised, very fast map

      remainingHits = remainingHits.dropWhile(_.ordinal < start)
      val inRead = remainingHits.takeWhile(_.ordinal + splitter.priorities.width <= end)
      countSummary.clear()
      for { hit <- inRead } {
        countSummary.put(hit.taxon, countSummary.getOrDefault(hit.taxon, 0) + hit.count)
      }

      classify(lca, inRead, countSummary).map(t => (taxon,t, 1L))
    })
  }

  /** Classify a single read efficiently */
  def classify(lca: LowestCommonAncestor, sortedHits: List[TaxonHit], summary: Int2IntMap): Option[Taxon] = {
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
      }.toDF("source","dest","count").groupBy("dest","source").agg(sum("count"))


      //select("dest","source","count").as[(Taxon,(Taxon,Long))].collect().toMap

    //For testing
    //HDFSUtil.usingWriter(outputLocation + "_brackenWeights.txt", wr => brackenWeightsReport(wr,withSequence))

    withSequence.show()

//    def brackenWeightsReport(output:PrintWriter, brakenOut:Map[Taxon,(Taxon,Long)]):Unit={
//
//
//      val headers = s"mapped_taxid\tgenome_taxids:kmers_mapped:total_genome_kmers"
//      output.println(headers)
//
//
//      for {
//        (dest,(source,count)) <- brakenOut
//        outputLine = s"${dest}\t"
//      }
//      {
//        output.println()
//      }
//
//    }

    //TODO group by source taxon and count
    //TODO group by dest taxon and aggregate
    //TODO collect and output TSV
  }
}

