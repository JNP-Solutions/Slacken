package com.jnpersson.slacken

import com.jnpersson.discount.{NTSeq, SeqLocation, SeqTitle}
import com.jnpersson.discount.spark.{AnyMinSplitter, HDFSUtil}
import com.jnpersson.discount.util.NTBitArray
import com.jnpersson.slacken.TaxonomicIndex.{getTaxonLabels, sufficientHitGroups}
import it.unimi.dsi.fastutil.ints.Int2IntMap
import org.apache.spark.sql.functions.{collect_list, sum, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.PrintWriter
import scala.collection.{BitSet, mutable}


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
  def distinctMinimizers(splitter: AnyMinSplitter): Iterator[Array[SeqLocation]] = {
    val noWhitespace = nucleotides.replaceAll("\\s+", "")
    val segments = Supermers.splitByAmbiguity(noWhitespace, Supermers.nonAmbiguousRegex(splitter.k))
    segments.flatMap{
      case (seq, SEQUENCE_FLAG) => splitter.superkmerPositions(seq, addRC = false).map(_._2)
      case (seq, AMBIGUOUS_FLAG) => Iterator.empty
    }.toArray.distinct.iterator.map(_.data)
  }

  /** Sliding window corresponding to a list of minimizers.
   * Each window position corresponds to one read. */
  class FragmentWindow(hits: Array[TaxonHit]) extends IndexedSeq[TaxonHit] {
    private var start = 0 //offsets in the hits array (inclusive)
    private var end = 0 // not inclusive

    val countSummary = new it.unimi.dsi.fastutil.ints.Int2IntArrayMap(16) //specialised, very fast map

    def advanceStart(to: Int): Unit = {
      while (start < hits.length && hits(start).ordinal < to) {
        start += 1
        val removed = hits(start - 1)
        countSummary.put(removed.taxon, countSummary.get(removed.taxon) - removed.count)
      }
    }

    def advanceEnd(to: Int, m: Int): Unit = {
      while (end < hits.length && hits(end).ordinal + m - 1 < to) {
        end += 1
        if (end < hits.length) {
          val added = hits(end)
          countSummary.put(added.taxon, countSummary.getOrDefault(added.taxon, 0) + added.count)
        }
      }
    }

    def apply(i: Int): TaxonHit = {
      hits(i + start)
    }

    override def length: Int =
      end - start

    override def iterator: Iterator[TaxonHit] = new Iterator[TaxonHit] {
      private var pos = start
      override def hasNext: Boolean =
        pos < end

      override def next(): TaxonHit = {
        val r = hits(pos)
        pos += 1
        r
      }
    }
  }

  /**
   * Generate reads from the fragment then classify them according to the LCAs.
   *
   * @param taxonomy   the taxonomy
   * @param minimizers all minimizers encountered in this fragment (to be paired with LCAs)
   * @param lcas       all LCAs of minimizers encountered in this fragment, in the same order as minimizers
   * @param splitter   the minimizer scheme
   * @param readLen    length of reads to be generated
   * @return an iterator of (source taxon, destination taxon, number of reads classified to destination taxon)
   */
  def readClassifications(taxonomy: Taxonomy, minimizers: Array[Array[Long]], lcas: Array[Taxon],
                          splitter: AnyMinSplitter, readLen: Int): Iterator[(Taxon, Taxon, Long)] = {

    val encodedMinimizers = minimizers.map(m => NTBitArray(m, splitter.priorities.width))

    // this map will contain a subset of the lca index that supports random access
    val lcaLookup = mutable.Map.empty[NTBitArray, Int]
    lcaLookup.sizeHint(minimizers.size)
    lcaLookup ++= encodedMinimizers.iterator.zip(lcas.iterator)

    val k = splitter.k

    val lca = new LowestCommonAncestor(taxonomy)
    val noWhitespace = nucleotides.replaceAll("\\s+", "")
    val segments = Supermers.splitByAmbiguity(noWhitespace, Supermers.nonAmbiguousRegex(k))
    var pos = 0

    //Adjust the position of each supermer to be across the entire fragment
    val allHits = segments.flatMap{
      case (seq, SEQUENCE_FLAG) =>
        val r = splitter.superkmerPositions(seq, addRC = false).map(x =>
          //Construct each minimizer hit.
          //Overloading the second argument (ordinal) to mean the absolute position in the fragment in this case
          TaxonHit(x._2.data, x._1 + pos, lcaLookup(x._2), x._3 - (k - 1))
        )
        pos += seq.length
        r
      case (seq, AMBIGUOUS_FLAG) =>
        pos += seq.length
        Iterator.empty
    }.toArray

    val hitWindow = new FragmentWindow(allHits)

    //For each window corresponding to a read (start and end),
    //classify the corresponding minimizers.
    Iterator.range(0, noWhitespace.length - readLen).map(start => { // inclusive
      val end = start + readLen //non inclusive
      hitWindow.advanceStart(start)
      hitWindow.advanceEnd(end, splitter.priorities.width)

      if (hitWindow.nonEmpty) {
        val dest = classify(lca, hitWindow, hitWindow.countSummary)
        (taxon, dest, 1L)
      } else (taxon, Taxonomy.NONE, 1L)
    })
  }

  /** Classify a single read efficiently */
  def classify(lca: LowestCommonAncestor, sortedHits: Seq[TaxonHit], summary: Int2IntMap): Taxon = {
    // confidence threshold is irrelevant for this purpose
    val confidenceThreshold = 0.0
    val minHitGroups = 2
    val taxon = lca.resolveTree(summary, confidenceThreshold)
    val classified = sufficientHitGroups(sortedHits, minHitGroups)
    if (classified) taxon else Taxonomy.NONE
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

  /**
   * Build Bracken weights for a given library.
   *
   * @param library        The genomes to simulate reads from
   * @param taxa           A taxon filter for the genomes (only included taxa will be simulated)
   * @param outputLocation File to write the results to
   */
  def buildWeights(library: GenomeLibrary, taxa: BitSet, outputLocation: String) = {

    val titlesTaxa = getTaxonLabels(library.labelFile).toDF("header", "taxon")

    val idSeqDF = library.inputs.getInputFragments(withRC = false, withAmbiguous = true)
    val presentTaxon = udf((x: Taxon) => taxa.contains(x))

    //Find all fragments of genomes
    val fragments = idSeqDF.join(titlesTaxa, List("header")).
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

    val brackenSourceLine = udf((source: Array[Taxon],counts: Array[Long],readCounts:Array[Long]) =>
      source.zip(counts).zip(readCounts).map{case ((s,c),r) => s"$s:$c:$r"}.mkString(" "))

    val sourceDestCounts = idMins.join(fragments, List("id")).
      select("id", "taxon", "nucleotides", "minimizers", "taxa").
      as[(String, Taxon, NTSeq, Array[Array[Long]], Array[Taxon])].
      flatMap { case (id, taxon, nts, ms, ts) =>
        val f = TaxonFragment(taxon, nts, id)
        f.readClassifications(bcTaxonomy.value, ms, ts, bcSplit.value, readLen)
      }.toDF("source","dest","count").groupBy("dest","source").agg(sum("count").as("count")).
      cache()

    val bySource = sourceDestCounts.groupBy("source").
      agg(sum("count").as("totalReads"))

//    bySource.agg(sum("totalReads")).show()

    val byDest = sourceDestCounts.join(bySource, List("source")).
      groupBy("dest").agg(
        collect_list("source").as("sources"),
        collect_list("count").as("counts"),
        collect_list("totalReads").as("totalReadsList")).
      select($"dest",brackenSourceLine($"sources",$"counts",$"totalReadsList")).
      as[(Taxon,String)].collect()

    sourceDestCounts.unpersist()
    HDFSUtil.usingWriter(outputLocation, wr => brackenWeightsReport(wr,byDest))

    def brackenWeightsReport(output: PrintWriter, brackenOut: Array[(Taxon, String)]): Unit = {
      val headers = s"mapped_taxid\tgenome_taxids:kmers_mapped:total_genome_kmers"
      output.println(headers)

      for { (dest, bLine) <- brackenOut } {
        output.println(s"${dest}\t${bLine}")
      }
    }
  }
}

