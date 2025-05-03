/*
 * This file is part of Slacken. Copyright (c) 2019-2025 Johan Nyström-Persson.
 *
 * Slacken is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 *  Slacken is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 * along with Slacken.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.jnpersson.slacken

import com.jnpersson.kmers._
import com.jnpersson.kmers.util.KmerTable
import com.jnpersson.slacken.Taxonomy.NONE
import it.unimi.dsi.fastutil.ints.Int2IntMap
import it.unimi.dsi.fastutil.objects.Object2IntOpenCustomHashMap
import it.unimi.dsi.fastutil.longs.LongArrays.HASH_STRATEGY
import org.apache.spark.sql.functions.{collect_list, ifnull, lit, regexp_replace, sum, udf}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util
import scala.collection.mutable.ArrayBuffer
import scala.collection.{BitSet, mutable}


/**
 * A fragment of a genome.
 * @param taxon The taxon that this fragment came from
 * @param nucleotides The nucleotide sequence
 * @param header ID of the sequence this fragment came from
 * @param location Position in the sequence this fragment came from
 */
final case class TaxonFragment(taxon: Taxon, nucleotides: NTSeq, header: String, location: SeqLocation) {

  /** Split this fragment into multiple subfragments of a bounded maximum length.
   * The value of k will be respected, so that no k-mers will be lost. Consecutive
   * splits will overlap by k-1 letters.
   */
  def splitToMaxLength(max: Int, k: Int): Iterator[TaxonFragment] = {
    def safeEnd(end: Int) =
      if (end > nucleotides.length) nucleotides.length else end

    if (nucleotides.length <= max)
      Iterator(this)
    else
      //Each subfragment will contain (max - k) k-mers and (k-1) bps overlapping with the following subfragment
      for {
        start <- Iterator.range(0, nucleotides.length - k + 1, max - (k - 1)) //starting positions of k-mers
        f = TaxonFragment(taxon, nucleotides.substring(start, safeEnd(start + max)), header, location + start)
      } yield f
  }

  /**
   * Returns all distinct minimizers in the nucleotide sequence
   *
   * @param splitter the minimizer scheme
   * @param defaultValue pseudo-minimizer to return when the fragment has no true minimizers
   * @return
   */
  def distinctMinimizers(splitter: AnyMinSplitter, defaultValue: Array[Long]): Iterator[Array[Long]] = {
    val segments = Supermers.splitByAmbiguity(nucleotides, splitter.k)
    val builder = KmerTable.builder(splitter.priorities.width, 10000)

    for {
      (seq, flag, _) <- segments
      if flag == SEQUENCE_FLAG
      sm <- splitter.superkmerPositions(seq)
    } {
      builder.addLongs(sm.rank)
    }

    val r = builder.result(true).distinctKmers
    if (r.isEmpty) {
      //no valid minimizers in the segment
      Iterator(defaultValue)
    } else r
  }

  /** Sliding window corresponding to a list of hits. Each hit is a super-mer with some number of k-mers.
   * Each window position corresponds to one read.
   *
   * Assumptions:
   * Positions refer to k-mer starting positions.
   * hits are sorted in order ("ordinal" which means absolute position here).
   * Every k-mer in the fragment is accounted for in some hit.
   * NONE hits are inserted to account for ambiguous regions (quasi-supermers with the correct length).
   *
   * @param hits hits in the fragment
   */
  class FragmentWindow(private var hits: Iterator[TaxonHit], kmersPerWindow: Int) {

    //Offsets in the fragment
    private var windowStart = 0 //inclusive
    private var windowEnd = kmersPerWindow // not inclusive
    private var lastInWindow: TaxonHit = _ //cache this for optimisation

    //Map taxon to k-mer count.
    //This mutable map updates to reflect the current window.
    val countSummary = new it.unimi.dsi.fastutil.ints.Int2IntArrayMap(16) //specialised, very fast map

    //Is at least one k-mer from the hit contained in the window?
    //Compare the final possible k-mer start with the bounds.
    private def inWindow(hit: TaxonHit) =
      hit.ordinal < windowEnd

    private def inWindow(pos: Int) =
      pos >= windowStart && pos < windowEnd

    //Has the hit already passed through the window (i.e., is it behind it?)
    private def passedWindow(hit: TaxonHit) =
      hit.ordinal + (hit.count - 1) < windowStart

    val currentWindow: mutable.ArrayBuffer[TaxonHit] = {
      val (window, rem) = hits.span(inWindow)
      hits = rem
      window.to[ArrayBuffer]
    }

    lastInWindow = currentWindow.last

    //Populate the initial state
    for {
      h <- currentWindow
      kmerStart <- h.ordinal until h.ordinal + h.count
      if inWindow(kmerStart)
    } {
      countSummary.put(h.taxon, countSummary.applyAsInt(h.taxon) + 1)
    }


    /** Move the window one step forward. */
    def advance(): Unit = {
      //Decrement one taxon
      val remove = currentWindow.head

      val updated = countSummary.applyAsInt(remove.taxon) - 1
      if (updated > 0)
        countSummary.put(remove.taxon, updated)
      else
        countSummary.remove(remove.taxon)

      windowStart += 1
      windowEnd += 1

      //Did the first hit pass out of the window?
      if (passedWindow(currentWindow.head)) {
        currentWindow.remove(0)
      }

      //Did a new hit move into the window?
      if (lastInWindow.ordinal + lastInWindow.count < windowEnd) {  //no longer touching the boundary
        if (hits.hasNext) currentWindow += hits.next()
        lastInWindow = currentWindow.last
      }

      //increment one taxon
      countSummary.put(lastInWindow.taxon, countSummary.applyAsInt(lastInWindow.taxon) + 1)
    }

  }

  /** Generate all TaxonHits from the fragment by combining the LCA taxa with the minimizers,
   * building super-mers.
   * @param minimizers Minimizers in this fragment
   * @param lcas Lca taxa of the minimizers, as fetched from the index. Corresponds position by position to minimizers.
   * @param splitter The splitter
   */
  def taxonHits(minimizers: Array[Array[Long]], lcas: Array[Taxon],
                splitter: AnyMinSplitter): Iterator[TaxonHit] = {

    // this map will contain a subset of the lca to taxon index
    val lcaLookup = new Object2IntOpenCustomHashMap[Array[Long]](minimizers.length, HASH_STRATEGY)
    var i = 0
    while (i < minimizers.length) {
      if (lcas.length > 0)
        lcaLookup.put(minimizers(i), lcas(i)) //lcas can be empty for the default (empty) minimizer
      i += 1
    }

    val k = splitter.k
    val segments = Supermers.splitByAmbiguity(nucleotides, k)

    //Construct all super-mers, including quasi-supermers (NONE) for ambiguous regions

    var first = true
    var lastMinimizer = Array[Long]()
    segments.flatMap {
      case (seq, SEQUENCE_FLAG, pos) =>
        splitter.superkmerPositions(seq).map(x => {
          //Construct each minimizer hit.
          //Overloading the second argument (ordinal) to mean the absolute position in the fragment in this case

          val distinct = first || !util.Arrays.equals(x.rank, lastMinimizer)
          first = false
          lastMinimizer = x.rank
          TaxonHit(distinct, x.location + pos, lcaLookup.applyAsInt(x.rank), x.length - (k - 1))
        }) ++
          //additional invalid k-mers that go into the next ambiguous segment, or past the end.
          //The total k-mer count has to be correct, or we can't simulate all reads from the sequence later.
          Iterator(TaxonHit(false, seq.length - (k - 1), Taxonomy.NONE, k - 1))

      case (seq, AMBIGUOUS_FLAG, pos) =>
        Iterator(
          TaxonHit(false, pos, Taxonomy.NONE, seq.length)
        )
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

    val k = splitter.k
    val lca = new LowestCommonAncestor(taxonomy)

    val allHits = taxonHits(minimizers, lcas, splitter)
    val kmersInRead = readLen - (k - 1)
    val hitWindow = new FragmentWindow(allHits, kmersInRead)

    //For each window corresponding to a read (start and end),
    //classify the corresponding minimizers.
    val classifications = Iterator.range(0, nucleotides.length - readLen + 1).map(start => { // inclusive
      if (start > 0) hitWindow.advance()

      val inWindow = hitWindow.currentWindow
      if (inWindow.nonEmpty) {
        val dest = classify(lca, inWindow, hitWindow.countSummary)
        dest
      } else Taxonomy.NONE
    }).buffered

    /*
        Pre-sum consecutive identical classifications to reduce the total amount of data emitted to Spark.
    This exploits the fact that we know each fragment can only classify to relatively few taxa
    (must be some taxon in its lineage, which is usually < 30).
     */

    new Iterator[(Taxon, Taxon, Long)] {
      def hasNext: Boolean =
        classifications.hasNext

      def next: (Taxon, Taxon, Long) = {
        val x = classifications.next()
        var count = 1L
        while (classifications.hasNext && classifications.head == x) {
          classifications.next()
          count += 1
        }
        (taxon, x, count)
      }
    }
  }

  /** Classify a single read efficiently.
   * This is a simplified version of [[Classifier$.classify]].
   * @param lca LCA calculator
   * @param sortedHits hits in this read. Used for sufficientHitGroups only. Counts will not be used.
   * @param summary taxon to k-mer count lookup map for this read
   */
  def classify(lca: LowestCommonAncestor, sortedHits: IndexedSeq[TaxonHit], summary: Int2IntMap): Taxon = {
    // confidence threshold is irrelevant for this purpose, as when we are self-classifying a library,
    // all the taxa that we hit should be in the same clade
    val confidenceThreshold = 0.0
    val minHitGroups = 2
    val reportTaxon = lca.resolveTree(summary, confidenceThreshold)
    val classified = sufficientHitGroups(sortedHits, minHitGroups)
    if (classified) reportTaxon else Taxonomy.NONE
  }

  /** For the given set of sorted hits, was there a sufficient number of hit groups wrt the given minimum?
   * This is a simplified version of [[Classifier.sufficientHitGroups()]] for this special use case.
   */
  def sufficientHitGroups(sortedHits: IndexedSeq[TaxonHit], minimum: Int): Boolean = {
    var hitCount = 0

    var i = 0
    while (i < sortedHits.length) {
      //count separate hit groups (adjacent but with different minimizers) for each sequence, imitating kraken2 classify.cc
      val hit = sortedHits(i)
      if (hit.taxon != Taxonomy.NONE && hit.distinct) {
        hitCount += 1
        if (hitCount >= minimum) return true
      }
      i += 1
    }
    false
  }
}

/**
 * Generate bracken-style weights (self-classifying all reads of genomes in a library against the library).
 * This is intended to be fully compatible with Bracken for abundance reestimation. The outputs should be compatible
 * with those generated by bracken-build.
 * See: https://github.com/jenniferlu717/Bracken
 *
 * @param keyValueIndex minimizer LCAs to classify genomes against.
 * @param readLen       length of reads to be generated and classified
 * @param spark
 */
class BrackenWeights(keyValueIndex: KeyValueIndex, readLen: Int)(implicit val spark: SparkSession) {

  import spark.sqlContext.implicits._

  //Split into subfragments of maximum this length for performance reasons
  final val FRAGMENT_MAX = 1024 * 1024

  /**
   * For a set of taxa, generate all reads from their genomes and classify them against the library.
   *
   * @param library The genomes to simulate reads from
   * @param taxa    A taxon filter for the genomes (only included taxa will be simulated)
   * @return All classified reads, counted by destination and source pairs.
   */
  def buildWeights(library: GenomeLibrary, taxa: BitSet): DataFrame = {

    val titlesTaxa = library.getTaxonLabels.toDF("header", "taxon")

    val readLen = this.readLen
    val idSeqDF = library.inputs.withK(readLen).getInputFragments(withAmbiguous = true)
    val presentTaxon = udf((x: Taxon) => taxa.contains(x))

    //Find all fragments of genomes
    val fragments = idSeqDF.join(titlesTaxa, List("header")).
      select($"taxon",
        regexp_replace($"nucleotides", "\\s+", "").as("nucleotides"),
        $"header", $"location").
      where(presentTaxon($"taxon")).
      as[TaxonFragment].
      flatMap(_.splitToMaxLength(FRAGMENT_MAX, readLen))

    val bcSplit = keyValueIndex.bcSplit
    val bcTaxonomy = keyValueIndex.bcTaxonomy
    val emptyMinimizer = Array.fill(keyValueIndex.numIdColumns)(0L)
    val records = keyValueIndex.records

    //Join fragment IDs with LCA taxa based on minimizers
    val idMins = fragments.flatMap { x =>
        x.distinctMinimizers(bcSplit.value, emptyMinimizer).map(m => (x.header, x.location, m))
      }.toDF("header", "location", "minimizer").
      select(keyValueIndex.idColumnsFromMinimizer ++ Seq($"header", $"location") :_*).
      join(records, keyValueIndex.idColumnNames, "left"). //left join to preserve fragments with the empty minimizer (all invalid reads)
      groupBy("header", "location").agg(
        collect_list(keyValueIndex.minimizerColumnFromIdColumns),
        collect_list(ifnull($"taxon", lit(NONE)))
      ).
      toDF("header", "location", "minimizers", "taxa")

    //Re-join with fragments again and classify all possible reads
    idMins.join(fragments, List("header", "location")).
      select("header", "location", "taxon", "nucleotides", "minimizers", "taxa").
      as[(String, SeqLocation, Taxon, NTSeq, Array[Array[Long]], Array[Taxon])].
      flatMap { case (header, location, taxon, nts, ms, ts) =>
        val f = TaxonFragment(taxon, nts, header, location)
        f.readClassifications(bcTaxonomy.value, ms, ts, bcSplit.value, readLen)
      }.toDF("source","dest","count").groupBy("dest","source").agg(sum("count").as("count"))
  }

  /** For a set of taxa, gradually classify all reads from all genomes (to reduce the impact of node interruption),
   * writing the results to a temporary parquet table.
   * @param library source of genomes
   * @param taxa taxa to include
   * @param tempLocation location to write the temporary table
   * @return the resulting table
   */
  def buildWeightsGradually(library: GenomeLibrary, taxa: BitSet, tempLocation: String): DataFrame = {
    //Break the genomes up into chunks and gradually append to the table,
    //in order to reduce the impact of interrupted spark nodes
    for {group <- taxa.grouped(taxa.size / 5)} {
      val weights = buildWeights(library, group)
      weights.
        write.mode(SaveMode.Append).
        parquet(tempLocation)
    }
    spark.read.parquet(tempLocation)
  }

  /** Group the counted reads by source as well as destination and sub-count each. */
  def groupData(sourceDestCounts: DataFrame): DataFrame = {
    val bySource = sourceDestCounts.groupBy("source").
      agg(sum("count").as("totalReads"))

    //Form triplets for each destination taxon
    sourceDestCounts.join(bySource, List("source")).
      groupBy("dest").agg(
        collect_list("source").as("sources"),
        collect_list("count").as("counts"),
        collect_list("totalReads").as("totalReadsList"))
  }

  /**
   * Build Bracken weights for a given library and write them to a Bracken-compatible file.
   *
   * @param library        The genomes to simulate reads from
   * @param taxa           A taxon filter for the genomes (only included taxa will be simulated)
   * @param outputLocation File to write the results to
   * @param gradual        If true, weights will be computed gradually and appended to a temporary table,
   *                       making the job more resilient to interrupted nodes
   */
  def buildAndWriteWeights(library: GenomeLibrary, taxa: BitSet, outputLocation: String, gradual: Boolean = false): Unit = {
    val tempLocation = outputLocation + "_tmp"
    val reads =
      if (gradual)
        buildWeightsGradually(library, taxa, tempLocation)
      else
        buildWeights(library, taxa).cache()
    try {
      writeKmerDistrib(groupData(reads), outputLocation)
    } finally {
      reads.unpersist()
      HDFSUtil.deleteRecursive(tempLocation)
    }
  }

  private val brackenSourceLine = udf((source: Array[Taxon], counts: Array[Long], readCounts: Array[Long]) =>
    source.zip(counts).zip(readCounts).map { case ((s, c), r) => s"$s:$c:$r" }.mkString(" "))

  /** Write a kmer_distrib file from the calculated bracken weights.
   */
  private def writeKmerDistrib(collectedData: DataFrame, outputLocation: String): Unit = {
    //Form bracken output lines for each source taxon
    val data = collectedData.select($"dest", brackenSourceLine($"sources",$"counts",$"totalReadsList")).
      as[(Taxon, String)].collect()

    HDFSUtil.usingWriter(outputLocation, output => {
      val headers = "mapped_taxid\tgenome_taxids:kmers_mapped:total_genome_kmers"
      output.println(headers)

      for {(dest, bLine) <- data}
        output.println(s"$dest\t$bLine")
    })
  }
}

