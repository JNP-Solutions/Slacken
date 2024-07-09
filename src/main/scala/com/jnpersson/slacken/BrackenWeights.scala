package com.jnpersson.slacken

import com.jnpersson.discount.{NTSeq, SeqLocation, SeqTitle}
import com.jnpersson.discount.spark.{AnyMinSplitter, HDFSUtil}
import com.jnpersson.discount.util.{KmerTable, NTBitArray}
import com.jnpersson.slacken.TaxonomicIndex.getTaxonLabels
import it.unimi.dsi.fastutil.ints.{Int2IntFunction, Int2IntMap}
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap
import org.apache.spark.sql.functions.{collect_list, sum, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

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
  def distinctMinimizers(splitter: AnyMinSplitter): Iterator[Array[Long]] = {
    val noWhitespace = nucleotides.replaceAll("\\s+", "")
    val segments = Supermers.splitByAmbiguity(noWhitespace, Supermers.nonAmbiguousRegex(splitter.k))
    val builder = KmerTable.builder(splitter.priorities.width, 10000, 1)

    for { (seq, flag) <- segments } {
      if (flag == SEQUENCE_FLAG) {
        val it = splitter.superkmerPositions(seq, addRC = false)
        while (it.hasNext) {
          builder.addLongs(it.next._2.data)
          builder.addLong(1) //count
        }
      }
    }

    builder.result(true).countedKmers.map(_._1)

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
    private var lastInWindow: TaxonHit = null //cache this for optimisation

    //Map taxon to k-mer count.
    //This mutable maps updates to reflect the current window.
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

    private var currentWindow: Vector[TaxonHit] = {
      val (window, rem) = hits.span(inWindow)
      hits = rem
      window.toVector
    }

    lastInWindow = currentWindow.last

    /** Hits currently in the window. */
    def hitsInWindow: Iterator[TaxonHit] =
      currentWindow.iterator

    //Populate the initial state
    for {
      h <- hitsInWindow
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
        currentWindow = currentWindow.tail
      }

      //Did a new hit move into the window?
      if (lastInWindow.ordinal + lastInWindow.count < windowEnd) {  //no longer touching the boundary
        if (hits.hasNext) currentWindow :+= hits.next
        lastInWindow = currentWindow.last
      }

      //increment one taxon
      countSummary.put(lastInWindow.taxon, countSummary.applyAsInt(lastInWindow.taxon) + 1)
    }

  }

  /** Generate all TaxonHits from the fragment by combining the LCA taxa with the minimizers,
   * building super-mers. */
  def taxonHits(minimizers: Array[Array[Long]], lcas: Array[Taxon],
                splitter: AnyMinSplitter) = {

    // this map will contain a subset of the lca to taxon index
    val lcaLookup = new Object2IntOpenHashMap[NTBitArray](minimizers.size)
    var i = 0
    while (i < minimizers.length) {
      val enc = NTBitArray(minimizers(i), splitter.priorities.width)
      lcaLookup.put(enc, lcas(i))
      i += 1
    }

    val k = splitter.k
    val segments = Supermers.splitByAmbiguity(nucleotides, Supermers.nonAmbiguousRegex(k))
    var pos = 0

    //Construct all super-mers, including quasi-supermers for ambiguous regions

    segments.flatMap {
      case (seq, SEQUENCE_FLAG) =>
        val r = splitter.superkmerPositions(seq, addRC = false).toList.map(x => {
          //Construct each minimizer hit.
          //Overloading the second argument (ordinal) to mean the absolute position in the fragment in this case
          TaxonHit(x._2.data, x._1 + pos, lcaLookup.applyAsInt(x._2), x._3 - (k - 1))
        })
        pos += seq.length - (k - 1)
        r
      case (seq, AMBIGUOUS_FLAG) =>
        val r = if (pos == 0) {
          List(TaxonHit(Array(), pos, Taxonomy.NONE, seq.length))
        } else {
          List(TaxonHit(Array(), pos, Taxonomy.NONE, seq.length + (k - 1)))
        }
        if (pos == 0) {
          pos += seq.length
        } else {
          //account for ambiguous hits from previous supermer group
          pos += seq.length + k - 1
        }
        r
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

      val inWindow = hitWindow.hitsInWindow
      if (inWindow.nonEmpty) {
        val dest = classify(lca, inWindow, hitWindow.countSummary)
        dest
      } else Taxonomy.NONE
    }).buffered

    /*
        Sum up consecutive identical classifications to reduce the amount of data generated.
    This exploits the fact that we know each fragment can only classify to relatively few taxa
    (must be some taxon in its lineage, which is usually < 30).
    At the same time this is computationally cheaper than building a full hash map and counting each distinct value
    (which Spark should do anyway)
     */

    new Iterator[(Taxon, Taxon, Long)] {
      def hasNext: Boolean =
        classifications.hasNext

      def next: (Taxon, Taxon, Long) = {
        val x = classifications.next
        var count = 1L
        while (classifications.hasNext && classifications.head == x) {
          classifications.next
          count += 1
        }
        (taxon, x, count)
      }
    }
  }

  /** Classify a single read efficiently.
   * This is a simplified version of [[TaxonomicIndex.classify()]].
   * @param lca LCA calculator
   * @param sortedHits hits in this read. Used for sufficientHitGroups only. Counts will not be used.
   * @param summary taxon to k-mer count lookup map for this read
   */
  def classify(lca: LowestCommonAncestor, sortedHits: Iterator[TaxonHit], summary: Int2IntMap): Taxon = {
    // confidence threshold is irrelevant for this purpose, as when we are self-classifying a library,
    // all the taxa that we hit should be in the same clade
    val confidenceThreshold = 0.0
    val minHitGroups = 2
    val reportTaxon = lca.resolveTree(summary, confidenceThreshold)
    val classified = sufficientHitGroups(sortedHits, minHitGroups)
    if (classified) reportTaxon else Taxonomy.NONE
  }

  /** For the given set of sorted hits, was there a sufficient number of hit groups wrt the given minimum?
   * This is a simplified version of [[TaxonomicIndex.sufficientHitGroups()]] for this special use case.
   */
  def sufficientHitGroups(sortedHits: Iterator[TaxonHit], minimum: Int): Boolean = {
    var hitCount = 0
    var lastMin: Array[Long] = Array()

    //count separate hit groups (adjacent but with different minimizers) for each sequence, imitating kraken2 classify.cc
    for { hit <- sortedHits } {
      if (hit.taxon != Taxonomy.NONE &&
        (hitCount == 0 || !java.util.Arrays.equals(hit.minimizer, lastMin))) {
        hitCount += 1
        if (hitCount >= minimum) return true
      }
      lastMin = hit.minimizer
    }
    false
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
   * @param library The genomes to simulate reads from
   * @param taxa    A taxon filter for the genomes (only included taxa will be simulated)
   * @return All classified reads, counted by destination and source pairs. The returned Dataframe will be cached
   *         and must be unpersisted after use.
   */
  def buildWeights(library: GenomeLibrary, taxa: BitSet) = {

    val titlesTaxa = getTaxonLabels(library.labelFile).toDF("header", "taxon")


    val idSeqDF = library.inputs.getInputFragments(withRC = false, withAmbiguous = true)
    val presentTaxon = udf((x: Taxon) => taxa.contains(x))

    /** Generate a unique fragment ID. Each seqId is unique by construction. */
    val fragmentId = udf((id: SeqTitle, loc: SeqLocation) => id + "|" + loc)

    //Prepare the sequence for super-mer splitting and encoding
    val noWhitespace = udf((x: NTSeq) => x.replaceAll("\\s+", ""))

    //Find all fragments of genomes
    val fragments = idSeqDF.join(titlesTaxa, List("header")).
      select($"taxon", noWhitespace($"nucleotides").as("nucleotides"), fragmentId($"header", $"location").as("id")).
      where(presentTaxon($"taxon")).
      as[(Taxon, NTSeq, String)].
      map(x => TaxonFragment(x._1, x._2, x._3))

    val bcSplit = keyValueIndex.bcSplit
    val bcTaxonomy = keyValueIndex.bcTaxonomy

    //Join fragment IDs with LCA taxa based on minimizers
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

    //Re-join with fragments again and classify all possible reads
    idMins.join(fragments, List("id")).
      select("id", "taxon", "nucleotides", "minimizers", "taxa").
      as[(String, Taxon, NTSeq, Array[Array[Long]], Array[Taxon])].
      flatMap { case (id, taxon, nts, ms, ts) =>
        val f = TaxonFragment(taxon, nts, id)
        f.readClassifications(bcTaxonomy.value, ms, ts, bcSplit.value, readLen)
      }.toDF("source","dest","count").groupBy("dest","source").agg(sum("count").as("count")).
      cache()
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
   */
  def buildAndWriteWeights(library: GenomeLibrary, taxa: BitSet, outputLocation: String) = {
    val reads = buildWeights(library, taxa)
    try {
      writeReport(groupData(reads), outputLocation)
    } finally {
      reads.unpersist()
    }
  }

  private val brackenSourceLine = udf((source: Array[Taxon], counts: Array[Long], readCounts: Array[Long]) =>
    source.zip(counts).zip(readCounts).map { case ((s, c), r) => s"$s:$c:$r" }.mkString(" "))

  /** Write a report from the calculated bracken weights.
   * Unpersists the data.
   */
  private def writeReport(collectedData: DataFrame, outputLocation: String): Unit = {
    //Form bracken output lines for each source taxon
    val data = collectedData.select($"dest", brackenSourceLine($"sources",$"counts",$"totalReadsList")).
      as[(Taxon, String)].collect()

    HDFSUtil.usingWriter(outputLocation, output => {
      val headers = "mapped_taxid\tgenome_taxids:kmers_mapped:total_genome_kmers"
      output.println(headers)

      for {(dest, bLine) <- data} {
        output.println(s"${dest}\t${bLine}")
      }
    })
  }
}

