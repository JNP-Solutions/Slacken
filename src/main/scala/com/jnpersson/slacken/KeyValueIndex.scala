/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */

package com.jnpersson.slacken

import com.jnpersson.discount.hash.{BucketId, InputFragment}
import com.jnpersson.discount.spark.Index.randomTableName
import com.jnpersson.discount.spark.Output.formatPerc
import com.jnpersson.discount.spark.{Discount, HDFSUtil, IndexParams}
import com.jnpersson.discount.{NTSeq, SeqTitle}
import com.jnpersson.slacken.TaxonomicIndex.{ClassifiedRead, getTaxonLabels}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

import scala.collection.mutable


/** Metagenomic index compatible with the Kraken 2 algorithm.
 * This index does not store super-mers, but instead stores k-mers and taxa as key-value pairs.
 * The taxa of identical k-mers can be combined using the LCA method.
 * @param params Parameters for k-mers, index bucketing and persistence
 * @param taxonomy The taxonomy
 */
final class KeyValueIndex(val params: IndexParams, taxonomy: Taxonomy)(implicit spark: SparkSession)
  extends TaxonomicIndex[(BucketId, BucketId, Taxon)](params, taxonomy) {

  import KeyValueIndex._
  import spark.sqlContext.implicits._

  override def checkInput(discount: Discount, inFiles: List[String]): Unit = {
    val fragments = discount.inputReader(inFiles: _*).getInputFragments(false).map(x =>
      (x.header, x.nucleotides))

    /* Check if there are input sequences with no valid minimizers.
    * If so, report them.  */
    val spl = bcSplit
    //count minimizers per input sequence ID
    val noMinInput = fragments.map(r => {
      val splitter = spl.value
      (splitter.superkmerPositions(r._2, addRC = false).size.toLong, r._1)
      }
    ).toDF("minimizers", "seqId").groupBy("seqId").agg(
      functions.sum("minimizers").as("sum")).filter($"sum" === 0L).cache()
    if (! noMinInput.isEmpty) {
      val noMinCount = noMinInput.count()
      println(s"Some input sequences had no minimizers (total $noMinCount): ")
      noMinInput.show()
    }
  }

  /** Given input genomes and their taxon IDs, build an index of minimizers and LCA taxa.
   * @param idsSequences pairs of (genome title, genome DNA)
   * @param seqLabels pairs of (genome title, taxon ID)
   * @param method LCA calculation method
   */
  def makeBuckets(idsSequences: Dataset[(SeqTitle, NTSeq)],
                    seqLabels: Dataset[(SeqTitle, Taxon)], method: LCAMethod): Dataset[(BucketId, BucketId, Taxon)] = {
    val bcSplit = this.bcSplit

    val idSeqDF = idsSequences.toDF("seqId", "seq")
    val labels = seqLabels.toDF("seqId", "taxon")

    val seqTaxa = idSeqDF.join(labels, idSeqDF("seqId") === labels("seqId")).
      select("seq", "taxon").as[(NTSeq, Taxon)]

    val lcas = seqTaxa.flatMap(r => {
      val splitter = bcSplit.value
      splitter.superkmerPositions(r._1, addRC = false).map { case (_, rank, _) =>
        (rank.data(0), rank.dataOrBlank(1), r._2)
      }
    })

    method match {
      case LCAAtLeastTwo =>
        reduceLCAs(lcas)
      case LCARequireAll =>
        //Filter out sequence IDs that are not present in the input data, so that the genome counts
        //will reflect only data that is actually being considered. E.g. we could be building a small library
        //using a large label file, and then counts based purely on the label file would not be valid.
        val onlyPresentLabels = idSeqDF.join(labels, List("seqId")).
          select("seqId", "taxon")
        val gc = GenomeCounts.build(taxonomy, onlyPresentLabels)
        reduceLCAsRequireAll(lcas, gc).
          toDF("id1", "id2", "taxon").as[(BucketId, BucketId, Taxon)]
    }
  }
  /** Write buckets to the given location */
  def writeBuckets(buckets: Dataset[(BucketId, BucketId, Taxon)], location: String): Unit = {
    params.write(location, s"Properties for Slacken KeyValueIndex $location")
    println(s"Saving index into ${params.buckets} partitions")

    //A unique table name is needed to make saveAsTable happy, but we will not need it again
    //when we read the index back (by HDFS path)
    val tableName = randomTableName
    /*
     * Use saveAsTable instead of ordinary parquet save to preserve buckets/partitioning.
     */
    buckets.
      write.mode(SaveMode.Overwrite).
      option("path", location).
      bucketBy(params.buckets, "id1", "id2").
      saveAsTable(tableName)
  }

  /** Given non-combined pairs of minimizers and taxa, combine them using the lowest common ancestor (LCA)
   * function to return only one LCA taxon per minimizer.
   * @param minimizersTaxa tuples of (minimizer part 1, minimizer part 2, taxon)
   * @return tuples of (minimizer part 1, minimizer part 2, LCA taxon)
   */
  def reduceLCAs(minimizersTaxa: Dataset[(BucketId, BucketId, Taxon)]): Dataset[(BucketId, BucketId, Taxon)] = {
    val bcTax = this.bcTaxonomy

    val udafLca = udaf(TaxonLCA(bcTax))
    minimizersTaxa.toDF("id1", "id2", "taxon").
      groupBy("id1", "id2").
      agg(udafLca($"taxon").as("taxon")).as[(BucketId, BucketId, Taxon)]
  }

  /** Given non-combined pairs of minimizers and taxa, combine them using the lowest common ancestor (LCA)
   * function to return only one LCA taxon per minimizer,
   * returning the result if and only if all genomes in the LCA taxon have this minimizer (not just two or more)
   *
   * @param minimizersTaxa tuples of (minimizer part 1, minimizer part 2, taxon)
   * @param genomeCounts counts of distinct genomes per taxon
   * @return tuples of (minimizer part 1, minimizer part 2, LCA taxon)
   */
  def reduceLCAsRequireAll(minimizersTaxa: Dataset[(BucketId, BucketId, Taxon)],
                           genomeCounts: Array[Long]): Dataset[(BucketId, BucketId, Taxon)] = {
    val bcTax = this.bcTaxonomy
    val bcCounts = spark.sparkContext.broadcast(genomeCounts)

    val udafLca = udaf(TaxonLCA(bcTax))
    val preFilter = minimizersTaxa.toDF("id1", "id2", "taxon").
      groupBy("id1", "id2").
      agg(udafLca($"taxon").as("taxon"), functions.count_distinct($"taxon").as("count"))

    val isAboveExpected = udf((x: Taxon, count: Long) => {
      val expected = bcCounts.value
      count >= expected(x)
    })

    preFilter.filter(isAboveExpected($"taxon", $"count")).select("id1", "id2", "taxon").
      as[(BucketId, BucketId, Taxon)]
  }

  /** Load buckets from the given location */
  def loadBuckets(location: String): Dataset[(BucketId, BucketId, Taxon)] = {
    //Does not delete the table itself, only removes it from the hive catalog
    //This is to ensure that we get the one in the expected location
    spark.sql("DROP TABLE IF EXISTS kv_taxidx")
    spark.sql(s"""|CREATE TABLE kv_taxidx(id1 long, id2 long, taxon int)
                  |USING PARQUET CLUSTERED BY (id1, id2) INTO $numIndexBuckets BUCKETS
                  |LOCATION '$location'
                  |""".stripMargin)
    spark.sql("SELECT id1, id2, taxon FROM kv_taxidx").as[(BucketId, BucketId, Taxon)]
  }

  /** Union several indexes. The indexes must use the same splitter and taxonomy.
   */
  def unionIndexes(locations: Iterable[String], outputLocation: String): Unit = {
    val buckets = locations.map(loadBuckets).foldLeft(spark.emptyDataset[(BucketId, BucketId, Taxon)])(_ union _)
    val compacted = reduceLCAs(buckets)
    writeBuckets(compacted, outputLocation)
  }

  /** Classify subject sequences using the supplied index (as a dataset) */
  def classify(buckets: Dataset[(BucketId, BucketId, Taxon)], subjects: Dataset[InputFragment],
               cpar: ClassifyParams): Dataset[ClassifiedRead] = {
    val k = this.k
    val bcTax = this.bcTaxonomy
    val bcSplit = this.bcSplit

    //Split input sequences by minimizer, preserving sequence ID and ordinal of the super-mer
    val taggedSegments = subjects.mapPartitions(fs => {
      val supermers = new Supermers(bcSplit.value)
      fs.flatMap(s =>
        supermers.splitFragment(s).map(x =>
          //Drop the sequence data
          OrdinalSpan(x.segment.id1, x.segment.id2,
            x.segment.segment.size - (k - 1), x.flag, x.ordinal, x.seqTitle)
        )
      )
    }).
      //The 'subject' struct constructs an S2OrdinalSegment.
      select($"id1", $"id2",
        struct($"id1", $"id2", $"kmers", $"flag", $"ordinal", $"seqTitle").as("subject"))

    val setTaxonUdf = udf(setTaxon(_, _))
    //Shuffling of the index in this join can be avoided when the partitioning column
    //and number of partitions is the same in both tables
    val taxonHits = taggedSegments.join(buckets, List("id1", "id2"), "left").
      select($"subject.seqTitle".as("seqTitle"),
        setTaxonUdf($"taxon", $"subject").as("hit"))

    //Group all hits by sequence title again so that we can reassemble (the hits from) each sequence according
    // to the original order.
    taxonHits.groupBy("seqTitle").agg(collect_list("hit")).
      as[(SeqTitle, Array[TaxonHit])].map { case (title, hits) =>
      val sortedHits = hits.sortBy(_.ordinal)

      val sufficientHits = sufficientHitGroups(sortedHits, cpar.minHitGroups)
      val summariesInOrder = TaxonCounts.concatenate(sortedHits.map(_.summary))

      TaxonomicIndex.classify(bcTax.value, title, summariesInOrder, sufficientHits, cpar.confidenceThreshold, k)
    }
  }

  /** Print statistics for this index. */
  def showIndexStats(): Unit = {
    val indexBuckets = loadBuckets()
    val allTaxa = indexBuckets.groupBy("taxon").agg(count("taxon")).as[(Taxon, Long)].collect()

    val leafTaxa = allTaxa.filter(x => taxonomy.isLeafNode(x._1))
    val treeSize = taxonomy.countDistinctTaxaWithAncestors(allTaxa.map(_._1))
    println(s"Tree size: $treeSize taxa, stored taxa: ${allTaxa.size}, of which ${leafTaxa.size} " +
      s"leaf taxa (${formatPerc(leafTaxa.size.toDouble/allTaxa.size)})")

    val recTotal = allTaxa.map(_._2).sum
    val leafTotal = leafTaxa.map(_._2).sum
    println(s"Total $m-minimizers: $recTotal, of which leaf records: $leafTotal (${formatPerc(leafTotal.toDouble/recTotal)})")
  }

  /**
   * Produce Kraken-style quasi reports detailing:
   * 1) contents of the index in minimizers (_min_report)
   * 2) contents of the index in genomes (_genome_report)
   * 3) missing genomes that are not uniquely identifiable by the index (_missing)
   *
   * @param checkLabelFile sequence label file used to build the index
   * @param output         output filename prefix
   */
  def report(checkLabelFile: Option[String], output: String): Unit = {
    val indexBuckets = loadBuckets()
    //Report the contents of the index, count minimizers
    val allTaxa = indexBuckets.groupBy("taxon").agg(count("taxon")).as[(Taxon, Long)].collect()
    HDFSUtil.usingWriter(output + "_min_report.txt", wr =>
      new KrakenReport(taxonomy, allTaxa).print(wr)
    )

    //count of 1 per genome
    HDFSUtil.usingWriter(output + "_genome_report.txt", wr =>
      new KrakenReport(taxonomy, allTaxa.map(t => (t._1, 1L))).print(wr)
    )

    //Report missing genomes that were present in the input label file but are not in the index
    for { labels <- checkLabelFile } {
      val presentTaxa = allTaxa.iterator.map(_._1)
      val inputTaxa = getTaxonLabels(labels).select("_2").distinct().as[Taxon].collect()
      //count of 1 per genome
      val missingLeaf = (mutable.BitSet.empty ++ inputTaxa -- presentTaxa).toArray.map(t => (t, 1L))
      HDFSUtil.usingWriter(output + "_missing_report.txt", wr =>
        new KrakenReport(taxonomy, missingLeaf).print(wr)
      )
    }
  }

  /** An iterator of (k-mer, taxonomic depth) pairs where the root level has depth zero. */
  def kmersDepths(buckets: Dataset[(BucketId, BucketId, Taxon)]): Dataset[(BucketId, BucketId, Int)] = {
    val bcTax = this.bcTaxonomy
    val depth = udf((x: Taxon) => bcTax.value.depth(x))
    buckets.select($"id1", $"id2", depth($"taxon").as("depth")).
      sort(desc("depth")).as[(BucketId, BucketId, Int)]
  }

  def taxonDepths(buckets: Dataset[(BucketId, BucketId, Taxon)]): Dataset[(Taxon, Int)] = {
    val bcTax = this.bcTaxonomy
    val depth = udf((x: Taxon) => bcTax.value.depth(x))
    buckets.select($"taxon").distinct.select($"taxon", depth($"taxon").as("depth")).
      sort(desc("depth")).as[(Taxon, Int)]
  }
}

object KeyValueIndex {
  /** Load index from the given location */
  def load(location: String, taxonomy: Taxonomy)(implicit spark: SparkSession): KeyValueIndex = {
    val params = IndexParams.read(location)
    new KeyValueIndex(params, taxonomy)
  }

  /** For a super-mer with a given minimizer, assign a taxon hit, handling ambiguity flags correctly
   * @param taxon The minimizer's LCA taxon
   * @param segment The super-mer from the original sequence
   * */
  def setTaxon(taxon: Option[Taxon], segment: OrdinalSpan): TaxonHit = {
    val reportTaxon =
      if (segment.flag == AMBIGUOUS_FLAG) AMBIGUOUS_SPAN
      else if (segment.flag == MATE_PAIR_BORDER_FLAG) MATE_PAIR_BORDER
      else taxon.getOrElse(Taxonomy.NONE)
    TaxonHit(segment.id1, segment.id2, segment.ordinal, reportTaxon, segment.kmers)
  }

  /** For the given set of sorted hits, was there a sufficient number of hit groups wrt the given minimum? */
  def sufficientHitGroups(sortedHits: Array[TaxonHit], minimum: Int): Boolean = {
    var hitCount = 0
    var lastHash1 = sortedHits(0).id1
    var lastHash2 = sortedHits(0).id2

    //count separate hit groups (adjacent but with different minimizers) for each sequence, imitating kraken2 classify.cc
    for { hit <- sortedHits } {
      if (hit.taxon != AMBIGUOUS_SPAN && hit.taxon != Taxonomy.NONE && hit.taxon != MATE_PAIR_BORDER &&
        (hitCount == 0 || (hit.id1 != lastHash1 || hit.id2 != lastHash2))) {
        hitCount += 1
      }
      lastHash1 = hit.id1
      lastHash2 = hit.id2
    }
    hitCount >= minimum
  }

  /** Build an empty KeyValueIndex.
   * @param inFiles Input files used for minimizer ordering construction only
   */
  def empty(discount: Discount, taxonomyLocation: String, inFiles: List[String])
           (implicit spark: SparkSession): KeyValueIndex = {
    val spl = discount.getSplitter(Some(inFiles))
    val params = IndexParams(spark.sparkContext.broadcast(spl), discount.partitions, "")
    new KeyValueIndex(params, TaxonomicIndex.getTaxonomy(taxonomyLocation))
  }
}

/** A single hit group for a taxon and some number of k-mers
 * @param id1 minimizer part 1 (upper 64 bits)
 * @param id2 minimizer part 2 (lower 64 bits)
 * @param ordinal the position of this hit in the sequence of hits in the query sequence
 *              (not same as position in sequence)
 * @param taxon the classified LCA taxon
 * @param count the number of k-mer hits
 * */
final case class TaxonHit(id1: BucketId, id2: BucketId, ordinal: Int, taxon: Taxon, count: Int) {
  def summary: TaxonCounts =
    TaxonCounts(ordinal, Array(taxon), Array(count))
}


/**
 * An aggregator that merges taxa of the same k-mer by applying the LCA function.
 */
final case class TaxonLCA(bcTaxonomy: Broadcast[Taxonomy]) extends Aggregator[Taxon, Taxon, Taxon] {
  override def zero: Taxon = Taxonomy.NONE

  @transient
  lazy val taxonomy = bcTaxonomy.value

  @transient
  private lazy val lca = new LowestCommonAncestor(taxonomy)

  override def reduce(b: Taxon, a: Taxon): Taxon = lca(b, a)

  override def merge(b1: Taxon, b2: Taxon): Taxon = lca(b1, b2)

  override def finish(reduction: Taxon): Taxon = reduction

  override def bufferEncoder: Encoder[Taxon] = Encoders.scalaInt

  override def outputEncoder: Encoder[Taxon] = Encoders.scalaInt
}


