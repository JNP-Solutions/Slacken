/*
 * This file is part of Discount. Copyright (c) 2019-2023 Johan Nyström-Persson.
 *
 * Discount is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Discount is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Discount.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.jnpersson.slacken

import com.jnpersson.discount.{NTSeq, SeqTitle}
import com.jnpersson.discount.hash.{BucketId, InputFragment}
import com.jnpersson.discount.spark.Index.randomTableName
import com.jnpersson.discount.spark.IndexParams
import com.jnpersson.slacken.TaxonomicIndex.ClassifiedRead
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{broadcast, collect_list, collect_set, struct, udaf, udf}

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

  /** Given input genomes and their taxon IDs, build an index of minimizers and LCA taxa.
   * @param idsSequences pairs of (genome title, genome DNA)
   * @param seqLabels pairs of (genome title, taxon ID)
   */
  def makeBuckets(idsSequences: Dataset[(SeqTitle, NTSeq)],
                    seqLabels: Dataset[(SeqTitle, Taxon)]): Dataset[(BucketId, BucketId, Taxon)] = {
    val bcSplit = this.bcSplit

    val idSeqDF = idsSequences.toDF("seqId", "seq")
    val labels = seqLabels.toDF("seqId", "taxon")

    //Materialize labels before broadcasting it
    labels.count()

    val idSeqLabels = idSeqDF.join(broadcast(labels), idSeqDF("seqId") === labels("seqId")).
      select("seq", "taxon").as[(String, Taxon)]

    val LCAs = idSeqLabels.flatMap(r => {
      val splitter = bcSplit.value
      splitter.superkmerPositions(r._1, addRC = false).map { case (_, rank, _) =>
        (rank.data(0), rank.dataOrBlank(1), r._2)
      }
    })

    reduceLCAs(LCAs)
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
    val bcPar = this.bcTaxonomy

    val udafLca = udaf(TaxonLCA(bcPar))
    minimizersTaxa.toDF("id1", "id2", "taxon").
      groupBy("id1", "id2").
      agg(udafLca($"taxon").as("taxon")).as[(BucketId, BucketId, Taxon)]
  }

  /** Load buckets from the given location */
  def loadBuckets(location: String): Dataset[(BucketId, BucketId, Taxon)] = {
    //Does not delete the table itself, only removes it from the hive catalog
    //This is to ensure that we get the one in the expected location
    spark.sql("DROP TABLE IF EXISTS taxidx")
    spark.sql(s"""|CREATE TABLE taxidx(id1 long, id2 long, taxon int)
                  |USING PARQUET CLUSTERED BY (id1, id2) INTO $numIndexBuckets BUCKETS
                  |LOCATION '$location'
                  |""".stripMargin)
    spark.sql("SELECT id1, id2, taxon FROM taxidx").as[(BucketId, BucketId, Taxon)]
  }

  /** Union several indexes. The indexes must use the same splitter and taxonomy.
   */
  def unionIndexes(locations: Iterable[String], outputLocation: String): Unit = {
    val buckets = locations.map(l => loadBuckets(l)).reduce(_ union _)
    val compacted = reduceLCAs(buckets)
    writeBuckets(compacted, outputLocation)
  }

  /** Classify subject sequences using the supplied index (as a dataset) */
  def classify(buckets: Dataset[(BucketId, BucketId, Taxon)], subjects: Dataset[InputFragment],
               minHitGroups: Int): Dataset[ClassifiedRead] = {
    val bcSplit = this.bcSplit
    val k = this.k
    val bcPar = this.bcTaxonomy

    //Split input sequences by minimizer, preserving sequence ID and ordinal of the super-mer
    val taggedSegments = subjects.flatMap(s => {
      val splitter = bcSplit.value
      HashSegments.splitFragment(s, splitter).map(x => {
        //Drop the sequence data
        S2OrdinalSegment(x.segment.id1, x.segment.id2,
          x.segment.segment.size - (k - 1), x.flag, x.ordinal, x.seqTitle)
      })
    }).
      //The 'subject' struct constructs an S2OrdinalSegment.
      select($"id1", $"id2",
        struct($"id1", $"id2", $"kmers", $"flag", $"ordinal", $"seqTitle").as("subject"))

    val setTaxonUdf = udf(setTaxon(_, _)) //TODO don't return NONE
    //Shuffling of the index in this join can be avoided when the partitioning column
    //and number of partitions is the same in both tables
    val taxonHits = taggedSegments.join(buckets, List("id1", "id2"), "left"). //TODO inner join
      select($"subject.seqTitle".as("seqTitle"),
        setTaxonUdf($"taxon", $"subject").as("hit"))

    //Group all hits by sequence title again so that we can reassemble (the hits from) each sequence according
    // to the original order.
    taxonHits.groupBy("seqTitle").agg(collect_list("hit")).
      as[(SeqTitle, Array[TaxonHit])].map { case (title, hits) =>
      val sortedHits = hits.sortBy(_.ordinal)

      val sufficientHits = sufficientHitGroups(sortedHits, minHitGroups)
      val summariesInOrder = TaxonSummary.concatenate(sortedHits.map(_.summary)) //TODO rewrite
      val allHits = TaxonSummary.hitCountsToMap(List(summariesInOrder))

      //More detailed output format for debugging purposes, may be passed instead of summariesInOrder below to
      //see it in the final output
      //      hits.sortBy(_.ordinal).mkString(" ")

      TaxonomicIndex.classify(bcPar.value, title, allHits, summariesInOrder, sufficientHits, k)
    }
  }

  /** Print statistics for this index. */
  def showIndexStats(): Unit = {
    val indexBuckets = loadBuckets()
    println(s"${indexBuckets.count()} $m-minimizers in index")
    val leafTaxons = indexBuckets.agg(collect_set("taxon")).as[Set[Taxon]].collect()(0)
    println(s"${taxonomy.countDistinctTaxaWithParents(leafTaxons)} distinct taxa in index")
  }

  /** Generate a histogram showing the number of taxons at each depth in the index. */
  def depthHistogram(): Dataset[(Taxon, Long)] = {
    val indexBuckets = loadBuckets()
    val bct = bcTaxonomy

    indexBuckets.select("taxon").as[Taxon].mapPartitions(bs => {
      val tax = bct.value
      bs.map(t => tax.depth(t))
    }).toDF("depth").groupBy("depth").count().
      sort("depth").as[(Taxon, Long)]
  }
}

object KeyValueIndex {

  /** Load index from the given location */
  def load(location: String, taxonomyLocation: String)(implicit spark: SparkSession): KeyValueIndex = {
    val params = IndexParams.read(location)
    new KeyValueIndex(params, TaxonomicIndex.getTaxonomy(taxonomyLocation))
  }

  /** For a super-mer with a given minimizer, assign a taxon hit, handling ambiguity flags correctly
   * @param taxon The minimizer's LCA taxon
   * @param x The super-mer from the original sequence
   * */
  def setTaxon(taxon: Option[Taxon], x: S2OrdinalSegment): TaxonHit = {
    val reportTaxon =
      if (x.flag == AMBIGUOUS_FLAG) AMBIGUOUS
      else if (x.flag == MATE_PAIR_BORDER_FLAG) MATE_PAIR_BORDER
      else {
        taxon match {
          case Some(taxon) => taxon
          case None => Taxonomy.NONE
        }
      }
    TaxonHit(x.id1, x.id2, x.ordinal, reportTaxon, x.kmers)
  }

  /** For the given set of sorted hits, was there a sufficient number of hit groups wrt the given minimum? */
  def sufficientHitGroups(sortedHits: Array[TaxonHit], minimum: Int): Boolean = {
    var hitCount = 0
    var lastHash1 = sortedHits(0).id1
    var lastHash2 = sortedHits(0).id2

    //count separate hit groups (adjacent but with different minimizers) for each sequence, imitating kraken2 classify.cc
    for { hit <- sortedHits } {
      if (hit.taxon != AMBIGUOUS && hit.taxon != Taxonomy.NONE && hit.taxon != MATE_PAIR_BORDER &&
        (hitCount == 0 || (hit.id1 != lastHash1 || hit.id2 != lastHash2))) {
        hitCount += 1
      }
      lastHash1 = hit.id1
      lastHash2 = hit.id2
    }
    hitCount >= minimum
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
  def summary: TaxonSummary =
    TaxonSummary(ordinal, Array(taxon), Array(count))
}


/**
 * An aggregator that merges taxons of the same k-mer by applying the LCA function.
 */
final case class TaxonLCA(bcTaxonomy: Broadcast[Taxonomy]) extends Aggregator[Taxon, Taxon, Taxon] {
  override def zero: Taxon = Taxonomy.NONE

  @transient
  lazy val taxonomy = bcTaxonomy.value
  @transient
  private lazy val buffer = taxonomy.newPathBuffer

  override def reduce(b: Taxon, a: Taxon): Taxon = taxonomy.lca(buffer)(b, a)

  override def merge(b1: Taxon, b2: Taxon): Taxon = taxonomy.lca(buffer)(b1, b2)

  override def finish(reduction: Taxon): Taxon = reduction

  override def bufferEncoder: Encoder[Taxon] = Encoders.scalaInt

  override def outputEncoder: Encoder[Taxon] = Encoders.scalaInt
}


