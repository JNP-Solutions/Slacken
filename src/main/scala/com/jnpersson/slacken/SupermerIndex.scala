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

import com.jnpersson.discount
import com.jnpersson.discount.bucket.{Reducer, ReducibleBucket}
import com.jnpersson.discount.{NTSeq, SeqTitle}
import com.jnpersson.discount.hash.{BucketId, InputFragment}
import com.jnpersson.discount.spark.{AnyMinSplitter, Discount, GroupedSegments, Index, IndexParams, Output}
import com.jnpersson.discount.util.{KmerTable, KmerTableBuilder, NTBitArray, TagProvider}
import com.jnpersson.slacken.TaxonomicIndex.ClassifiedRead
import com.jnpersson.slacken.Taxonomy.NONE
import it.unimi.dsi.fastutil.longs.LongArrays
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{broadcast, collect_list, desc, struct, udf}

import scala.collection.mutable.ArrayBuffer

object SupermerIndex {
  def load(location: String, taxonomyLocation: String)(implicit spark: SparkSession): SupermerIndex = {
    val params = IndexParams.read(location)
    new SupermerIndex(params, TaxonomicIndex.getTaxonomy(taxonomyLocation))
  }

  /** Build an empty SupermerIndex.
   * @param inFiles Input files used for minimizer ordering construction only
   * @param location Location used to store minimizer orderings only
   */
  def empty(discount: Discount, taxonomyLocation: String, inFiles: List[String],
            location: Option[String] = None)(implicit spark: SparkSession): SupermerIndex = {
    val spl = discount.getSplitter(Some(inFiles), location)
    val params = IndexParams(spark.sparkContext.broadcast(spl), discount.partitions, location.getOrElse(""))
    new SupermerIndex(params, TaxonomicIndex.getTaxonomy(taxonomyLocation))
  }
}

/** Metagenomic index compatible with the Kraken 1 algorithm.
 * For space efficiency, stores super-mers using the [[ReducibleBucket]]. Identical k-mers can be combined using
 * a LCA operation.
 * @param params Parameters for k-mers, index bucketing and persistence
 * @param taxonomy The taxonomy
 */
final class SupermerIndex(val params: IndexParams, taxonomy: Taxonomy)(implicit spark: SparkSession)
  extends TaxonomicIndex[TaxonBucket](params, taxonomy) {
  import HashSegments._

  import spark.sqlContext.implicits._

  assert(params.m <= 31) //Support for wider minimizers yet to be implemented

  def taggedToBuckets(segments: Dataset[(BucketId, Array[(NTBitArray, Taxon)])], k: Int): Dataset[ReducibleBucket] = {
    val bcTax = bcTaxonomy
    segments.map { case (hash, segments) =>
      val reducer = TaxonLCAReducer(k, bcTax.value)
      val supermers = segments.map(_._1)
      val tags = segments.map(x => Array.fill(x._1.size - (k - 1))(x._2))
      ReducibleBucket(hash, supermers, tags).reduceCompact(reducer)
    }
  }

  def segmentsToBuckets(segments: Dataset[(discount.spark.HashSegment, Taxon)], k: Int): Dataset[ReducibleBucket] = {
    val grouped = segments.groupBy($"_1.hash")
    val byHash = grouped.agg(collect_list(struct($"_1.segment", $"_2"))).
      as[(BucketId, Array[(NTBitArray, Taxon)])]
    taggedToBuckets(byHash, k)
  }

  def makeBuckets(idsSequences: Dataset[(SeqTitle, NTSeq)],
                  taxonLabels: Dataset[(SeqTitle, Taxon)]): Dataset[TaxonBucket] = {
    val bcSplit = this.bcSplit
    val idSeqDF = idsSequences.toDF("seqId", "seq")
    val labels = taxonLabels.toDF("seqId", "taxon")
    val idSeqLabels = idSeqDF.join(broadcast(labels), idSeqDF("seqId") === labels("seqId")).
      select("seq", "taxon").as[(String, Taxon)]

    val segments = idSeqLabels.flatMap(r => GroupedSegments.hashSegments(r._1, bcSplit.value).map(s => (s, r._2)))
    segmentsToBuckets(segments, bcSplit.value.k).toDF("id", "supermers", "taxa").as[TaxonBucket]
  }

  def writeBuckets(buckets: Dataset[TaxonBucket], location: String): Unit = {
    val bkts = buckets.toDF("id", "supermers", "taxons").as[ReducibleBucket]
    val idx = new Index(params, bkts)
    idx.write(location)
  }

  def loadBuckets(location: String): Dataset[TaxonBucket] = {
    //Does not delete the table itself, only removes it from the hive catalog
    //This is to ensure that we get the one in the expected location
    spark.sql("DROP TABLE IF EXISTS taxidx")
    spark.sql(s"""|CREATE TABLE taxidx(id long, supermers array<struct<data: array<long>, size: int>>, tags array<array<int>>)
                  |USING PARQUET CLUSTERED BY (id) INTO ${params.buckets} BUCKETS
                  |LOCATION '$location'
                  |""".stripMargin)
    spark.sql(s"SELECT id, supermers, tags as taxa FROM taxidx").as[TaxonBucket]
  }

  def classify(buckets: Dataset[TaxonBucket], subjects: Dataset[InputFragment],
               minHitGroups: Int): Dataset[ClassifiedRead] = {
    val bcSplit = this.bcSplit
    val bcPar = this.bcTaxonomy
    val k = this.k

    println(s"Warning: SupermerIndex does not yet respect minHitGroups (to be implemented)")

    //Segments will be tagged with sequence ID
    val taggedSegments = subjects.flatMap(s => splitFragment(s, bcSplit.value)).
      //aliasing the hash column before grouping (rather than after) avoids an unnecessary
      // shuffle in the join with indexBuckets further down
      select($"segment.id1".as("id"), $"segment", $"ordinal", $"flag", $"seqTitle").
      groupBy("id").
      agg(collect_list(struct("segment", "flag", "ordinal", "seqTitle")).as("subjects"))

    //Join segments with index buckets.
    //Shuffling of the index in this join can be avoided when the partitioning column
    //and number of partitions is the same in both tables.
    //We do a left join to ensure that there's some result for each part of the input to be classified.
    val subjectWithIndex = taggedSegments.join(buckets, List("id"), "left").
      select("id", "subjects", "supermers", "taxa").
      as[(BucketId, Array[OrdinalSegmentWithSequence], Array[NTBitArray], Array[Array[Taxon]])]


    val taxonSummaries = subjectWithIndex.flatMap { data =>
      val tbkt = if (data._3 != null) TaxonBucket(data._1, data._3, data._4)
      else TaxonBucket(data._1, Array(), Array())
      tbkt.classifyKmers(k, data._2)
    }

    //Group by sequence ID
    taxonSummaries.groupBy("_1").agg(collect_list($"_2")).
      as[(String, Array[TaxonSummary])].map(x => {
      val summariesInOrder = TaxonSummary.concatenate(x._2.sortBy(_.ordinal))

      val allHits = TaxonSummary.hitCountsToMap(Seq(summariesInOrder))

      //Useful alternative for debugging
      //x._2.sortBy(_.order).mkString(" ")

      TaxonomicIndex.classify(bcPar.value, x._1, allHits, summariesInOrder, sufficientHits = true, k)
    })
  }

  def showIndexStats(): Unit = {
    val i = Index.read(params.location)
    Output.showStats(i.stats())
  }

  def depthHistogram(): Dataset[(Int, Long)] = {
    val indexBuckets = loadBuckets()
    val parentMap = bcTaxonomy
    val k = this.k

    indexBuckets.flatMap(b => {
      b.depths(k, parentMap.value)
    }).toDF("depth").groupBy("depth").count().
      sort("depth").as[(Int, Long)]
  }

  def kmerDepthTable(): Dataset[(NTSeq, BucketId)] =
    kmerDepthTable(loadBuckets())

  def kmerDepthTable(buckets: Dataset[TaxonBucket]): Dataset[(NTSeq, BucketId)] = {
    val parentMap = bcTaxonomy
    val k = this.k

    buckets.flatMap(b => {
      b.kmersDepths(k, parentMap.value).map(x => (x._1.toString, x._2))
    }).toDF("kmer", "depth").
      sort(desc("depth")).as[(NTSeq, Long)]
  }

  def writeKmerDepthOrdering(buckets: Dataset[TaxonBucket], location: String): Unit =
    kmerDepthTable(buckets).
      write.option("sep", "\t").csv(s"${location}_minimizers")

  /** Construct a taxon depth-based minimizer ordering of k-mers (here, k is assumed to equal m for the
   * new minimizer ordering). Deep (more specific) minimizers will appear first, having higher priority.
   * The resulting minimizer ordering is encoded as integers, so only k <= 15 is supported.
   * @param buckets Taxon bucket index to construct from
   * @param complete Whether to extend the set with k-mers that were not seen in the index, so as to
   *                 include all k-mers (4<sup>m</sup> values)
   */
  def minimizerDepthOrdering(buckets: Dataset[TaxonBucket], complete: Boolean): Array[Int] = {
    assert(k <= 15)
    val encToInt = udf((x: String) => NTBitArray.encode(x).toInt)

    val counted = kmerDepthTable(buckets).select(encToInt($"kmer")).as[Int].
      collect()
    if (!complete) {
      counted
    } else {
      val asSet = scala.collection.mutable.BitSet.empty ++ counted
      val notInSet = Iterator.range(0, 1 << (2 * k)).filter(x => !asSet.contains(x)).toArray
      counted ++ notInSet
    }
  }

  /**
   * Write the histogram of this data to HDFS.
   * @param output Directory to write to (prefix name)
   */
  def writeDepthHistogram(output: String): Unit =
    depthHistogram().
      write.mode(SaveMode.Overwrite).option("sep", "\t").csv(s"${output}_taxonDepths")

}

/**
 * A bucket where each k-mer is tagged with a taxon.
 * K-mers are sorted.
 *
 * @param id hash/minimizer of the bucket
 * @param supermers super-mers
 * @param taxa taxon annotations of each k-mer in super-mers
 */
final case class TaxonBucket(id: BucketId, supermers: Array[NTBitArray], taxa: Array[Array[Taxon]]) {

  def kmerTable(k: Int): KmerTable =
    ReducibleBucket(id, supermers, taxa).writeToSortedTableNoRowCol(k, forwardOnly = false)

  //potential improvement: only read the necessary column off disk when computing this
  def depths(k: Int, taxonomy: Taxonomy): Iterator[Int] = {
    for {
      i <- Iterator.range(0, supermers.length)
      sm = supermers(i)
      tx = taxa(i)
      offset <- Iterator.range(0, sm.size - k + 1)
      tax = tx(offset)
      if tax != NONE
      depth = taxonomy.depth(tax)
    } yield depth
  }

  /** An iterator of (k-mer, taxonomic depth) pairs where the root level has depth zero. */
  def kmersDepths(k: Int, taxonomy: Taxonomy): Iterator[(NTBitArray, Int)] = {
    /* Possible concern here:
    Does the depth need to be normalized in some way? Different taxa
    may not have the same number of steps down to species/strain level
     */
    for {
      i <- Iterator.range(0, supermers.length)
      sm = supermers(i)
      tx = taxa(i)
      offset <- Iterator.range(0, sm.size - k + 1)
      tax = tx(offset)
      if tax != NONE
      depth = taxonomy.depth(tax)
      kmer = sm.sliceAsCopy(offset, k)
    } yield (kmer, depth)
  }

  def stats(splitter: AnyMinSplitter, taxonomy: Taxonomy): TaxonBucketStats = {
    val allTaxa = taxa.flatten
    val depths = allTaxa.map(taxonomy.depth(_))
    val avgDepth = depths.map(_.toDouble).sum / allTaxa.length
    TaxonBucketStats(splitter.humanReadable(id), kmerTable(splitter.k).size, allTaxa.distinct.length, avgDepth)
  }

  def classifyKmers(k: Int, subjects: Array[OrdinalSegmentWithSequence]): Iterator[(SeqTitle, TaxonSummary)] = {
    val (sequence, ambiguousOrGap) = subjects.partition(_.flag == SEQUENCE_FLAG)

    val provider = new TagProvider {
      def tagWidth = 1
      override def writeForRowCol(row: Int, col: Int, to: KmerTableBuilder): Unit = {
        //subject, ordinal, column
        to.addLong(row.toLong << 32 | sequence(row).ordinal << 16 | col)
      }
    }
    //tags layout after left join: [subj row, subj ordinal, subj col], [0 const], [taxon]
    val subjectTable = KmerTable.fromSupermers(sequence.map(_.segment.segment), k, forwardOnly = false,
      sort = true, provider)

    val joint = KmerTableOps.leftJoinTables(subjectTable, kmerTable(k), 0, Taxonomy.NONE)
    val jointTags = joint.kmers.drop(joint.kmerWidth) //tag columns only
    LongArrays.radixSort(jointTags)

    val taxonRows = jointTags(0).indices.iterator.buffered
    //note jointTags is column-major

    new Iterator[(SeqTitle, TaxonSummary)] {
      def hasNext: Boolean =
        taxonRows.hasNext

      def next: (SeqTitle, TaxonSummary) = {
        val row = taxonRows.head
        val keys = jointTags(0)(row)

        val subjectRow = (keys >> 32).toInt
        val ordinal = ((keys >> 16) & Short.MaxValue).toShort
        val subjectId = sequence(subjectRow).seqTitle

        //consume the prefix of taxonRows corresponding to the first subject and ordinal
        (subjectId, getTaxonSummary(taxonRows, jointTags, subjectRow, ordinal))
      }
    } ++ ambiguousOrGap.iterator.map(x => {
      val taxon = x.flag match {
        case AMBIGUOUS_FLAG => AMBIGUOUS
        case MATE_PAIR_BORDER_FLAG => MATE_PAIR_BORDER
        case _ => ???
      }
      val numKmers = x.segment.segment.size - (k - 1)
      (x.seqTitle, TaxonSummary.forTaxon(x.ordinal, taxon, numKmers))
    })
  }

  /**
   * From a non-empty buffered iterator, extract the prefix corresponding to one subject and ordinal,
   * constructing a TaxonSummary.
   * Since the rows (tag data) have been sorted, each taxon will only be encountered once while traversing.
   */
  private def getTaxonSummary(indices: BufferedIterator[Int], rows: Array[Array[Long]],
                              subjRow: Int, ordinal: Short): TaxonSummary = {
    val taxBuffer = new ArrayBuffer[Taxon](20)
    val countBuffer = new ArrayBuffer[Taxon](20)
    var taxon = rows(1)(indices.head)
    var count = 0

    while(indices.hasNext &&
      (rows(0)(indices.head) >> 32).toInt == subjRow &&
      ((rows(0)(indices.head) >> 16) & Short.MaxValue) == ordinal) {
      val top = rows(1)(indices.next)
      if (top == taxon) {
        count += 1
      } else {
        taxBuffer += taxon.toInt
        countBuffer += count
        taxon = top
        count = 1
      }
    }
    taxBuffer += taxon.toInt
    countBuffer += count
    TaxonSummary(ordinal.toInt, taxBuffer, countBuffer)
  }
}

final case class TaxonBucketStats(id: String, numKmers: Long, distinctTaxa: Long, avgDepth: Double)

/**
 * A [[Reducer]] that merges taxons of the same k-mer by applying the LCA function.
 * @param k k-mer length
 * @param taxonomy the taxonomy
 */
final case class TaxonLCAReducer(k: Int, taxonomy: Taxonomy) extends Reducer {
  def forwardOnly = false

  val tagOffset: Taxon = KmerTable.longsForK(k) + 1

  override val zeroValue: Taxon = Taxonomy.NONE

  override def shouldKeep(table: KmerTable, kmer: Int): Boolean =
    table.kmers(tagOffset)(kmer) != Taxonomy.NONE

  //Reusable buffer for the LCA operation
  private val buffer = taxonomy.newPathBuffer

  def reduceEqualKmers(table: KmerTable, into: Int, from: Int): Unit = {
    val tax1 = table.kmers(tagOffset)(from).toInt
    val tax2 = table.kmers(tagOffset)(into).toInt
    table.kmers(tagOffset)(into) = taxonomy.lca(buffer)(tax1, tax2)
    //Discard this k-mer on compaction
    table.kmers(tagOffset)(from) = Taxonomy.NONE
  }
}
