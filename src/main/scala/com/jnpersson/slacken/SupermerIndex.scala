/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */


package com.jnpersson.slacken

import com.jnpersson.discount
import com.jnpersson.discount.bucket.{ReduceParams, Reducer, ReducibleBucket}
import com.jnpersson.discount.{NTSeq, SeqTitle}
import com.jnpersson.discount.hash.{BucketId, InputFragment}
import com.jnpersson.discount.spark.{AnyMinSplitter, Discount, GroupedSegments, Index, IndexParams, Output}
import com.jnpersson.discount.util.KmerTable.BuildParams
import com.jnpersson.discount.util.{KmerTable, KmerTableBuilder, NTBitArray, TagProvider}
import com.jnpersson.slacken.TaxonomicIndex.ClassifiedRead
import com.jnpersson.slacken.Taxonomy.NONE
import it.unimi.dsi.fastutil.longs.LongArrays
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{broadcast, collect_list, desc, first, struct, udaf, udf}

import scala.collection.mutable.ArrayBuffer

object SupermerIndex {
  def load(location: String, taxonomy: Taxonomy)(implicit spark: SparkSession): SupermerIndex = {
    val params = IndexParams.read(location)
    new SupermerIndex(params, taxonomy)
  }

  /** Build an empty SupermerIndex.
   * @param inFiles Input files used for minimizer ordering construction only
   */
  def empty(discount: Discount, taxonomyLocation: String, inFiles: List[String])
           (implicit spark: SparkSession): SupermerIndex = {
    val spl = discount.getSplitter(Some(inFiles))
    val params = IndexParams(spark.sparkContext.broadcast(spl), discount.partitions, "")
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
  import Supermers._

  import spark.sqlContext.implicits._

  assert(params.m <= 31) //Support for wider minimizers yet to be implemented

  def taggedToBuckets(segments: Dataset[(BucketId, Array[(NTBitArray, Taxon)])], k: Int): Dataset[ReducibleBucket] = {
    val bcTax = bcTaxonomy
    segments.map { case (hash, segments) =>
      val reducer = TaxonLCAReducer(ReduceParams(k, false, false), bcTax.value)
      val supermers = segments.map(_._1)
      val tags = segments.map(x => Array.fill(x._1.size - (k - 1))(x._2))
      ReducibleBucket(hash, supermers, tags).reduceCompact(reducer)
    }
  }

  def segmentsToBuckets(segments: Dataset[(discount.spark.HashSegment, Taxon)], k: Int): Dataset[ReducibleBucket] = {
    val bcTax = this.bcTaxonomy
    val udafLca = udaf(TaxonLCA(bcTax))

    val byHash = segments.toDF("segment", "taxon").groupBy($"segment")
    //Aggregate distinct supermers to remove any skew from repetitive data
    val pregrouped = byHash.agg(udafLca($"taxon").as("taxon"))
    val collected = pregrouped.groupBy($"segment.hash").
      agg(collect_list(struct($"segment.segment", $"taxon"))).
      as[(BucketId, Array[(NTBitArray, Taxon)])]
    taggedToBuckets(collected, k)
  }

  def makeBuckets(idsSequences: Dataset[(SeqTitle, NTSeq)],
                  taxonLabels: Dataset[(SeqTitle, Taxon)], method: LCAMethod): Dataset[TaxonBucket] = {
    if (method != LCAAtLeastTwo) {
      throw new Exception(s"The method $method is not yet supported in SupermerIndex")
    }

    val bcSplit = this.bcSplit
    val idSeqDF = idsSequences.toDF("seqId", "seq")
    val labels = taxonLabels.toDF("seqId", "taxon")
    val idSeqLabels = idSeqDF.join(labels, idSeqDF("seqId") === labels("seqId")).
      select("seq", "taxon").as[(String, Taxon)]

    val segments = idSeqLabels.flatMap(r => GroupedSegments.hashSegments(r._1, bcSplit.value).map(s => (s, r._2)))
    segmentsToBuckets(segments, bcSplit.value.k).toDF("id", "supermers", "taxa").as[TaxonBucket]
  }

  def writeBuckets(buckets: Dataset[TaxonBucket], location: String): Unit = {
    val bkts = buckets.toDF("id", "supermers", "tags").as[ReducibleBucket]
    val idx = new Index(params, bkts)
    idx.write(location)
  }

  def loadBuckets(location: String): Dataset[TaxonBucket] = {
    //Does not delete the table itself, only removes it from the hive catalog
    //This is to ensure that we get the one in the expected location
    spark.sql("DROP TABLE IF EXISTS sm_taxidx")
    spark.sql(
      s"""|CREATE TABLE sm_taxidx(id long, supermers array<struct<data: array<long>, size: int>>, tags array<array<int>>)
          |USING PARQUET CLUSTERED BY (id) INTO ${params.buckets} BUCKETS
          |LOCATION '$location'
          |""".stripMargin)
    spark.sql("SELECT id, supermers, tags as taxa FROM sm_taxidx").as[TaxonBucket]
  }

  def classify(buckets: Dataset[TaxonBucket], subjects: Dataset[InputFragment],
               cpar: ClassifyParams): Dataset[ClassifiedRead] = {
    val bcSplit = this.bcSplit
    val bcTax = this.bcTaxonomy
    val k = this.k

    println("Warning: SupermerIndex does not yet respect minHitGroups (to be implemented)")

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
      as[(BucketId, Array[OrdinalSupermer], Array[NTBitArray], Array[Array[Taxon]])]

    val taxonSummaries = subjectWithIndex.flatMap { data =>
      val tbkt = if (data._3 != null) TaxonBucket(data._1, data._3, data._4)
      else TaxonBucket(data._1, Array(), Array())
      tbkt.classifyKmers(k, data._2)
    }

    //Group by sequence ID
    taxonSummaries.groupBy("_1").agg(collect_list($"_2")).
      as[(String, Array[TaxonCounts])].map(x => {
      val summariesInOrder = TaxonCounts.concatenate(x._2.sortBy(_.ordinal))

      //Useful alternative for debugging
      //x._2.sortBy(_.order).mkString(" ")

      TaxonomicIndex.classify(bcTax.value, x._1, summariesInOrder, sufficientHits = true, 0, k)
    })
  }

  def showIndexStats(): Unit = {
    val i = Index.read(params.location)
    Output.showStats(i.stats())
  }

  override def kmersDepths(buckets: Dataset[TaxonBucket]): Dataset[(BucketId, BucketId, Int)] = {
    val parentMap = bcTaxonomy
    val k = this.k

    buckets.flatMap(b => {
      b.kmersDepths(k, parentMap.value).map(x => (x._1.data(0), x._1.dataOrBlank(1), x._2))
    }).toDF("id1", "id2", "depth").
      sort(desc("depth")).as[(BucketId, BucketId, Int)]
  }

  def taxonDepths(buckets: Dataset[TaxonBucket]): Dataset[(Taxon, Int)] = {
    ???
  }
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

  def classifyKmers(k: Int, subjects: Array[OrdinalSupermer]): Iterator[(SeqTitle, TaxonCounts)] = {
    val (sequence, ambiguousOrGap) = subjects.partition(_.flag == SEQUENCE_FLAG)

    val provider = new TagProvider {
      def tagWidth = 1
      override def writeForRowCol(row: Int, col: Int, to: KmerTableBuilder): Unit = {
        //subject, ordinal, column
        to.addLong(row.toLong << 32 | sequence(row).ordinal << 16 | col)
      }
    }
    //tags layout after left join: [subj row, subj ordinal, subj col], [0 const], [taxon]
    val subjectTable = KmerTable.fromSupermers(sequence.map(_.segment.segment),
      BuildParams(k, forwardOnly = false, sort = true), provider)

    val joint = KmerTableOps.leftJoinTables(subjectTable, kmerTable(k), 0, Taxonomy.NONE)
    val jointTags = joint.kmers.drop(joint.kmerWidth) //tag columns only
    LongArrays.radixSort(jointTags)

    val taxonRows = jointTags(0).indices.iterator.buffered
    //note jointTags is column-major

    new Iterator[(SeqTitle, TaxonCounts)] {
      def hasNext: Boolean =
        taxonRows.hasNext

      def next: (SeqTitle, TaxonCounts) = {
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
        case AMBIGUOUS_FLAG => AMBIGUOUS_SPAN
        case MATE_PAIR_BORDER_FLAG => MATE_PAIR_BORDER
        case _ => ???
      }
      val numKmers = x.segment.segment.size - (k - 1)
      (x.seqTitle, TaxonCounts.forTaxon(x.ordinal, taxon, numKmers))
    })
  }

  /**
   * From a non-empty buffered iterator, extract the prefix corresponding to one subject and ordinal,
   * constructing a TaxonSummary.
   * Since the rows (tag data) have been sorted, each taxon will only be encountered once while traversing.
   */
  private def getTaxonSummary(indices: BufferedIterator[Int], rows: Array[Array[Long]],
                              subjRow: Int, ordinal: Short): TaxonCounts = {
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
    TaxonCounts(ordinal.toInt, taxBuffer, countBuffer)
  }
}

final case class TaxonBucketStats(id: String, numKmers: Long, distinctTaxa: Long, avgDepth: Double)

/**
 * A [[Reducer]] that merges taxa of the same k-mer by applying the LCA function.
 * @param k k-mer length
 * @param taxonomy the taxonomy
 */
final case class TaxonLCAReducer(params: ReduceParams, taxonomy: Taxonomy) extends Reducer {

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
