/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */


package com.jnpersson.slacken

import com.jnpersson.discount.{NTSeq, SeqTitle}
import com.jnpersson.discount.hash.{BucketId, InputFragment}
import com.jnpersson.discount.spark.{AnyMinSplitter, Discount, HDFSUtil, IndexParams}
import com.jnpersson.discount.util.NTBitArray
import com.jnpersson.slacken.TaxonomicIndex.{ClassifiedRead, getTaxonLabels}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{count, desc, udf}

import scala.collection.mutable

/** A method for calculating LCA's (least common ancestors) */
sealed trait LCAMethod

/**
 * Standard Kraken/Kraken2 method. The LCA is assigned if at least two descendants share the k-mer/minimizer.
 * In addition, no node outside the LCA's clade will have it.
 */
case object LCAAtLeastTwo extends LCAMethod

/**
 * Stronger method. The LCA is assigned only if all genomes under the node share the k-mer/minimizer.
 * In addition, no node outside the LCA's clade will have it.
 */
case object LCARequireAll extends LCAMethod

/** Parameters for classification of reads
 * @param minHitGroups min number of hit groups
 * @param confidenceThreshold min. confidence score (fraction of k-mers/minimizers that must be in the classified
 *                            taxon's clade)
 * @param withUnclassified whether to include unclassified reads in the outputh
 */
case class ClassifyParams(minHitGroups: Int, confidenceThreshold: Double, withUnclassified: Boolean)

/** Parameters for a Kraken1/2 compatible taxonomic index for read classification. Associates k-mers with LCA taxa.
 * @param params Parameters for k-mers, index bucketing and persistence
 * @param taxonomy The taxonomy
 * @tparam Record type of index records
 */
abstract class TaxonomicIndex[Record](params: IndexParams, taxonomy: Taxonomy)(implicit spark: SparkSession) {
  val sc: org.apache.spark.SparkContext = spark.sparkContext

  import spark.sqlContext.implicits._

  def split: AnyMinSplitter = bcSplit.value
  def bcSplit: Broadcast[AnyMinSplitter] = params.bcSplit
  def numIndexBuckets: Int = params.buckets
  def k: Int = split.k
  def m: Int = split.priorities.width

  lazy val bcTaxonomy = sc.broadcast(taxonomy)

  /**
   * Construct buckets for a new index from genomes.
   *
   * @param discount         Discount object for input reading
   * @param inFiles          Files with genomic sequences to index
   * @param seqLabelLocation Location of a file that labels each genome with a taxon
   * @param addRC            Whether to add reverse complements
   * @param method           LCA calculation method
   * @return index buckets
   */
  def makeBuckets(discount: Discount, inFiles: List[String], seqLabelLocation: String,
                  addRC: Boolean, method: LCAMethod = LCAAtLeastTwo): Dataset[Record] = {
    val input = discount.inputReader(inFiles: _*).getInputFragments(addRC).map(x =>
      (x.header, x.nucleotides))
   val seqLabels = getTaxonLabels(seqLabelLocation)
   makeBuckets(input, seqLabels, method)
  }

  /**
   * Build index buckets
   *
   * @param idsSequences Pairs of (genome title, genome)
   * @param taxonLabels  Pairs of (genome title, taxon)
   * @param method       LCA calculation method
   */
  def makeBuckets(idsSequences: Dataset[(SeqTitle, NTSeq)], taxonLabels: Dataset[(SeqTitle, Taxon)],
                  method: LCAMethod): Dataset[Record]

  def writeBuckets(buckets: Dataset[Record], location: String): Unit

  /** Load index bucket from the params location */
  def loadBuckets(): Dataset[Record] =
    loadBuckets(params.location)

  /** Load index buckets from the specified location */
  def loadBuckets(location: String): Dataset[Record]

  /** Classify subject sequences using the index configured at the IndexParams location */
  def classify(buckets: Dataset[Record], subjects: Dataset[InputFragment],
               cpar: ClassifyParams): Dataset[ClassifiedRead]

  /** Classify subject sequences using the index configured at the IndexParams location,
   * writing the results to a designated output location */
  def classifyAndWrite(subjects: Dataset[InputFragment], outputLocation: String, cpar: ClassifyParams): Unit = {
    val cs = classify(loadBuckets(), subjects, cpar)
    writeOutput(cs, outputLocation, cpar)
  }

  /** Classify subject sequences using the given buckets, writing the results to a designated output location */
  def classifyAndWrite(buckets: Dataset[Record], subjects: Dataset[InputFragment], output: String,
                       cpar: ClassifyParams): Unit =
    writeOutput(classify(buckets, subjects, cpar), output, cpar)

  /**
   * Write classified reads to a directory, with the _classified suffix, as well as a kraken-style kreport.txt
   * @param reads classified reads
   * @param location directory/prefix to write to
   * @param cpar
   */
  def writeOutput(reads: Dataset[ClassifiedRead], location: String, cpar: ClassifyParams): Unit = {
    reads.cache
    val bcTax = bcTaxonomy
    try {
      val outputRows = if (cpar.withUnclassified) {
        reads.map(r => r.outputLine)
      } else {
        reads.where($"classified" === true).map(r => r.outputLine)
      }
      val countByTaxon = reads.groupBy("taxon").agg(count("*").as("count")).
        sort(desc("count")).as[(Taxon, Long)].collect()

      //This table will be relatively small and we coalesce mainly to avoid generating a lot of small files
      //in the case of a fine grained index with many partitions
      outputRows.coalesce(200).write.mode(SaveMode.Overwrite).
        text(s"${location}_classified")
      val report = new KrakenReport(bcTax.value, countByTaxon)
      val writer = HDFSUtil.getPrintWriter(s"${location}_kreport.txt")
      report.print(writer)
      writer.close()
    } finally {
      reads.unpersist()
    }
  }

  /** K-mers or minimizers in this index (keys) sorted by taxon depth from deep to shallow */
  def kmersDepths(buckets: Dataset[Record]): Dataset[(BucketId, BucketId, Int)]

  def depthHistogram(): Dataset[(Int, Long)] = {
    val indexBuckets = loadBuckets()
    kmersDepths(indexBuckets).select("depth").groupBy("depth").count().
      sort("depth").as[(Int, Long)]
  }

  /**
   * Write the histogram of this data to HDFS.
   * @param output Directory to write to (prefix name)
   */
  def writeDepthHistogram(output: String): Unit =
    depthHistogram().
      write.mode(SaveMode.Overwrite).option("sep", "\t").csv(s"${output}_taxonDepths")

  def writeKmerDepthOrdering(buckets: Dataset[Record], location: String): Unit = {
    val k = this.k
    kmersDepths(buckets).map(x => (NTBitArray.fromLong(x._1, k).toString, x._3)).
      write.option("sep", "\t").csv(s"${location}_minimizers")
  }

  /** Construct a taxon depth-based minimizer ordering of k-mers (here, k is assumed to equal m for the
   * new minimizer ordering). Deep (more specific) minimizers will appear first, having higher priority.
   * The resulting minimizer ordering is encoded as integers, so only k <= 15 is supported.
   * @param buckets Taxon bucket index to construct from
   * @param complete Whether to extend the set with k-mers that were not seen in the index, so as to
   *                 include all k-mers (4<sup>m</sup> values)
   */
  def minimizerDepthOrdering(buckets: Dataset[Record], complete: Boolean): Array[Int] = {
    assert(this.k <= 15)

    val k = this.k
    val decodeToInt = udf((x: Long) => (x >>> (64 - k * 2)).toInt)
    val counted = kmersDepths(buckets).select(decodeToInt($"id1")).as[Int].
      collect()
    if (!complete) {
      counted
    } else {
      val asSet = scala.collection.mutable.BitSet.empty ++ counted
      val notInSet = Iterator.range(0, 1 << (2 * k)).filter(x => !asSet.contains(x)).toArray
      counted ++ notInSet
    }
  }
}

object TaxonomicIndex {

  /**
   * Read a taxon label file (TSV format)
   * Maps sequence id to taxon id.
   * This file is expected to be small (the data will be broadcast)
   * @param file Path to the file
   * @return
   */
  def getTaxonLabels(file: String)(implicit spark: SparkSession): Dataset[(String, Taxon)] = {
    import spark.sqlContext.implicits._
    spark.read.option("sep", "\t").csv(file).
      map(x => (x.getString(0), x.getString(1).toInt))
  }

  /**
   * Read a taxonomy from a directory with NCBI nodes.dmp and names.dmp.
   * The files are expected to be small.
   * @return
   */
  def getTaxonomy(dir: String)(implicit spark: SparkSession): Taxonomy = {
    val nodes = HDFSUtil.getSource(s"$dir/nodes.dmp").
      getLines().map(_.split("\\|")).
      map(x => (x(0).trim.toInt, x(1).trim.toInt, x(2).trim))

    val names = HDFSUtil.getSource(s"$dir/names.dmp").
      getLines().map(_.split("\\|")).
      flatMap(x => {
        val nameType = x(3).trim
        if (nameType == "scientific name") {
          Some((x(0).trim.toInt, x(1).trim))
        } else None
      })

    Taxonomy.fromNodesAndNames(nodes.toArray, names)
  }

  /** A classified read.
   * @param classified Could the read be classified?
   * @param title Sequence title/ID
   * @param taxon The assigned taxon
   * @param lengthString Length of the classified sequence
   * @param hitDetails Human-readable details for the hits
   */
  final case class ClassifiedRead(classified: Boolean, title: SeqTitle, taxon: Taxon, lengthString: String,
                                  hitDetails: String) {
    def classifyFlag: String = if (!classified) "U" else "C"

    //Imitate the Kraken output format
    def outputLine: String = s"$classifyFlag\t$title\t$taxon\t$lengthString\t$hitDetails"
  }

  /** Classify a read.
   * @param taxonomy Parent map for taxa
   * @param title Sequence title/ID
   * @param summary Information about classified hit groups
   * @param sufficientHits Whether there are sufficient hits to classify the sequence
   * @param confidenceThreshold Minimum fraction of k-mers/minimizers that must be in the match (KeyValueIndex only)
   * @param k Length of k-mers
   */
  def classify(taxonomy: Taxonomy, title: SeqTitle, summary: TaxonCounts,
               sufficientHits: Boolean, confidenceThreshold: Double, k: Int): ClassifiedRead = {
    val taxon = taxonomy.resolveTree(summary, confidenceThreshold)
    val classified = taxon != Taxonomy.NONE && sufficientHits

    val reportTaxon = if (classified) taxon else Taxonomy.NONE
    ClassifiedRead(classified, title, reportTaxon, summary.lengthString(k), summary.groupsInOrder)
  }
}

