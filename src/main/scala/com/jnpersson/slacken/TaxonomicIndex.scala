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
import com.jnpersson.discount.spark.{AnyMinSplitter, Discount, HDFSUtil, IndexParams}
import com.jnpersson.slacken.TaxonomicIndex.{ClassifiedRead, getTaxonLabels}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{count, desc}

import scala.collection.mutable

/** Parameters for a Kraken1/2 compatible taxonomic index for read classification. Associates k-mers with LCA taxons.
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

  /** Construct buckets for a new index from genomes.
   *  @param discount Discount object for input reading
   *  @param inFiles Files with genomic sequences to index
   *  @param seqLabelLocation Location of a file that labels each genome with a taxon
   *  @param addRC Whether to add reverse complements
   *  @return index buckets
   */
  def makeBuckets(discount: Discount, inFiles: List[String], seqLabelLocation: String,
                  addRC: Boolean): Dataset[Record] = {
    val input = discount.inputReader(inFiles: _*).getInputFragments(addRC).map(x =>
      (x.header, x.nucleotides))
   val seqLabels = getTaxonLabels(seqLabelLocation)
   makeBuckets(input, seqLabels)
  }

  /** Build index buckets
   * @param idsSequences Pairs of (genome title, genome)
   * @param taxonLabels Pairs of (genome title, taxon)
   */
  def makeBuckets(idsSequences: Dataset[(SeqTitle, NTSeq)], taxonLabels: Dataset[(SeqTitle, Taxon)]): Dataset[Record]

  def writeBuckets(buckets: Dataset[Record], location: String): Unit

  /** Load index bucket from the params location */
  def loadBuckets(): Dataset[Record] =
    loadBuckets(params.location)

  /** Load index buckets from the specified location */
  def loadBuckets(location: String): Dataset[Record]

  /** Classify subject sequences using the index configured at the IndexParams location */
  def classify(buckets: Dataset[Record], subjects: Dataset[InputFragment],
               minHitGroups: Int): Dataset[ClassifiedRead]

  /** Classify subject sequences using the index configured at the IndexParams location,
   * writing the results to a designated output location */
  def classifyAndWrite(subjects: Dataset[InputFragment], outputLocation: String, withUnclassified: Boolean,
                       minHitGroups: Int): Unit = {
    val cs = classify(loadBuckets(), subjects, minHitGroups)
    writeOutput(cs, outputLocation, withUnclassified)
  }

  /** Classify subject sequences using the given buckets, writing the results to a designated output location */
  def classifyAndWrite(buckets: Dataset[Record], subjects: Dataset[InputFragment], output: String,
                       withUnclassified: Boolean, minHitGroups: Int): Unit =
    writeOutput(classify(buckets, subjects, minHitGroups), output, withUnclassified)

  /**
   * Write classified reads to a directory, with the _classified suffix, as well as a kraken-style kreport.txt
   * @param reads classified reads
   * @param location directory/prefix to write to
   * @param withUnclassified whether to include unclassified reads in the output
   */
  def writeOutput(reads: Dataset[ClassifiedRead], location: String, withUnclassified: Boolean): Unit = {
    reads.cache
    val bcPar = bcTaxonomy
    try {
      val outputRows = if (withUnclassified) {
        reads.map(c => c.outputLine)
      } else {
        reads.where($"classified" === true).map(c => c.outputLine)
      }
      val countByTaxon = reads.groupBy("taxon").agg(count("*").as("count")).
        sort(desc("count")).as[(Taxon, Long)].collect()

      //This table will be relatively small and we coalesce mainly to avoid generating a lot of small files
      //in the case of a fine grained index with many partitions
      outputRows.coalesce(200).write.mode(SaveMode.Overwrite).
        text(s"${location}_classified")
      val report = new KrakenReport(bcPar.value, countByTaxon)
      val writer = HDFSUtil.getPrintWriter(s"${location}_kreport.txt")
      report.print(writer)
      writer.close()
    } finally {
      reads.unpersist()
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
   * @param hitMap Lookup for hit counts for each taxon in this read
   * @param summary Information about classified hit groups
   * @param sufficientHits Whether there are sufficient hits to classify the sequence
   * @param k Length of k-mers
   */
  def classify(taxonomy: Taxonomy, title: SeqTitle, hitMap: mutable.Map[Taxon, Int], summary: TaxonSummary,
               sufficientHits: Boolean, k: Int): ClassifiedRead = {
    val taxon = taxonomy.resolveTree(hitMap)
    val classified = taxon != Taxonomy.NONE && sufficientHits

    val reportTaxon = if (classified) taxon else Taxonomy.NONE
    ClassifiedRead(classified, title, reportTaxon, summary.lengthString(k), summary.groupsInOrder)
  }
}

