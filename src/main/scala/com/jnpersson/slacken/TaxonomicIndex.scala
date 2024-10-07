/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */


package com.jnpersson.slacken

import com.jnpersson.kmers.minimizer._
import com.jnpersson.kmers._

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.{countDistinct, udf}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import scala.collection.mutable

/** Parameters for a Kraken1/2 compatible taxonomic index for read classification. Associates k-mers with LCA taxa.
 * @param records
 * @param params Parameters for k-mers, index bucketing and persistence
 * @param taxonomy The taxonomy
 * @tparam Record type of index records
 */
abstract class TaxonomicIndex[Record](val records: Dataset[Record], params: IndexParams,
                                      val taxonomy: Taxonomy)(implicit spark: SparkSession) {
  val sc: org.apache.spark.SparkContext = spark.sparkContext

  import spark.sqlContext.implicits._

  def split: AnyMinSplitter = bcSplit.value
  def bcSplit: Broadcast[AnyMinSplitter] = params.bcSplit
  def numIndexBuckets: Int = params.buckets
  def k: Int = split.k
  def m: Int = split.priorities.width

  lazy val bcTaxonomy = sc.broadcast(taxonomy)

  /** Sanity check input data */
  def checkInput(inputs: Inputs): Unit = {}

  def withRecords(records: Dataset[Record]): TaxonomicIndex[Record]

  /**
   * Construct records for a new index from genomes.
   *
   * @param library          Input data
   * @param addRC            Whether to add reverse complements
   * @param taxonFilter      Optionally limit input sequences to only taxa in this set (and their descendants)
   * @return index records
   */
  def makeRecords(library: GenomeLibrary, addRC: Boolean,
                  taxonFilter: Option[mutable.BitSet] = None)(implicit spark: SparkSession): Dataset[Record] = {
    val input = taxonFilter match {
      case Some(tf) =>
        val titlesTaxa = library.getTaxonLabels.
          filter(l => tf.contains(l._2)).as[(SeqTitle, Taxon)].toDF("header", "taxon").cache //TODO unpersist

        println("Construct dynamic records from:")
        titlesTaxa.select(countDistinct($"header"), countDistinct($"taxon")).show()

        library.inputs.getInputFragments(addRC).join(titlesTaxa, List("header")).
          select("taxon", "nucleotides").as[(Taxon, NTSeq)].
          repartition(numIndexBuckets, List() :_*)
      case None =>
        library.joinSequencesAndLabels(addRC)
    }

    val bcTax = this.bcTaxonomy
    val isValid = udf((t: Taxon) => bcTax.value.isDefined(t))
    val filtered = input.filter(isValid($"taxon"))
    makeRecords(filtered)
  }


  /**
   * Build index records
   *
   * @param taxaSequences Pairs of (taxon, genome)
   */
  def makeRecords(taxaSequences: Dataset[(Taxon, NTSeq)]): Dataset[Record]

  def writeRecords(location: String): Unit

  /** Respace this index to larger numbers of spaced seeds, creating a new index for
   * each value. This is possible because an index with s spaces contains all information necessary
   * to construct an index with s+x spaces (we effectively project it into the new space with some information loss)
   * Each new index will be written to a separate location.
   */
  def respaceMultiple(spaces: List[Int], outputLocation: String): Unit = {
    for {s <- spaces} {
      val idx = respace(s)
      val reg = "_s[0-9]+".r
      if (reg.findFirstIn(outputLocation).isEmpty) {
        throw new Exception(s"Unable to guess the correct output location for new indexes at: $outputLocation")
      }

      val outLoc = reg.replaceFirstIn(outputLocation, s"_s$s")
      idx.writeRecords(outLoc)
      Taxonomy.copyToLocation(params.location + "_taxonomy", outLoc + "_taxonomy")
      println(s"Stats for $outLoc")
      idx.withRecords(idx.loadRecords(outLoc)).showIndexStats(None)
    }
  }

  /** Remap this index to a larger number of spaces in the bit mask (irreversibly). */
  def respace(spaces: Int): TaxonomicIndex[Record]

  /** Load index records from the params location */
  def loadRecords(): Dataset[Record] =
    loadRecords(params.location)

  /** Load index records from the specified location */
  def loadRecords(location: String): Dataset[Record]

  /** Find TaxonHits from InputFragments and set their taxa, without grouping them by seqTitle. */
  def findHits(subjects: Dataset[InputFragment]): Dataset[TaxonHit]

  /** Find the number of distinct minimizers for each of the given taxa */
  def distinctMinimizersPerTaxon(taxa: Seq[Taxon]): Array[(Taxon, Long)]

  /** Classify subject sequences */
  def classify(subjects: Dataset[InputFragment]): Dataset[(SeqTitle, Array[TaxonHit])]

  def classifySpans(subjects: Dataset[OrdinalSpan]): Dataset[(SeqTitle, Array[TaxonHit])]

  /** K-mers or minimizers in this index (keys) sorted by taxon depth from deep to shallow */
  def kmersDepths: DataFrame

  /** Taxa in this index (values) together with their depths */
  def taxonDepths: Dataset[(Taxon, Int)]

  import GenomeLibrary.rankStrUdf

  def kmerDepthHistogram(): DataFrame = {
    kmersDepths.select("depth").groupBy("depth").count().
      sort("depth").
      withColumn("rank", rankStrUdf($"depth")).
      select("depth", "rank", "count")
  }

  def taxonDepthHistogram(): DataFrame = {
    taxonDepths.select("depth").groupBy("depth").count().
      sort("depth").
      withColumn("rank", rankStrUdf($"depth")).
      select("depth", "rank", "count")
  }

  /**
   * Write the histogram of this data to HDFS.
   * @param output Directory to write to (prefix name)
   */
  def writeDepthHistogram(output: String): Unit =
    kmerDepthHistogram().
      write.mode(SaveMode.Overwrite).option("sep", "\t").csv(s"${output}_taxonDepths")

  /** Print basic statistics for this index.
   * Optionally, input sequences and a label file can be specified, and they will then be checked against
   * the database.
   */
  def showIndexStats(genomes: Option[GenomeLibrary]): Unit

  /**
   * Produce reports describing the index.
   *
   * If genomeLib is not given, we produce Kraken-style quasi reports detailing:
   * 1) contents of the index in minimizers (_min_report)
   * 2) contents of the index in genomes (_genome_report)
   * 3) missing genomes that are not uniquely identifiable by the index (_missing)
   *
   * If genomeLib is given, we produce a new report format that describes the average total k-mer count and genome
   * sizes for each taxon.
   *
   * @param checkLabelFile sequence label file used to build the index
   * @param output        output filename prefix
   * @param genomelib     If supplied, produce new-style k-mer count reports, otherwise produce traditional reports
   */
  def report(checkLabelFile: Option[String],
             output: String, genomelib: Option[GenomeLibrary] = None): Unit
}

