/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */

package com.jnpersson.slacken

import com.jnpersson.discount.hash.{InputFragment, MinSplitter, SpacedSeed}
import com.jnpersson.discount.spark.Helpers.randomTableName
import com.jnpersson.discount.spark.Output.formatPerc
import com.jnpersson.discount.spark.{AnyMinSplitter, HDFSUtil, IndexParams, Inputs, KmerKeyedIndex, SparkTool}
import com.jnpersson.discount.util.NTBitArray
import com.jnpersson.discount.{NTSeq, SeqTitle}
import com.jnpersson.slacken.TaxonomicIndex.getTaxonLabels
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
final class KeyValueIndex(val params: IndexParams, taxonomy: Taxonomy)(implicit val spark: SparkSession)
  extends TaxonomicIndex[Row](params, taxonomy) with KmerKeyedIndex {
  import spark.sqlContext.implicits._

  lazy val recordColumnNames: Seq[String] = idColumnNames :+ "taxon"

  lazy val idLongs = NTBitArray.longsForSize(params.m)

  override def checkInput(inputs: Inputs): Unit = {
    val fragments = inputs.getInputFragments(withRC = false).map(x => (x.header, x.nucleotides))

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
    } else {
      println("Input sequences checked, all had minimizers.")
    }
  }

  def findMinimizers(seqTaxa: Dataset[(Taxon, NTSeq)]): DataFrame = {
    val bcSplit = this.bcSplit

    numIdColumns match {
      case 1 =>
        seqTaxa.flatMap(r => {
          bcSplit.value.superkmerPositions(r._2, addRC = false).map { case (_, rank, _) =>
            (rank.data(0), r._1)
          }
        }).toDF(recordColumnNames: _*)
      case 2 =>
        seqTaxa.flatMap(r => {
          bcSplit.value.superkmerPositions(r._2, addRC = false).map { case (_, rank, _) =>
            (rank.data(0), rank.data(1), r._1)
          }
        }).toDF(recordColumnNames: _*)
      case 3 =>
        seqTaxa.flatMap(r => {
          bcSplit.value.superkmerPositions(r._2, addRC = false).map { case (_, rank, _) =>
            (rank.data(0), rank.data(1), rank.data(2), r._1)
          }
        }).toDF(recordColumnNames: _*)
      case 4 =>
        seqTaxa.flatMap(r => {
          bcSplit.value.superkmerPositions(r._2, addRC = false).map { case (_, rank, _) =>
            (rank.data(0), rank.data(1), rank.data(2), rank.data(3), r._1)
          }
        }).toDF(recordColumnNames: _*)
      case _ =>
        //In case of minimizers wider than 128 bp (4 longs), expand this section
        ???
    }
  }

  /** Given input genomes and their taxon IDs, build an index of minimizers and LCA taxa.
   * @param taxaSequences pairs of (taxon, genome DNA)
   */
  def makeBuckets(taxaSequences: Dataset[(Taxon, NTSeq)]): DataFrame =
    reduceLCAs(findMinimizers(taxaSequences))

  /** Write buckets to the given location */
  def writeBuckets(buckets: DataFrame, location: String): Unit = {
    params.write(location, s"Properties for Slacken KeyValueIndex $location")
    println(s"Saving index into ${params.buckets} partitions at $location")

    //A unique table name is needed to make saveAsTable happy, but we will not need it again
    //when we read the index back (by HDFS path).
    val tableName = randomTableName

    //Use saveAsTable instead of ordinary parquet save to preserve buckets/partitioning.
    buckets.
      write.mode(SaveMode.Overwrite).
      option("path", location).
      bucketBy(params.buckets, idColumnNames(0), idColumnNames.drop(1): _*).
      saveAsTable(tableName)
  }

  /** Map buckets into a new set of buckets where a larger number of spaces have been applied
   * in the spaced seed mask. Loses information, as the new index is expected to be smaller (this is a
   * dimensionality reduction).
   * @param buckets buckets to map
   * @param spaces new number of spaces
   * @return A new KeyValueIndex with identical parameters to this one (except for spaces) and the new set of buckets
   */
  def respace(buckets: DataFrame, spaces: Int): (KeyValueIndex, DataFrame) = {

    val newPriorities = params.splitter.priorities match {
      case SpacedSeed(s, inner) =>
        if (spaces <= s) {
          throw new Exception(s"Respacing to a smaller or identical number of spaces is not meaningful. (was $s, requested $spaces)")
        }
        SpacedSeed(spaces, inner)
      case p => SpacedSeed(spaces, p)
    }
    val newSplitter: AnyMinSplitter = MinSplitter(newPriorities, k)
    val bcSpl = spark.sparkContext.broadcast(newSplitter)
    val bcSs = spark.sparkContext.broadcast(newPriorities)
    val newParams = params.copy(bcSpl, params.buckets, "")

    val applySpaceUdf = udf((data: Array[Long]) => {
      val min = NTBitArray(data, bcSs.value.width)
      bcSs.value.maskSpacesOnly(min).data
    })

    val bcTax = this.bcTaxonomy
    val udafLca = udaf(TaxonLCA(bcTax))

    val nbuckets = buckets.select(applySpaceUdf(array(idColumns :_*)).as("minimizer"), $"taxon").
      select($"taxon" +: idColumnsFromMinimizer :_*).
      groupBy(idColumns: _*).
      agg(udafLca($"taxon").as("taxon"))

    (new KeyValueIndex(newParams, taxonomy), nbuckets)
  }

  /** Given non-combined pairs of minimizers and taxa, combine them using the lowest common ancestor (LCA)
   * function to return only one LCA taxon per minimizer.
   * @param minimizersTaxa tuples of (minimizer part 1, minimizer part 2, taxon)
   * @return tuples of (minimizer part 1, minimizer part 2, LCA taxon)
   */
  def reduceLCAs(minimizersTaxa: DataFrame): DataFrame = {
    val bcTax = this.bcTaxonomy
    val udafLca = udaf(TaxonLCA(bcTax))
    minimizersTaxa.
      groupBy(idColumns: _*).
      agg(udafLca($"taxon").as("taxon"))
  }

  /** Load buckets from the given location */
  def loadBuckets(location: String): DataFrame = {
    //Does not delete the table itself, only removes it from the hive catalog
    //This is to ensure that we get the one in the expected location
    spark.sql("DROP TABLE IF EXISTS kv_taxidx")
    spark.sql(s"""|CREATE TABLE kv_taxidx($idColumnsTypes, taxon int)
                  |USING PARQUET CLUSTERED BY ($idColumnsString) INTO $numIndexBuckets BUCKETS
                  |LOCATION '$location'
                  |""".stripMargin)
    spark.sql(s"SELECT $idColumnsString, taxon FROM kv_taxidx")
  }

  def getSpans(subjects: Dataset[InputFragment], withTitle: Boolean): Dataset[OrdinalSpan] = {
    val bcSplit = this.bcSplit
    val k = this.k
    val ni = numIdColumns

    //Split input sequences by minimizer, optionally preserving ordinal of the super-mer and optionally sequence ID
    subjects.mapPartitions(fs => {
      val supermers = new Supermers(bcSplit.value, ni)
      fs.flatMap(s =>
        supermers.splitFragment(s).map(x =>
          //Drop the sequence data
          if (withTitle)
            OrdinalSpan(x.segment.minimizer,
              x.segment.segment.size - (k - 1), x.flag, x.ordinal, x.seqTitle)
          else
            OrdinalSpan(x.segment.minimizer,
              x.segment.segment.size - (k - 1), x.flag, x.ordinal, null)
        )
      )
    })
  }

  /** Find TaxonHits from InputFragments and set their taxa, without grouping them by seqTitle. */
  def findHits(buckets: DataFrame, subjects: Dataset[InputFragment]): Dataset[TaxonHit] = {
    val spans = getSpans(subjects, withTitle = false)
    //The 'subject' struct constructs an OrdinalSpan
    val taggedSpans = spans.select(
      struct($"minimizer", $"kmers", $"flag", $"ordinal", $"seqTitle").as("subject") +:
        idColumnsFromMinimizer
        :_*)
    val setTaxonUdf = udf((tax: Option[Taxon], span: OrdinalSpan) => span.toHit(tax))

    //Shuffling of the index in this join can be avoided when the partitioning column
    //and number of partitions is the same in both tables
    taggedSpans.join(buckets, idColumnNames, "left").
      select(setTaxonUdf($"taxon", $"subject").as("hit")).
      select($"hit.*").as[TaxonHit]
  }

  def distinctMinimizersPerTaxa(buckets: DataFrame, taxa: Seq[Taxon]): Array[(Taxon, Long)] = {
    val precalcLocation = s"${params.location}_distinctMinimizers"
    if (!HDFSUtil.fileExists(precalcLocation)) {
      /** Precompute these values and store them for reuse later */
      println(s"$precalcLocation didn't exist, creating now.")
      buckets.
        groupBy("taxon").agg(functions.count_distinct(idColumns.head, idColumns.tail :_*).as("count")).
        coalesce(200).
        write.mode(SaveMode.Overwrite).option("sep", "\t").csv(precalcLocation)
    }
    spark.read.option("sep", "\t").csv(precalcLocation).map(x =>
      (x.getString(0).toInt, x.getString(1).toLong)).
      toDF("taxon", "count").
      join(taxa.toDF("taxon"), List("taxon")).as[(Taxon, Long)].
      collect()
  }

  /** Classify subject sequences using the supplied index (as a dataset) */
  def classify(buckets: DataFrame, subjects: Dataset[InputFragment]): Dataset[(SeqTitle, Array[TaxonHit])] =
    classifySpans(buckets, getSpans(subjects, withTitle = true))

  def classifySpans(buckets: DataFrame, subjects: Dataset[OrdinalSpan]): Dataset[(SeqTitle, Array[TaxonHit])] = {
    //The 'subject' struct constructs an OrdinalSpan
    val taggedSpans = subjects.select(
      struct($"minimizer", $"kmers", $"flag", $"ordinal", $"seqTitle").as("subject") +:
        idColumnsFromMinimizer
        :_*)
    val setTaxonUdf = udf((tax: Option[Taxon], span: OrdinalSpan) => span.toHit(tax))

    //Shuffling of the index in this join can be avoided when the partitioning column
    //and number of partitions is the same in both tables
    val taxonHits = taggedSpans.join(buckets, idColumnNames, "left").
      select($"subject.seqTitle".as("seqTitle"),
        setTaxonUdf($"taxon", $"subject").as("hit"))

    //Group all hits by sequence title again so that we can reassemble (the hits from) each sequence according
    // to the original order.

    taxonHits.groupBy("seqTitle").agg(collect_list("hit").as("hits")).
      as[(SeqTitle, Array[TaxonHit])]
  }

  def showIndexStats(indexBuckets: DataFrame, genomes: Option[GenomeLibrary]): Unit = {
    val allTaxa = indexBuckets.groupBy("taxon").agg(count("taxon")).as[(Taxon, Long)].collect()

    val leafTaxa = allTaxa.filter(x => taxonomy.isLeafNode(x._1))
    val treeSize = taxonomy.countDistinctTaxaWithAncestors(allTaxa.map(_._1))
    println(s"Tree size: $treeSize taxa, stored taxa: ${allTaxa.size}, of which ${leafTaxa.size} " +
      s"leaf taxa (${formatPerc(leafTaxa.size.toDouble/allTaxa.size)})")

    val recTotal = allTaxa.map(_._2).sum
    val leafTotal = leafTaxa.map(_._2).sum
    println(s"Total $m-minimizers: $recTotal, of which leaf records: $leafTotal (${formatPerc(leafTotal.toDouble/recTotal)})")
    for { library <- genomes} showTaxonCoverageStats(indexBuckets, library)
  }

  /** For each genome in the input sequences, count all its minimizers (with repetitions) and calculate the fraction
   * that is assigned (in the index) to that genome's taxon, rather than some ancestor.
   * This is a measure of how well we can identify each distinct genome.
   * @param indexBuckets index with LCAs
   * @param genomes genome sequences to check (intended to be a subset of the sequences that were used
   *               to build the index)
   */
  private def showTaxonCoverageStats(indexBuckets: DataFrame, genomes: GenomeLibrary): Unit = {
    val inputSequences = joinSequencesAndLabels(genomes, addRC = false)
    val mins = findMinimizers(inputSequences)

    //1. Count how many times per input taxon each minimizer occurs
    val agg = mins.groupBy(idColumns :+ $"taxon": _*).agg(count("*").as("countAll"))

    //2. Join with buckets, find the fraction that is assigned to the same (leaf) taxon
    val joint = agg.join(indexBuckets.withColumnRenamed("taxon", "idxTaxon"),
        idColumnNames, "left").
      withColumn("countLeaf", when($"idxTaxon" === $"taxon", $"countAll").
        otherwise(lit(0L))).
      groupBy("taxon").
      agg((sum("countLeaf") / sum("countAll")).as("fracLeaf"),
        sum("countAll").as("total"))

    joint.select("fracLeaf", "total").summary().show()
  }

  def report(indexBuckets: DataFrame, checkLabelFile: Option[String], output: String): Unit = {

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
  def kmersDepths(buckets: DataFrame): DataFrame = {
    val bcTax = this.bcTaxonomy
    val depth = udf((x: Taxon) => bcTax.value.depth(x))
    buckets.select(depth($"taxon").as("depth") +: idColumns :_*).
      sort(desc("depth"))
  }

  def taxonDepths(buckets: DataFrame): Dataset[(Taxon, Int)] = {
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
    val sp = SparkTool.newSession(spark, params.buckets) //Ensure that new datasets have the same number of partitions
    new KeyValueIndex(params, taxonomy)(sp)
  }
}

/** A single hit group for a taxon and some number of k-mers
 * @param minimizer minimizer
 * @param ordinal the position of this hit in the sequence of hits in the query sequence
 *              (not same as position in sequence)
 * @param taxon the classified LCA taxon
 * @param count the number of k-mer hits
 * */
final case class TaxonHit(minimizer: Array[Long], ordinal: Int, taxon: Taxon, count: Int) {
  def summary: TaxonCounts =
    TaxonCounts(ordinal, Array(taxon), Array(count))

  def trueTaxon: Option[Taxon] = taxon match {
    case AMBIGUOUS_SPAN | MATE_PAIR_BORDER => None
    case _ => Some(taxon)
  }
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


