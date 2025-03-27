/*
 *
 *  * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 *  *
 *  * Slacken is free software: you can redistribute it and/or modify
 *  * it under the terms of the GNU General Public License as published by
 *  * the Free Software Foundation, either version 3 of the License, or
 *  * (at your option) any later version.
 *  *
 *  * Slacken is distributed in the hope that it will be useful,
 *  * but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  * GNU General Public License for more details.
 *  *
 *  * You should have received a copy of the GNU General Public License
 *  * along with Slacken.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package com.jnpersson.slacken

import com.jnpersson.kmers._
import com.jnpersson.kmers.minimizer._
import com.jnpersson.kmers.Helpers.randomTableName
import com.jnpersson.kmers.Helpers.formatPerc
import com.jnpersson.kmers.util.NTBitArray
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

import java.util
import scala.collection.mutable

/** Metagenomic index compatible with the Kraken 2 algorithm.
 * This index stores k-mers and LCA taxa as key-value pairs.
 *
 * @param records LCA records. The number of columns depends on the minimizer width:
 *                one long value for each 32 bp of the minimizers, and an integer for the taxon.
 * @param params Parameters for k-mers, index bucketing and persistence
 * @param taxonomy The taxonomy
 */
final class KeyValueIndex(val records: DataFrame, val params: IndexParams, val taxonomy: Taxonomy)
                         (implicit val spark: SparkSession) extends KmerKeyedIndex {
  import spark.sqlContext.implicits._

  def bcSplit: Broadcast[AnyMinSplitter] = params.bcSplit
  def split: AnyMinSplitter = bcSplit.value
  lazy val bcTaxonomy: Broadcast[Taxonomy] =
    spark.sparkContext.broadcast(taxonomy)

  val idLongs = NTBitArray.longsForSize(params.m)
  private val recordColumnNames: Seq[String] = idColumnNames :+ "taxon"


  /** Sanity check input data.
   * Validates that all input sequences have minimizers.
   */
  def checkInput(inputs: Inputs): Unit = {
    val fragments = inputs.getInputFragments().map(x => (x.header, x.nucleotides))

    /* Check if there are input sequences with no valid minimizers.
    * If so, report them.  */
    val spl = bcSplit
    //count minimizers per input sequence ID
    val noMinInput = fragments.map(r => {
      val splitter = spl.value
      (splitter.superkmerPositions(r._2).size.toLong, r._1)
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

  /** Find (minimizer, taxon) pairs in the given pairs of (taxon, NT sequence).
   * @param seqTaxa pairs of (taxon, NT sequence)
   * @return pairs of (minimizer, taxon).
   */
  def findMinimizers(seqTaxa: Dataset[(Taxon, NTSeq)]): DataFrame = {
    val bcSplit = this.bcSplit

    numIdColumns match {
      case 1 =>
        seqTaxa.flatMap(r =>
          bcSplit.value.superkmerPositions(r._2).map { min =>
            (min.rank(0), r._1)
          }
        ).toDF(recordColumnNames: _*)
      case 2 =>
        seqTaxa.flatMap(r =>
          bcSplit.value.superkmerPositions(r._2).map { min =>
            (min.rank(0), min.rank(1), r._1)
          }
        ).toDF(recordColumnNames: _*)
      case 3 =>
        seqTaxa.flatMap(r =>
          bcSplit.value.superkmerPositions(r._2).map { min =>
            (min.rank(0), min.rank(1), min.rank(2), r._1)
          }
        ).toDF(recordColumnNames: _*)
      case 4 =>
        seqTaxa.flatMap(r =>
          bcSplit.value.superkmerPositions(r._2).map { min =>
            (min.rank(0), min.rank(1), min.rank(2), min.rank(3), r._1)
          }
        ).toDF(recordColumnNames: _*)
      case _ =>
        //In case of minimizers wider than 128 bp (4 longs), expand this section
        ???
    }
  }

  /** Given input genomes and their taxon IDs, build index records of minimizers and LCA taxa.
   * @param taxaSequences pairs of (taxon, genome DNA)
   * @return index records
   */
  def makeRecords(taxaSequences: Dataset[(Taxon, NTSeq)]): DataFrame = {
    val minimizersTaxa = findMinimizers(taxaSequences)

    val bcTax = this.bcTaxonomy
    val udafLca = udaf(TaxonLCA(bcTax))
    minimizersTaxa.
      groupBy(idColumns: _*).
      agg(udafLca($"taxon").as("taxon"))
  }

  /**
   * Construct records for a new index from genomes.
   *
   * @param library     Input data
   * @param taxonFilter If given, limit input sequences to only taxa in this set (and their descendants)
   * @return index records
   */
  def makeRecords(library: GenomeLibrary, taxonFilter: Option[mutable.BitSet] = None): DataFrame = {
    val input = taxonFilter match {
      case Some(tf) =>
        val titlesTaxa = library.getTaxonLabels.
          filter(l => tf.contains(l._2)).as[(SeqTitle, Taxon)].toDF("header", "taxon").cache() //TODO unpersist

        println("Construct dynamic records from:")
        titlesTaxa.select(countDistinct($"header"), countDistinct($"taxon")).show()

        library.inputs.getInputFragments().join(titlesTaxa, List("header")).
          select("taxon", "nucleotides").as[(Taxon, NTSeq)].
          repartition(params.buckets, List(): _*)
      case None =>
        library.joinSequencesAndLabels()
    }

    val bcTax = this.bcTaxonomy
    val isValid = udf((t: Taxon) => bcTax.value.isDefined(t))
    val filtered = input.filter(isValid($"taxon"))
    makeRecords(filtered)
  }

  /** Write records to the given location */
  def writeRecords(location: String): Unit = {
    params.write(location, s"Properties for Slacken KeyValueIndex $location")
    println(s"Saving index into ${params.buckets} partitions at $location")

    //A unique table name is needed to make saveAsTable happy, but we will not need it again
    //when we read the index back (by HDFS path).
    val tableName = randomTableName

    //Use saveAsTable instead of ordinary parquet save to preserve buckets/partitioning.
    records.
      write.mode(SaveMode.Overwrite).
      option("path", location).
      bucketBy(params.buckets, idColumnNames.head, idColumnNames.tail: _*).
      saveAsTable(tableName)
  }

  /** Produce a copy of this index with the same parameters but different records */
  def withRecords(recs: Dataset[Row]): KeyValueIndex =
    new KeyValueIndex(recs, params, taxonomy)

  /** Load index records from the params location (default location for this index) */
  def loadRecords(): DataFrame =
    loadRecords(params.location)

  /** Load records from the given location */
  def loadRecords(location: String): DataFrame = {
    //Does not delete the table itself, only removes it from the hive catalog
    //This is to ensure that we get the one in the expected location
    spark.sql("DROP TABLE IF EXISTS kv_taxidx")
    spark.sql(s"""|CREATE TABLE kv_taxidx($idColumnsTypes, taxon int)
                  |USING PARQUET CLUSTERED BY ($idColumnsString) INTO ${params.buckets} BUCKETS
                  |LOCATION '$location'
                  |""".stripMargin)
    spark.sql(s"SELECT $idColumnsString, taxon FROM kv_taxidx")
  }

  /** Split fragments into ordinal spans, which are either super-mers with a minimizer, or invalid spans.
   * Whitespace (e.g. newlines) must have been removed prior to using this function. */
  def getSpans(subjects: Dataset[InputFragment], withTitle: Boolean): Dataset[OrdinalSpan] = {
    val bcSplit = this.bcSplit
    val k = params.k
    val ni = numIdColumns

    //Split input sequences by minimizer, optionally preserving ordinal of the super-mer and optionally sequence ID
    subjects.mapPartitions(fs => {
      val supermers = new Supermers(bcSplit.value, ni)

      fs.flatMap(s => {
        val sms = supermers.splitFragment(s)

        new Iterator[OrdinalSpan] {
          private var first = true
          private var lastMinimizer = Array[Long]()

          override def hasNext: Boolean =
            sms.hasNext

          override def next(): OrdinalSpan = {
            val x = sms.next()
            val flag = x.flag

            //Track whether two consecutive valid minimizers are distinct. This allows us to count the number of
            //hit groups later.
            val distinct =
              flag != AMBIGUOUS_FLAG && flag != MATE_PAIR_BORDER_FLAG &&
                (first || !util.Arrays.equals(x.segment.rank, lastMinimizer))
            val title = if (withTitle) x.seqTitle else null
            if (flag != AMBIGUOUS_FLAG && flag != MATE_PAIR_BORDER_FLAG) {
              lastMinimizer = x.segment.rank
            }

            first = false

            OrdinalSpan(x.segment.rank, distinct, x.segment.nucleotides.size - (k - 1),
              x.flag, x.ordinal, title)
          }
        }
      })
    })
  }

  /** Build a TaxonHit from an OrdinalSpan in spark SQL */
  private def spanToHit(withOrdinal: Boolean): List[Column] =
    List($"distinct",
      if (withOrdinal) $"ordinal" else lit(0).as("ordinal"),
      when($"flag" === lit(AMBIGUOUS_FLAG), lit(AMBIGUOUS_SPAN)).
        when($"flag" === lit(MATE_PAIR_BORDER_FLAG), lit(MATE_PAIR_BORDER)).
        when(isnotnull($"taxon"), $"taxon").
        otherwise(lit(Taxonomy.NONE)).
        as("taxon"),
    $"kmers".as("count")
  )

  /** Find TaxonHits from InputFragments and set their taxa, without grouping them by seqTitle. */
  def findHits(subjects: Dataset[InputFragment]): Dataset[TaxonHit] = {
    val spans = getSpans(subjects, withTitle = false)

    val taggedSpans = spans.select(
      Array($"distinct", $"kmers", $"flag", $"ordinal", $"seqTitle") ++
        idColumnsFromMinimizer
        :_*)

    taggedSpans.join(records, idColumnNames, "left").
      select(
        spanToHit(false) : _*
      ).as[TaxonHit]
  }

  /** Find TaxonHits from InputFragments and set their taxa, without grouping them by seqTitle.
   * This version preserves each hit's minimizer.
   */
  def findHitsWithMinimizers(subjects: Dataset[InputFragment]): Dataset[(TaxonHit, Array[Long])] = {
    val spans = getSpans(subjects, withTitle = false)

    val taggedSpans = spans.select(
      Array($"minimizer", $"distinct", $"kmers", $"flag", $"ordinal", $"seqTitle") ++
        idColumnsFromMinimizer
        :_*)

    taggedSpans.join(records, idColumnNames, "left").
      select(
        struct(spanToHit(false): _*).as("_1"), $"minimizer".as("_2")
      ).as[(TaxonHit, Array[Long])]
  }

  /** Find the number of distinct minimizers for each of the given taxa */
  def distinctMinimizersPerTaxon(taxa: Seq[Taxon]): Array[(Taxon, Long)] = {
    val precalcLocation = s"${params.location}_distinctMinimizers"
    if (!HDFSUtil.fileExists(precalcLocation)) {
      /** Precompute these values and store them for reuse later */
      println(s"$precalcLocation didn't exist, creating now.")
      records.
        groupBy("taxon").agg(functions.count_distinct(idColumns.head, idColumns.tail :_*).as("count")).
        coalesce(200).
        write.mode(SaveMode.Overwrite).option("sep", "\t").csv(precalcLocation)
    }
    spark.read.option("sep", "\t").csv(precalcLocation).
      select($"_c0".cast("int").as("taxon"), $"_c1".cast("long").as("count")).
      join(taxa.toDF("taxon"), List("taxon")).as[(Taxon, Long)].
      collect()
  }

  /** Classify subject sequences (as a dataset) */
  def collectHitsBySequence(subjects: Dataset[InputFragment], withOrdinal: Boolean): Dataset[(SeqTitle, Array[TaxonHit])] =
    spansToGroupedHits(getSpans(subjects, withTitle = true), withOrdinal)

  /** Group super-mers (minimizer spans) by sequence title and convert them to taxon hits.
   */
  def spansToGroupedHits(subjects: Dataset[OrdinalSpan], withOrdinal: Boolean): Dataset[(SeqTitle, Array[TaxonHit])] = {
    //The 'subject' struct constructs an OrdinalSpan
    val taggedSpans = subjects.select(
      Seq($"distinct", $"kmers", $"flag", $"ordinal", $"seqTitle") ++
        idColumnsFromMinimizer
        :_*)

    val taxonHits = taggedSpans.join(records, idColumnNames, "left").
      select(
        $"seqTitle",
        struct(spanToHit(withOrdinal) : _*).as("hit")
      )

    //Group all hits by sequence title again so that we can reassemble (the hits from) each sequence according
    // to the original order.
    taxonHits.groupBy("seqTitle").agg(collect_list("hit").as("hits")).
      as[(SeqTitle, Array[TaxonHit])]
  }

  /** Print basic statistics for this index.
   * Optionally, input sequences and a label file can be specified, and they will then be checked against
   * the database.
   */
  def showIndexStats(genomes: Option[GenomeLibrary]): Unit = {
    val allTaxa = records.groupBy("taxon").agg(count("taxon")).as[(Taxon, Long)].collect()
    val leafTaxa = allTaxa.filter(x => taxonomy.isLeafNode(x._1))
    val treeSize = taxonomy.countDistinctTaxaWithAncestors(allTaxa.map(_._1))
    println(s"Tree size: $treeSize taxa, stored taxa: ${allTaxa.length}, of which ${leafTaxa.length} " +
      s"leaf taxa (${formatPerc(leafTaxa.length.toDouble/ allTaxa.length)})")

    val recTotal = allTaxa.map(_._2).sum
    val leafTotal = leafTaxa.map(_._2).sum

    val m = params.m
    println(s"Total $m-minimizers: $recTotal, of which leaf records: $leafTotal (${formatPerc(leafTotal.toDouble/recTotal)})")
//    for { library <- genomes} showTaxonCoverageStats(records, library)
    for { library <- genomes} {
      val irs = new IndexStatistics(this)
      irs.showTaxonFullCoverageStats(library)
    }
  }

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
             output: String, genomelib: Option[GenomeLibrary] = None): Unit = {

    //Report the contents of the index, count minimizers of taxa with distinct minimizer counts
    val allTaxa = records.groupBy("taxon").agg(count("*")).as[(Taxon, Long)].collect()
    genomelib match {
      case Some(gl) =>
        val irs = new IndexStatistics(this)
        val report = irs.totalKmerCountReport(gl)
        HDFSUtil.usingWriter(output + "_min_report.txt", wr => report.print(wr))

      case None =>
        HDFSUtil.usingWriter(output + "_min_report.txt", wr =>
          new KrakenReport(taxonomy, allTaxa).print(wr)
        )
    }

    //count of 1 per genome
    HDFSUtil.usingWriter(output + "_genome_report.txt", wr =>
      new KrakenReport(taxonomy, allTaxa.map(t => (t._1, 1L))).print(wr)
    )

    //Report missing genomes that were present in the input label file but are not in the index
    for { labels <- checkLabelFile } {
      val presentTaxa = allTaxa.iterator.map(_._1)
      val inputTaxa = GenomeLibrary.getTaxonLabels(labels).select("_2").distinct().as[Taxon].collect()
      //count of 1 per genome
      val missingLeaf = (mutable.BitSet.empty ++ inputTaxa -- presentTaxa).toArray.map(t => (t, 1L))
      HDFSUtil.usingWriter(output + "_missing_report.txt", wr =>
        new KrakenReport(taxonomy, missingLeaf).print(wr)
      )
    }
  }

  /** K-mers or minimizers in this index (keys) sorted by taxon depth from deep to shallow */
  def kmersDepths: DataFrame = {
    val bcTax = this.bcTaxonomy
    val depth = udf((x: Taxon) => bcTax.value.depth(x))
    records.select(depth($"taxon").as("depth") +: idColumns :_*).
      sort(desc("depth"))
  }

  /** Taxa in this index (values) together with their depths */
  def taxonDepths: Dataset[(Taxon, Int)] = {
    val bcTax = this.bcTaxonomy
    val depth = udf((x: Taxon) => bcTax.value.depth(x))
    records.select($"taxon").distinct.select($"taxon", depth($"taxon").as("depth")).
      sort(desc("depth")).as[(Taxon, Int)]
  }

  import GenomeLibrary.numericalRankToStrUdf

  def kmerDepthHistogram(): DataFrame = {
    kmersDepths.select("depth").groupBy("depth").count().
      sort("depth").
      withColumn("rank", numericalRankToStrUdf($"depth")).
      select("depth", "rank", "count")
  }

  def taxonDepthHistogram(): DataFrame = {
    taxonDepths.select("depth").groupBy("depth").count().
      sort("depth").
      withColumn("rank", numericalRankToStrUdf($"depth")).
      select("depth", "rank", "count")
  }

  /**
   * Write the histogram of this data to HDFS.
   * @param output Directory to write to (prefix name)
   */
  def writeDepthHistogram(output: String): Unit =
    kmerDepthHistogram().
      write.mode(SaveMode.Overwrite).option("sep", "\t").csv(s"${output}_taxonDepths")


  /** Map records into a new set of records where a larger number of spaces have been applied
   * in the spaced seed mask. Loses information, as the new index is expected to be smaller (this is a
   * dimensionality reduction).
   * @param spaces new number of spaces
   * @return A new KeyValueIndex with identical parameters to this one (except for spaces) and the new set of records
   */
  def respace(spaces: Int): KeyValueIndex = {

    val newPriorities = params.splitter.priorities match {
      case SpacedSeed(s, inner) =>
        if (spaces <= s) {
          throw new Exception(s"Respacing to a smaller or identical number of spaces is not meaningful. (was $s, requested $spaces)")
        }
        SpacedSeed(spaces, inner)
      case p => SpacedSeed(spaces, p)
    }
    val newSplitter: AnyMinSplitter = MinSplitter(newPriorities, params.k)
    val bcSpl = spark.sparkContext.broadcast(newSplitter)
    val bcSs = spark.sparkContext.broadcast(newPriorities)
    val newParams = params.copy(bcSpl, params.buckets, "")

    val applySpaceUdf = udf((data: Array[Long]) => {
      val min = NTBitArray(data, bcSs.value.width)
      bcSs.value.maskSpacesOnly(min).data
    })

    val bcTax = this.bcTaxonomy
    val udafLca = udaf(TaxonLCA(bcTax))

    val nrecords = records.select(applySpaceUdf(minimizerColumnFromIdColumns), $"taxon").
      select($"taxon" +: idColumnsFromMinimizer :_*).
      groupBy(idColumns: _*).
      agg(udafLca($"taxon").as("taxon"))

    new KeyValueIndex(nrecords, newParams, taxonomy)
  }


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
}

object KeyValueIndex {
  /** Load index from the given location */
  def load(location: String, taxonomy: Taxonomy)(implicit spark: SparkSession): KeyValueIndex = {

    val params = IndexParams.read(location)
    val sp = SparkTool.newSession(spark, params.buckets) //Ensure that new datasets have the same number of partitions
    val i = new KeyValueIndex(spark.sqlContext.emptyDataFrame, params, taxonomy)(sp)
    i.withRecords(i.loadRecords())
  }
}

/** A single hit group for a taxon and some number of k-mers
 * @param distinct whether the minimizer was distinct from the previous valid minimizer
 * @param ordinal the position of this hit in the sequence of hits in the query sequence
 *              (not same as position in sequence)
 * @param taxon the classified LCA taxon
 * @param count the number of k-mer hits
 * */
final case class TaxonHit(distinct: Boolean, ordinal: Int, taxon: Taxon, count: Int) {
  def trueTaxon: Option[Taxon] = taxon match {
    case AMBIGUOUS_SPAN | MATE_PAIR_BORDER => None
    case _ => Some(taxon)
  }
}
