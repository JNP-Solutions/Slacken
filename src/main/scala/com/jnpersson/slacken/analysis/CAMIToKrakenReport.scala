/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */

package com.jnpersson.slacken.analysis

import com.jnpersson.slacken.Taxonomy.Rank
import com.jnpersson.slacken.{KrakenReport, Taxon, TaxonomicIndex, Taxonomy}
import org.apache.spark.sql.SparkSession

import java.io.{FileWriter, PrintWriter}
import scala.collection.mutable

/** Tool to convert a CAMI2 read mapping into a Kraken-style report
 * for easy comparison. Filters by taxonomic level. Writes report
 * and also a text file with filtered IDs.
 *
 * Example input mapping lines:
 *
 * #anonymous_read_id      genome_id       tax_id  read_id
 * S0R0/1  SP160_S41       1313    NODE_16_length_48984_cov_20.837321-595/1
 * S0R0/2  SP160_S41       1313    NODE_16_length_48984_cov_20.837321-595/2
 */
object CAMIToKrakenReport {

  implicit val spark = SparkSession.builder().appName("CAMIToKraken").
    enableHiveSupport().
    getOrCreate()

  /** Reads stdin. Writes the Kraken report to stdout.
   * Argument 0: taxonomy location (directory)
   * Argument 1: highest taxonomic level to keep (e.g. species). Higher levels will be discarded and
   *   the total (as fractions) rescaled accordingly. Read counts will not be rescaled.
   *   Use Root for no filtering.
   * Argument 2: location of CAMISIM mapping
   * Argument 3: output location (prefix name)
   *
   */
  def main(args: Array[String]): Unit = {
    val taxonomyDir = args(0)
    val minLevel = if (args.length > 1) Taxonomy.rank(args(1).toLowerCase()) else None
    val tax = TaxonomicIndex.getTaxonomy(taxonomyDir)
    val location = args(2)

    val c2r = new CAMIToKrakenReport(args(2), tax, minLevel)
    c2r.writeFilteredReport(s"$location.kreport.txt")
    c2r.writeFilteredIDs(s"$location.ids_filtered.txt")
  }
}

class CAMIToKrakenReport(mappingLocation: String, tax: Taxonomy, minRank: Option[Rank])(implicit spark: SparkSession) {
  import spark.sqlContext.implicits._
  val bcTax = spark.sparkContext.broadcast(tax)

  val minDepth = minRank.map(_.depth)

  /** Mappings for only reads at or below the taxonomic cutoff level */
  val filteredMapping = spark.read.option("sep", "\t").option("header", "true").csv(mappingLocation).
    filter(read => {
      val tax = bcTax.value.primary(read.getString(2).toInt)
    minDepth.forall(d => bcTax.value.depth(tax) >= d)
  })

  def filteredIDs =
    filteredMapping.map(_.getString(0))

  /** Write filtered read IDs to a local file */
  def writeFilteredIDs(location: String): Unit = {
    val writer = new PrintWriter(new FileWriter(location))
    try {
      val ids = filteredIDs.collect()
      writer.println(ids.mkString("\n"))
    } finally {
      writer.close()
    }
  }

  /** Write a filtered kraken report to a local file */
  def writeFilteredReport(location: String): Unit = {
    val counts = mutable.Map[Taxon, Long]().withDefaultValue(0)
    for {line <- filteredMapping} {
      val taxon = tax.primary(line.getString(2).toInt)
      if (!tax.isDefined(taxon)) {
        Console.err.println(s"Warning: undefined taxon $taxon, omitting from output")
      }
      counts(taxon) += 1
    }

    //adjust from single reads to mate-pair counts
    for {(k, v) <- counts} {
      counts(k) = v / 2
    }

    val pairs = counts.toArray
    val report = new KrakenReport(tax, pairs)
    val writer = new PrintWriter(new FileWriter(location))
    try {
      report.print(writer)
    } finally {
      writer.close()
    }
  }
}