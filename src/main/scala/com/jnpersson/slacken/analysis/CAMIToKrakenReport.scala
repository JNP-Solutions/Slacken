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

package com.jnpersson.slacken.analysis

import com.jnpersson.slacken.Taxonomy.Rank
import com.jnpersson.slacken.{KrakenReport, Taxon, Taxonomy}
import org.apache.spark.sql
import org.apache.spark.sql.{Dataset, SparkSession}

import java.io.{FileWriter, PrintWriter}

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
   * Argument 0: taxonomy location (directory) (HDFS)
   * Argument 1: highest taxonomic level to keep (e.g. species). Higher levels will be discarded and
   *   the total (as fractions) rescaled accordingly. Read counts will not be rescaled.
   *   Use Root for no filtering.
   * Argument 2: location of CAMISIM mapping (HDFS)
   * Argument 3: output location (prefix name) (local path)
   *
   */
  def main(args: Array[String]): Unit = {
    val taxonomyDir = args(0)
    val minLevel = if (args.length > 1) Taxonomy.rank(args(1).toLowerCase()) else None
    val tax = Taxonomy.load(taxonomyDir)
    val location = args(3)

    val c2r = new CAMIToKrakenReport(args(2), tax, minLevel)
    c2r.writeFilteredReport(s"$location.kreport.txt")
    c2r.writeFilteredIDs(s"$location.ids_filtered.txt")
  }
}

class CAMIToKrakenReport(mappingLocation: String, tax: Taxonomy, minRank: Option[Rank])(implicit spark: SparkSession) {
  import spark.sqlContext.implicits._
  val bcTax = spark.sparkContext.broadcast(tax)

  /** Mappings for only reads at or below the taxonomic cutoff level */
  private val filteredMapping = {
    val bcTax = this.bcTax
    val minDepth = minRank.map(_.depth)

    spark.read.option("sep", "\t").option("header", "true").csv(mappingLocation).
      filter(read => {
        val tax = bcTax.value.primary(read.getString(2).toInt)
        minDepth.forall(d => bcTax.value.depth(tax) >= d)
      })
  }

  def filteredIDs: Dataset[String] =
    filteredMapping.map(_.getString(0))

  /** Write filtered read IDs to a local file */
  def writeFilteredIDs(location: String): Unit = {
    val writer = new PrintWriter(new FileWriter(location))
    try {
      val ids = filteredIDs.collect()
      for { line <- ids } {
        writer.println(line)
      }
    } finally {
      writer.close()
    }
  }

  /** Write a filtered kraken report to a local file */
  def writeFilteredReport(location: String): Unit = {
    //count by taxon and divide by 2 to adjust from single reads to paired end counts
    val counts = filteredMapping.map(line => line.getString(2).toInt).toDF("taxon").
      groupBy("taxon").agg(
      sql.functions.floor(
        sql.functions.count("*") / 2
      )
    ).as[(Taxon, Long)].collect

    val pairs = counts
    val report = new KrakenReport(tax, pairs)
    val writer = new PrintWriter(new FileWriter(location))
    try {
      report.print(writer)
    } finally {
      writer.close()
    }
  }
}