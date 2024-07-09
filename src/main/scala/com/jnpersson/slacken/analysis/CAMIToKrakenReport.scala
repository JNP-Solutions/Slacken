/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */

package com.jnpersson.slacken.analysis

import com.jnpersson.slacken.{KrakenReport, Taxon, TaxonomicIndex, Taxonomy, TreeAggregator}
import org.apache.spark.sql.SparkSession

import java.io.PrintWriter
import scala.collection.mutable
import scala.io.Source

/** Tool to convert a CAMI2 read mapping into a Kraken-style report
 * for easy comparison.
 * Needs to be run inside Spark for now (in order to read the Taxonomy)
 *
 * Example report lines:
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
   * Argument 1: highest taxonomic level to keep (e.g. species) (if any). Higher levels will be discarded and
   *   the total rescaled accordingly.
   */
  def main(args: Array[String]): Unit = {
    val input = Source.stdin.getLines.drop(1)
    val taxonomyDir = args(0)
    val minLevel = if (args.length > 1) Taxonomy.rank(args(1).toLowerCase()) else None
    val tax = TaxonomicIndex.getTaxonomy(taxonomyDir)
    val counts = mutable.Map[Taxon, Long]().withDefaultValue(0)
    for { line <- input } {
      val spl = line.split("\t")
      val taxon = spl(2).toInt
      if (!tax.isDefined(taxon)) {
        Console.err.println(s"Warning: undefined taxon $taxon, omitting from output")
      } else if (minLevel.forall(tax.depth(taxon) >= _.depth)) {
        counts(taxon) += 1
      }
    }

    //adjust from single reads to mate-pair counts
    for { (k, v) <- counts } {
      counts(k) = v/2
    }

    val pairs = counts.toArray
    val report = new KrakenReport(tax, pairs)
    val writer = new PrintWriter(scala.Console.out)
    try {
      report.print(writer)
    } finally {
      writer.close()
    }
  }
}
