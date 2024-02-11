/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */

package com.jnpersson.slacken

import com.jnpersson.slacken.Taxonomy.NONE
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.{FileWriter, PrintWriter}

object GenomeCounts {
  /**
   * Build a lookup array that counts how many distinct genomes are available in total for each
   * taxon in the taxonomy (recursively including descendants)
   * @param tax the taxonomy
   * @param seqLabels sequence labels (each one considered to correspond to one genome)
   */
  def build(tax: Taxonomy, seqLabels: DataFrame)(implicit spark: SparkSession): Array[Long] = {
    import spark.sqlContext.implicits._
    val taxa = seqLabels.select("taxon").distinct().as[Taxon].collect()

    val r = new Array[Long](tax.size)
    for { taxon <- taxa } {
      //each taxon has an initial count of 1
      r(taxon) = 1

      //propagate counts up the tree
      var at = tax.parents(taxon)
      while (at != NONE) {
        r(at) = r(at) + 1
        at = tax.parents(at)
      }
    }

    r
  }

  //Print the genome counts per taxon using a quasi-report
  private def debugPrint(tax: Taxonomy, counts: Array[(Taxon, Long)]) =
    new KrakenReport(tax, counts).
      print(new PrintWriter(new FileWriter("genomeCounts_report.txt")))

}
