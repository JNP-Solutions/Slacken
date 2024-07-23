/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */

package com.jnpersson.slacken.analysis

import com.jnpersson.slacken.{Taxon, TaxonomicIndex, Taxonomy}
import org.apache.spark.sql.{Dataset, SparkSession}

object FilterReadIDs {
  implicit val spark = SparkSession.builder().appName("CAMIToKraken").
    enableHiveSupport().
    getOrCreate()

  /**
   * Filter a CAMISIM reference mapping, outputting only read IDs.
   * Writes the filtered ID set to stdout. The resulting ID set is intended for use with e.g. seqkit grep -f.
   *
   * Argument 0: mapping location
   * Argument 1: taxonomy location (directory)
   * Argument 2: highest taxonomic level to keep (e.g. species). Higher levels will be discarded and
   *   the total rescaled accordingly.
   */
  def main(args: Array[String]): Unit = {
    import spark.sqlContext.implicits._
    val taxonomyDir = args(1)
    val minLevel = Taxonomy.rank(args(2).toLowerCase()).get
    val tax = TaxonomicIndex.getTaxonomy(taxonomyDir)

    val bcTax = spark.sparkContext.broadcast(tax)

    val filtered = spark.read.option("sep", "\t").option("header", "true").csv(args(0)).flatMap(read => {
      val id = read.getString(0)
      val tax = bcTax.value.primary(read.getString(2).toInt)
      if (bcTax.value.depth(tax) >= minLevel.depth) Some(id) else None
    })

    val reads = filtered.collect()
    for { r <- reads } println(r)
  }

}
