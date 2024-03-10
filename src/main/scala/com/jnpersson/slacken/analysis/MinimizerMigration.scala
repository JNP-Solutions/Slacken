/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */

package com.jnpersson.slacken.analysis

import com.jnpersson.discount.spark.HDFSUtil
import com.jnpersson.slacken.Taxonomy.ROOT
import com.jnpersson.slacken.{KeyValueIndex, KrakenReport, Taxon}
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.{Dataset, SparkSession}

/** Compare one taxonomic index against another in terms of how minimizers have changed.
 * "reference" is assumed to be a superset of index, and thus expected to have minimizer at higher or the same
 * levels in the tree.
 *
 * The two indexes must have the same disk format and must use compatible taxonomies.
 * They must use the same minimizer scheme for the comparison to be meaningful.
 */
class MinimizerMigration(index: KeyValueIndex, reference: KeyValueIndex)(implicit spark: SparkSession) {
  import spark.sqlContext.implicits._

  /** Compute triples of: (taxon in index, taxon in reference, number of steps that the taxon moved up),
   * final depth */
  def taxaDistances(): Dataset[(Taxon, Taxon, Int, Int)] = {

    val b1 = index.loadBuckets()
    val b2 = reference.loadBuckets()
    val bcTax = reference.bcTaxonomy //Assumed to be the larger if the two taxonomies are different
    val cols = index.idColumnNames

    //null-safe equals, since both sides could be null for smaller disk formats
    val condition = cols.map(c => b1(c) <=> b2(c)).reduce(_ && _)
    val joint = b1.joinWith(b2, condition)
    joint.map(row => {
      val t1 = row._1.getAs[Int]("taxon")
      val t2 = row._2.getAs[Int]("taxon")
      val l1 = bcTax.value.depth(t1) //larger numbers are deeper (more specific)
      val l2 = bcTax.value.depth(t2)
      if (l1 == -1) {
        println("Warning: found depth -1 in subject index")
        (t1, t2, -100, l2) //assign special value -100 so that we can identify these
      } else if (l2 == -1) {
        println("Warning: found depth -1 in reference index")
        (t1, t2, -200, l2) //assign special value -200 so that we can identify these
      } else {
        //number of steps that this record moved up the taxonomic tree in the reference.
        //e.g. 3 for species to order.
        val steps = l1 - l2
        (t1, t2, steps, l2)
      }
    }).as[(Taxon, Taxon, Int, Int)]
  }

  def run(output: String): Unit = {
    val dist = taxaDistances().toDF("t1", "t2", "steps", "l2").cache()
    dist.select("steps").groupBy("steps").agg(count("steps")).
      sort("steps").
      show()

    val toRoot = dist.where($"t2" === ROOT && $"t1" =!= ROOT).select("t1").
      groupBy("t1").agg(count("t1")).as[(Int, Long)].collect()

    HDFSUtil.usingWriter(output + "_taxaToRoot_report.txt", wr =>
      new KrakenReport(index.bcTaxonomy.value, toRoot).print(wr)
    )
  }
}
