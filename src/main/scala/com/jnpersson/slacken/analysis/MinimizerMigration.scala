/*
 * This file is part of Slacken. Copyright (c) 2019-2025 Johan Nyström-Persson.
 *
 * Slacken is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 *  Slacken is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 * along with Slacken.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.jnpersson.slacken.analysis

import com.jnpersson.kmers.HDFSUtil
import com.jnpersson.slacken.Taxonomy.ROOT
import com.jnpersson.slacken.{KeyValueIndex, KrakenReport, Taxon}
import org.apache.spark.sql.functions.{count, lit}
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
   */
  def taxaDistances(): Dataset[(Taxon, Taxon, Int)] = {

    val b1 = index.loadRecords()
    val b2 = reference.loadRecords()
    val bcTax = reference.bcTaxonomy //Assumed to be the larger if the two taxonomies are different
    val cols = index.idColumnNames

    //null-safe equals, since both sides could be null for smaller disk formats
    val condition = cols.map(c => b1(c) <=> b2(c)).fold(lit(true))(_ && _)
    val joint = b1.joinWith(b2, condition)
    joint.map(row => {
      val t1 = row._1.getAs[Int]("taxon")
      val t2 = row._2.getAs[Int]("taxon")
      val l1 = bcTax.value.depth(t1) //larger numbers are deeper (more specific)
      val l2 = bcTax.value.depth(t2)
      if (l1 == -1) {
        println("Warning: found depth -1 in subject index")
        (t1, t2, -100) //assign special value -100 so that we can identify these
      } else if (l2 == -1) {
        println("Warning: found depth -1 in reference index")
        (t1, t2, -200) //assign special value -200 so that we can identify these
      } else {
        //number of steps that this record moved up the taxonomic tree in the reference.
        //e.g. 3 for species to order.
        val steps = l1 - l2
        (t1, t2, steps)
      }
    }).as[(Taxon, Taxon, Int)]
  }

  def run(output: String): Unit = {
    val dist = taxaDistances().toDF("t1", "t2", "steps").cache()
    dist.select("steps").groupBy("steps").agg(count("steps")).
      sort("steps").
      show()

    val cellularOrganismsTaxon = 131567
    //The level of ROOT and cellular organisms can be seen as broadly equivalent to "almost nothing known".
    //Moving into or out of the set comprised by these two taxa is significant.
    val toRoot = dist.where(($"t2" === ROOT || $"t2" === cellularOrganismsTaxon)
      && $"t1" =!= ROOT && $"t1" =!= cellularOrganismsTaxon).select("t1").
      groupBy("t1").agg(count("t1")).as[(Int, Long)].collect()

    HDFSUtil.usingWriter(output + "_taxaToRoot_report.txt", wr =>
      new KrakenReport(index.bcTaxonomy.value, toRoot).print(wr)
    )
  }
}
