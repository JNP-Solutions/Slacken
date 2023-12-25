/*
 * This file is part of Discount. Copyright (c) 2019-2023 Johan Nyström-Persson.
 *
 * Discount is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Discount is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Discount.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.jnpersson.slacken

import com.jnpersson.discount.SeqTitle
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SparkSession}

class MappingComparison(tax: Broadcast[Taxonomy], reference: String,
                        refIdCol: Int, refTaxonCol: Int, skipHeader: Boolean)(implicit spark: SparkSession) {
  import MappingComparison._
  val referenceData = readCustomFormat(reference, refIdCol, refTaxonCol, skipHeader).
    toDF("id", "refTaxon").
    cache()

  def compare(dataFile: String): Unit = {
    import spark.sqlContext.implicits._

    val cmpData = readKrakenFormat(dataFile).toDF("id", "testTaxon")
    val joint = referenceData.join(cmpData, referenceData("id") === cmpData("id"), "outer").
      select("refTaxon", "testTaxon").as[(Option[Taxon], Option[Taxon])]

    val totalReads = referenceData.count()
    val classified = joint.filter("testTaxon is not null").count()

    println(s"Total reads $totalReads")
    println(s"Classified $classified ${formatPerc(classified.toDouble/totalReads)}")

    levelComparison(joint, "G", totalReads)
  }

  /** Comparison at a specific taxonomic level */
  def levelComparison(refCmp: Dataset[(Option[Taxon], Option[Taxon])], level: String, totalReads: Long) = {
    import spark.sqlContext.implicits._
    val bctax = this.tax
    val categoryBreakdown = refCmp.mapPartitions(p => {
      val tv = bctax.value
      p.map(x => {
        hitCategory(tv, x._1, x._2, level).toString
      })
    }).toDF("category").groupBy("category").count().as[(String, Long)].cache()
    categoryBreakdown.show()
    val catMap = categoryBreakdown.collect().toMap

    val sensitivity = catMap("TruePos").toDouble / totalReads
    val ppv = catMap("TruePos").toDouble / (catMap("FalsePos") + catMap("TruePos"))
    println(s"PPV ${formatPerc(ppv)} Sensitivity ${formatPerc(sensitivity)}")
  }

  def readKrakenFormat(location: String)(implicit spark: SparkSession): Dataset[(SeqTitle, Taxon)] = {
    import spark.sqlContext.implicits._
    spark.read.option("sep", "\t").csv(location).
      filter(x => x.getString(0) == "C"). //keep only classified reads
      map(x => (x.getString(1), x.getString(2).toInt))
  }

  def readCustomFormat(location: String, idCol: Int, taxonCol: Int, skipHeader: Boolean)(implicit spark: SparkSession):
    Dataset[(SeqTitle, Taxon)] = {
    import spark.sqlContext.implicits._
    spark.read.option("sep", "\t").option("header", skipHeader.toString).csv(location).
      map(x => (x.getString(idCol - 1), x.getString(taxonCol - 1).toInt))
  }
}

object MappingComparison {

  //For an explanation of these categories see the Kraken 2 paper
  //https://genomebiology.biomedcentral.com/articles/10.1186/s13059-019-1891-0#ethics
  //section "Evaluation of accuracy in strain exclusion experiments"
  sealed trait HitCategory extends Serializable

  /** At or below the expected taxon */
  case object TruePos extends HitCategory

  /** Ancestor of the expected taxon */
  case object VaguePos extends HitCategory

  /** Not classified, but should have been */
  case object FalseNeg extends HitCategory

  /** Wrongly classified */
  case object FalsePos extends HitCategory

  def hitCategory(tax: Taxonomy, refTaxon: Option[Taxon], testTaxon: Option[Taxon], level: String): HitCategory = {
    (refTaxon, testTaxon) match {
      case (Some(ref), Some(test)) =>
        val levelAncestor = tax.ancestorAtLevel(ref, level)
        if (ref == test) TruePos
        else if (levelAncestor != Taxonomy.ROOT && tax.hasAncestor(test, levelAncestor)) TruePos
        else if (tax.hasAncestor(ref, test)) VaguePos //classified as some ancestor of ref
        else FalsePos
      case (Some(_), None) => FalseNeg
      case (None, Some(_)) =>
        throw new Exception("Found a read that has no corresponding taxon in the reference. Is the reference complete?")
      case (None, None) => ???
    }
  }

  def formatPerc(d: Double) = "%.3f%%".format(d * 100)
}