/*
 * This file is part of Hypercut. Copyright (c) 2023 Johan Nyström-Persson.
 */


package com.jnpersson.slacken

import com.jnpersson.discount.SeqTitle
import com.jnpersson.slacken.Taxonomy.Rank
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SparkSession}

class MappingComparison(tax: Broadcast[Taxonomy], reference: String,
                        refIdCol: Int, refTaxonCol: Int, skipHeader: Boolean)(implicit spark: SparkSession) {
  import MappingComparison._
  import spark.sqlContext.implicits._

  val referenceData = readCustomFormat(reference, refIdCol, refTaxonCol).
    toDF("id", "refTaxon").
    cache()

  def compare(dataFile: String, level: Rank): Unit = {
    val cmpData = readKrakenFormat(dataFile).toDF("id", "testTaxon")
    val joint = referenceData.join(cmpData, referenceData("id") === cmpData("id"), "outer").
      select("refTaxon", "testTaxon").as[(Option[Taxon], Option[Taxon])]

    val totalReads = referenceData.count()
    val classified = joint.filter("testTaxon is not null").count()

    println(s"Total reads $totalReads")
    println(s"Classified $classified ${formatPerc(classified.toDouble/totalReads)}")

    levelComparison(joint, level, totalReads)
  }

  /** Comparison at a specific taxonomic level */
  def levelComparison(refCmp: Dataset[(Option[Taxon], Option[Taxon])], level: Rank, totalReads: Long) = {
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

  def readKrakenFormat(location: String): Dataset[(SeqTitle, Taxon)] = {
    spark.read.option("sep", "\t").csv(location).
      filter(x => x.getString(0) == "C"). //keep only classified reads
      map(x => (x.getString(1), x.getString(2).toInt))
  }

  def readCustomFormat(location: String, idCol: Int, taxonCol: Int):
    Dataset[(SeqTitle, Taxon)] = {
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

  def hitCategory(tax: Taxonomy, refTaxon: Option[Taxon], testTaxon: Option[Taxon], level: Rank): HitCategory = {
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