/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */


package com.jnpersson.slacken

import com.jnpersson.discount.SeqTitle
import com.jnpersson.slacken.Taxonomy.Rank
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Dataset, SparkSession, functions}

import scala.collection.mutable

class MappingComparison(tax: Broadcast[Taxonomy], reference: String,
                        refIdCol: Int, refTaxonCol: Int, skipHeader: Boolean)(implicit spark: SparkSession) {
  import MappingComparison._
  import spark.sqlContext.implicits._

  val referenceData = readCustomFormat(reference, refIdCol, refTaxonCol).
    toDF("id", "refTaxon").
    cache()

  def perTaxonComparison(dataFile: String, level: Rank, minCount: Long): Unit = {
    val tax = this.tax
    val ancestorAtLevel = udf((x: Taxon) => tax.value.ancestorAtLevel(x, level))
    val cmpData = readKrakenFormat(dataFile).toDF("id", "testTaxon").
      withColumn("taxon", ancestorAtLevel($"testTaxon"))

    val refClass = referenceData.
      withColumn("taxon", ancestorAtLevel($"refTaxon"))
    val refVector = abundanceVector(
      refClass.groupBy("taxon").
        agg(functions.count("taxon").as("count")).
        select("taxon", "count").
        as[(Taxon, Long)].collect()
    )
    val refTaxa = mutable.BitSet.empty ++ refClass.
      select("taxon").distinct().
      as[Taxon].collect()

    val cmpClass = cmpData.
      groupBy("taxon").agg(functions.count("*").as("count")).
      filter(s"count >= $minCount")
    val cmpVector = abundanceVector(
      cmpClass.select("taxon", "count").as[(Taxon, Long)].collect()
    )
    val cmpTaxa = mutable.BitSet.empty ++ cmpClass.select("taxon").
      as[Taxon].collect()

    val truePos = refTaxa.intersect(cmpTaxa).size
    val falsePos = (cmpTaxa -- refTaxa).size
    val falseNeg = (refTaxa -- cmpTaxa).size
    val precision = truePos.toDouble / cmpTaxa.size
    val recall = truePos.toDouble / refTaxa.size
    val l1 = l1Dist(cmpVector, refVector)

    println(s"*** Per taxon comparison (minimum $minCount) at level $level")
    println(s"Total ref taxa ${refTaxa.size}, total classified taxa ${cmpTaxa.size}")
    println(s"TP $truePos, FP $falsePos, FN $falseNeg, L1 dist. ${"%.3g".format(l1)}")
    println(s"Precision ${formatPerc(precision)} recall ${formatPerc(recall)}")
    println("")
  }

  def abundanceVector(countedTaxons: Array[(Taxon, Long)]): Array[Double] = {
    val r = new Array[Double](tax.value.parents.length)
    val totalWeight = countedTaxons.map(_._2).sum
    for { (taxon, c) <- countedTaxons } {
      r(taxon) = c.toDouble / totalWeight
    }
    r
  }

  /** Compute L1 distance of two vectors of equal length. */
  def l1Dist(v1: Array[Double], v2: Array[Double]): Double = {
    var r = 0d
    for { i <- v1.indices } {
      r += Math.abs(v1(i) - v2(i))
    }
    r
  }

  def perReadComparison(dataFile: String, level: Rank): Unit = {
    val cmpData = readKrakenFormat(dataFile).toDF("id", "testTaxon")
    val joint = referenceData.join(cmpData, referenceData("id") === cmpData("id"), "outer").
      select("refTaxon", "testTaxon").as[(Option[Taxon], Option[Taxon])]

    val totalReads = referenceData.count()
    val classified = joint.filter("testTaxon is not null").count()

    println(s"Total reads $totalReads")
    println(s"Classified $classified ${formatPerc(classified.toDouble/totalReads)}")

    perReadComparison(joint, level, totalReads)
  }

  /** Per read comparison at a specific taxonomic level */
  def perReadComparison(refCmp: Dataset[(Option[Taxon], Option[Taxon])], level: Rank, totalReads: Long) = {
    println(s"*** Per-read comparison at level $level")
    val bctax = this.tax
    val categoryBreakdown = refCmp.mapPartitions(p => {
      val tv = bctax.value
      p.map(x => {
        hitCategory(tv, x._1, x._2, level).toString
      })
    }).toDF("category").groupBy("category").count().as[(String, Long)].cache()
    categoryBreakdown.show()
    val catMap = categoryBreakdown.collect().toMap

    val tp = catMap.getOrElse("TruePos", 0L)
    val fp = catMap.getOrElse("FalsePos", 0L)
    val sensitivity = tp.toDouble / totalReads
    val ppv = if (tp + fp > 0)
      tp.toDouble / (tp + fp)
    else 0
    println(s"PPV ${formatPerc(ppv)} Sensitivity ${formatPerc(sensitivity)}")
    println("")
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