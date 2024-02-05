/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */


package com.jnpersson.slacken.analysis

import com.jnpersson.discount.SeqTitle
import com.jnpersson.slacken.Taxonomy.{Genus, Rank, Species}
import com.jnpersson.slacken.{Taxon, Taxonomy}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Dataset, SparkSession, functions}

import scala.collection.mutable


case class PerTaxonMetrics(refCount: Long, classifiedCount: Long, l1: Double, precision: Double, recall: Double,
                           unifrac: Double) {
  def toTSVString: String = s"$refCount\t$classifiedCount\t$l1\t$precision\t$recall\t$unifrac"
}

object PerTaxonMetrics {
  def header = s"taxon_classified\ttaxon_total\ttaxon_l1\ttaxon_precision\ttaxon_recall\ttaxon_unifrac"
}

case class PerReadMetrics(classifiedCount: Long, totalCount: Long, truePos: Long, falsePos: Long, vaguePos: Long,
                          falseNeg: Long, ppv: Double, sensitivity: Double) {
  def toTSVString: String = s"$classifiedCount\t$totalCount\t$truePos\t$falsePos\t$vaguePos\t$falseNeg\t$ppv\t$sensitivity"
}

object PerReadMetrics {
  def header = s"read_classified\tread_total\tread_tp\tread_fp\tread_vp\tread_fn\tread_ppv\tread_sensitivity"
}

/** A single result line */
case class Metrics(title: String, rank: Rank, perTaxon: PerTaxonMetrics, perRead: PerReadMetrics) {
  //Extract some variables from expected filename patterns
  val pattern = """M1_S(\d+)_(s2_2023|nt|s2_2023_all)_(\d+)_(\d+)(f|ff|)(_\d+)?(_s\d+)?(_c[\d.]+)?_classified""".r

  def toTSVString = title match {
    case pattern(sample, library, k, m, frequency, freqLenRaw, sRaw, cRaw) =>
      //Note: freqLen is meaningless if the ordering is not frequency
      val freqLen = Option(freqLenRaw).map(_.drop(1)).getOrElse("10")
      val c = Option(cRaw).map(_.drop(2)).getOrElse("0.0")
      val s = Option(sRaw).map(_.drop(2)).getOrElse("7")
      s"$title\t$sample\t$library\t$k\t$m\t$frequency\t$freqLen\t$s\t$c\t$rank\t${perTaxon.toTSVString}\t${perRead.toTSVString}"
    case _ =>
      println(s"Couldn't extract variables from filename: $title")
      ???
  }
}

object Metrics {
  def header = s"title\tsample\tlibrary\tk\tm\tfrequency\tfl\ts\tc\trank\t${PerTaxonMetrics.header}\t${PerReadMetrics.header}"
}

class MappingComparison(tax: Broadcast[Taxonomy], reference: String,
                        refIdCol: Int, refTaxonCol: Int, skipHeader: Boolean,
                        minCountTaxon: Long)(implicit spark: SparkSession) {
  import MappingComparison._
  import spark.sqlContext.implicits._

  val referenceData = readCustomFormat(reference, refIdCol, refTaxonCol).
    toDF("id", "refTaxon").
    cache()

  def allMetrics(dataFile: String): Iterator[Metrics] = {
    println(s"--------$dataFile--------")
    Iterator(
      allMetrics(dataFile, Genus),
      allMetrics(dataFile, Species)
    )
  }

  def allMetrics(dataFile: String, rank: Rank): Metrics = {
    val title = dataFile.split("/").last
    Metrics(title, rank,
      perTaxonComparison(dataFile, rank),
      perReadComparison(dataFile, rank)
    )
  }

  def perTaxonComparison(dataFile: String, rank: Rank): PerTaxonMetrics = {
    val tax = this.tax
    val ancestorAtLevel = udf((x: Taxon) => tax.value.ancestorAtLevel(x, rank))
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
      filter(s"count >= $minCountTaxon")
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
    val unifrac = new UniFrac(tax.value, cmpTaxa, refTaxa).distance

    println(s"*** Per taxon comparison (minimum $minCountTaxon) at level $rank")
    println(s"Total ref taxa ${refTaxa.size}, total classified taxa ${cmpTaxa.size}")
    println(s"TP $truePos, FP $falsePos, FN $falseNeg, L1 dist. ${"%.3g".format(l1)} unifrac dist. ${"%.3g".format(unifrac)}")
    println(s"Precision ${formatPerc(precision)} recall ${formatPerc(recall)}")
    println("")

    PerTaxonMetrics(cmpTaxa.size, refTaxa.size, l1, precision, recall, unifrac)
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

  def perReadComparison(dataFile: String, rank: Rank): PerReadMetrics = {
    val cmpData = readKrakenFormat(dataFile).toDF("id", "testTaxon")
    val joint = referenceData.join(cmpData, referenceData("id") === cmpData("id"), "outer").
      select("refTaxon", "testTaxon").as[(Option[Taxon], Option[Taxon])]

    val totalReads = referenceData.count()
    val classified = joint.filter("testTaxon is not null").count()

    println(s"Total reads $totalReads")
    println(s"Classified $classified ${formatPerc(classified.toDouble/totalReads)}")

    perReadComparison(joint, rank, classified, totalReads)
  }

  /** Per read comparison at a specific taxonomic level */
  def perReadComparison(refCmp: Dataset[(Option[Taxon], Option[Taxon])], rank: Rank, classified: Long,
                        totalReads: Long): PerReadMetrics = {
    println(s"*** Per-read comparison at level $rank")
    val bctax = this.tax
    val categoryBreakdown = refCmp.mapPartitions(p => {
      val tv = bctax.value
      p.map(x => {
        hitCategory(tv, x._1, x._2, rank).toString
      })
    }).toDF("category").groupBy("category").count().as[(String, Long)].cache()
    categoryBreakdown.show()
    val catMap = categoryBreakdown.collect().toMap

    val tp = catMap.getOrElse("TruePos", 0L)
    val fp = catMap.getOrElse("FalsePos", 0L)
    val vp = catMap.getOrElse("VaguePos", 0L)
    val fn = catMap.getOrElse("FalseNeg", 0L)
    val sensitivity = tp.toDouble / totalReads
    val ppv = if (tp + fp > 0)
      tp.toDouble / (tp + fp)
    else 0
    println(s"PPV ${formatPerc(ppv)} Sensitivity ${formatPerc(sensitivity)}")
    println("")
    PerReadMetrics(classified, totalReads, tp, fp, vp, fn, ppv, sensitivity)
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