/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */


package com.jnpersson.slacken.analysis

import com.jnpersson.discount.SeqTitle
import com.jnpersson.discount.spark.Output.formatPerc
import com.jnpersson.slacken.Taxonomy.{Genus, ROOT, Rank, Species}
import com.jnpersson.slacken.{Taxon, Taxonomy}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}

import scala.collection.mutable


final case class PerTaxonMetrics(refCount: Long, classifiedCount: Long, l1: Double, precision: Double, recall: Double,
                           unifrac: Double) {
  def toTSVString: String = s"$refCount\t$classifiedCount\t$l1\t$precision\t$recall\t$unifrac"
}

object PerTaxonMetrics {
  def header = "taxon_classified\ttaxon_total\ttaxon_l1\ttaxon_precision\ttaxon_recall\ttaxon_unifrac"
}

case class PerReadMetrics(classifiedCount: Long, totalCount: Long, truePos: Long, falsePos: Long, vaguePos: Long,
                          falseNeg: Long, ppv: Double, sensitivity: Double) {
  def toTSVString: String = s"$classifiedCount\t$totalCount\t$truePos\t$falsePos\t$vaguePos\t$falseNeg\t$ppv\t$sensitivity"
}

object PerReadMetrics {
  def header = "read_classified\tread_total\tread_tp\tread_fp\tread_vp\tread_fn\tread_ppv\tread_sensitivity"
}

/** A single result line */
final case class Metrics(title: String, rank: Option[Rank], perTaxon: PerTaxonMetrics, perRead: PerReadMetrics) {
  //Extract some variables from expected filename patterns
  val pattern1 = """M1_S(\d+)_(s2_2023|nt|s2_2023_all)_(\d+)_(\d+)(f|ff|)(_\d+)?(_s\d+)?(_c[\d.]+)?_classified""".r

  val pattern2 = """(.*)/(.*)/(s2_2023|nt|s2_2023_all)_(\d+)_(\d+)_s(\d+)_c([\d.]+)_classified/sample=(.*)""".r

  def toTSVString: Option[String] = title match {
    case pattern1(sample, library, k, m, freqRaw, freqLenRaw, sRaw, cRaw) =>

      //Note: freqLen is meaningless if the ordering is not frequency
      val freqLen = Option(freqLenRaw).map(_.drop(1)).getOrElse("10")
      val c = Option(cRaw).map(_.drop(2)).getOrElse("0.0")
      val s = Option(sRaw).map(_.drop(2)).getOrElse("7")
      val frequency = if (freqRaw == "") "none" else freqRaw
      val rankStr = rank.map(_.toString).getOrElse("All")
      Some(
        s"$title\t\t\t$sample\t$library\t$k\t$m\t$frequency\t$freqLen\t$s\t$c\t$rankStr\t${perTaxon.toTSVString}\t${perRead.toTSVString}"
      )
    case pattern2(family, group, library, k, m, s, c, sample) =>
      val rankStr = rank.map(_.toString).getOrElse("All")
      Some(
        s"$title\t$family\t$group\t$sample\t$library\t$k\t$m\t0\t0\t$s\t$c\t$rankStr\t${perTaxon.toTSVString}\t${perRead.toTSVString}"
      )
    case _ =>
      println(s"Couldn't extract variables from filename: $title. Omitting from output.")
      None
  }
}

object Metrics {
  def header = s"title\tfamily\tgroup\tsample\tlibrary\tk\tm\tfrequency\tfl\ts\tc\trank\t${PerTaxonMetrics.header}\t${PerReadMetrics.header}"
}

class MappingComparison(tax: Broadcast[Taxonomy], reference: String,
                        refIdCol: Int, refTaxonCol: Int, skipHeader: Boolean,
                        minCountTaxon: Long,
                        multiSample: Boolean)(implicit spark: SparkSession) {
  import MappingComparison._
  import spark.sqlContext.implicits._

  val unfilteredRefData = readCustomFormat(reference, refIdCol, refTaxonCol).
    toDF("id", "refTaxon").cache()

  def readReferenceData: DataFrame = {
    val bcTax = this.tax

    for {
      taxon <- unfilteredRefData.select("refTaxon").distinct().as[Taxon].collect()
      if ! tax.value.isDefined(taxon)
    } {
      println(s"Reference contains unknown taxon, not known to taxonomy: $taxon. It will be filtered out.")
    }

    val contains = udf((x: Taxon) => bcTax.value.isDefined(x))
    unfilteredRefData.filter(contains($"refTaxon")).cache
  }

  val referenceData = readReferenceData
  println(s"Filtered reference size: ${referenceData.count}, unfiltered size: ${unfilteredRefData.count}")

  def allMetrics(dataFile: String): Iterator[Metrics] = {
    println(s"--------$dataFile--------")
    Iterator(
      allMetrics(dataFile, Some(Genus)),
      allMetrics(dataFile, Some(Species)),
      allMetrics(dataFile, None)
    )
  }

  def allMetrics(dataFile: String, rank: Option[Rank]): Metrics = {
    val spl = dataFile.split("/")
    val title = if (multiSample) spl.takeRight(4).mkString("/") else spl.last

    //Inner join: filter out reads not present in the reference
    //(may have been removed due to unknown taxon).

    val cmpData = readKrakenFormat(dataFile).toDF("id", "testTaxon").
      join(referenceData, List("id")).select("id", "testTaxon").
      cache()
    try {
      Metrics(title, rank,
        perTaxonComparison(cmpData, rank),
        perReadComparison(cmpData, rank)
      )
    } finally {
      cmpData.unpersist()
    }
  }

  def perTaxonComparison(cmpDataRaw: DataFrame, rank: Option[Rank]): PerTaxonMetrics = {
    val tax = this.tax
    val ancestorAtLevel = udf((x: Taxon) => tax.value.ancestorAtLevel(x, rank))
    val cmpData = cmpDataRaw.withColumn("taxon", ancestorAtLevel($"testTaxon"))

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
    val vagueTaxa = tax.value.taxaWithAncestors(refTaxa) -- refTaxa

    val cmpClass = cmpData.
      groupBy("taxon").agg(functions.count("*").as("count")).
      filter(s"count >= $minCountTaxon")
    val cmpVector = abundanceVector(
      cmpClass.select("taxon", "count").as[(Taxon, Long)].collect()
    )

    //when ranks are not rounded to a standard level, we remove every classification above species level instead
    val cmpTaxa = mutable.BitSet.empty ++ cmpClass.select("taxon").
      as[Taxon].collect().filter(
      x => rank.nonEmpty || tax.value.depth(x) >= Species.depth)

    val truePos = refTaxa.intersect(cmpTaxa).size
    val falsePos = (cmpTaxa -- refTaxa -- vagueTaxa).size
    val vaguePos = cmpTaxa.intersect(vagueTaxa).size
    val falseNeg = (refTaxa -- cmpTaxa).size
    val precision = truePos.toDouble / (cmpTaxa -- vagueTaxa).size
    val recall = truePos.toDouble / refTaxa.size
    val l1 = l1Dist(cmpVector, refVector)
    val unifrac = new UniFrac(tax.value, cmpTaxa, refTaxa).distance

    println(s"*** Per taxon comparison (minimum $minCountTaxon) at level $rank")
    println(s"Total ref taxa ${refTaxa.size}, total classified taxa ${cmpTaxa.size}")
    println(s"TP $truePos, VP $vaguePos FP $falsePos, FN $falseNeg, L1 dist. ${"%.3g".format(l1)} unifrac dist. ${"%.3g".format(unifrac)}")
    println(s"Precision ${formatPerc(precision)} recall ${formatPerc(recall)}")
    println("")

    PerTaxonMetrics(cmpTaxa.size, refTaxa.size, l1, precision, recall, unifrac)
  }

  def abundanceVector(countedTaxa: Array[(Taxon, Long)]): Array[Double] = {
    val r = new Array[Double](tax.value.size)
    val totalWeight = countedTaxa.map(_._2).sum
    for { (taxon, c) <- countedTaxa} {
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

  def perReadComparison(cmpData: DataFrame, rank: Option[Rank]): PerReadMetrics = {
    val joint = referenceData.join(cmpData, referenceData("id") === cmpData("id"), "outer").
      select("refTaxon", "testTaxon").as[(Option[Taxon], Option[Taxon])]

    val totalReads = referenceData.count()
    val classified = joint.filter("testTaxon is not null").count()

    println(s"Total reads $totalReads")
    println(s"Classified $classified ${formatPerc(classified.toDouble/totalReads)}")

    perReadComparison(joint, rank, classified, totalReads)
  }

  /** Per read comparison at a specific taxonomic level */
  def perReadComparison(refCmp: Dataset[(Option[Taxon], Option[Taxon])], rank: Option[Rank], classified: Long,
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
      filter(x => !x.getString(idCol -1).contains("/2")). //paired end
      map(x => (
        x.getString(idCol - 1).replaceAll("/1", ""),  //remove /1 in paired end reads
        x.getString(taxonCol - 1).toInt)
      )
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

  def hitCategory(tax: Taxonomy, refTaxon: Option[Taxon], testTaxon: Option[Taxon], level: Option[Rank]): HitCategory = {
    (refTaxon, testTaxon) match {
      case (Some(ref), Some(test)) =>
        val levelAncestor = tax.ancestorAtLevel(ref, level)
        if (ref == test) TruePos
        else if (levelAncestor != Taxonomy.ROOT && tax.hasAncestor(test, levelAncestor)) TruePos
        else if (levelAncestor == Taxonomy.ROOT || tax.hasAncestor(ref, test)) VaguePos //classified as some ancestor of ref
        else if (test == Taxonomy.ROOT) VaguePos //We never consider ROOT to be a TruePos since this contains no information
        else FalsePos
      case (Some(_), None) => FalseNeg
      case (None, Some(_)) =>
        throw new Exception("Found a read that has no corresponding taxon in the reference. Is the reference complete?")
      case (None, None) => ???
    }
  }
}