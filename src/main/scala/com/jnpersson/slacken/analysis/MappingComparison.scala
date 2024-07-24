/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */


package com.jnpersson.slacken.analysis

import com.jnpersson.kmers._
import com.jnpersson.kmers.minimizer._
import com.jnpersson.kmers.Output.formatPerc
import com.jnpersson.slacken.Taxonomy.{Genus, Rank, Species}
import com.jnpersson.slacken.{Taxon, Taxonomy}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.{avg, isnotnull, udf}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}

import scala.collection.mutable

/** A single line of per-taxon metrics, corresponding to the classification of one sample and a set of parmeters. */
final case class PerTaxonMetrics(refCount: Long, classifiedCount: Long, precision: Double, recall: Double) {
  def toTSVString: String = s"$refCount\t$classifiedCount\t$precision\t$recall"
}

object PerTaxonMetrics {
  def header = "taxon_classified\ttaxon_total\ttaxon_precision\ttaxon_recall"
}

/** A single line of per-read metrics, corresponding to the classification of one sample and a set of parameters. */
final case class PerReadMetrics(classifiedCount: Long, totalCount: Long, truePos: Long, falsePos: Long, vaguePos: Long,
                          falseNeg: Long, ppv: Double, sensitivity: Double, index: Double) {
  def toTSVString: String = s"$classifiedCount\t$totalCount\t$truePos\t$falsePos\t$vaguePos\t$falseNeg\t$ppv\t$sensitivity\t$index"
}

object PerReadMetrics {
  def header = "read_classified\tread_total\tread_tp\tread_fp\tread_vp\tread_fn\tread_ppv\tread_sensitivity\tread_index"
}

/** A single result line (per taxon and per read) */
final case class Metrics(title: String, rank: Option[Rank], perTaxon: PerTaxonMetrics, perRead: PerReadMetrics) {
  //Extract some variables from expected filename patterns
  val pattern1 = """(.*)/(.*)/(.+)_(\d+)_(\d+)_s(\d+)_c([\d.]+)_classified/sample=(.*)""".r

  def toTSVString: Option[String] = title match {
    case pattern1(family, group, library, k, m, s, c, sample) =>
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

/** Compare a set of classifications against a reference. */
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
      allMetrics(dataFile, Some(Species))
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
    val ancestorAtLevel = udf((x: Taxon) =>
      rank.flatMap(r => tax.value.standardAncestorAtLevel(x, r)).getOrElse(x))
    val cmpData = cmpDataRaw.withColumn("taxon", ancestorAtLevel($"testTaxon"))

    val refClass = referenceData.
      withColumn("taxon", ancestorAtLevel($"refTaxon"))

    val refTaxa = mutable.BitSet.empty ++ refClass.
      select("taxon").distinct().
      as[Taxon].collect()
    val vagueTaxa = tax.value.taxaWithAncestors(refTaxa) -- refTaxa

    val cmpClass = cmpData.
      groupBy("taxon").agg(functions.count("*").as("count")).
      filter(s"count >= $minCountTaxon")

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

    println(s"*** Per taxon comparison (minimum $minCountTaxon) at level $rank")
    println(s"Total ref taxa ${refTaxa.size}, total classified taxa ${cmpTaxa.size}")
    println(s"TP $truePos, VP $vaguePos FP $falsePos, FN $falseNeg}")
    println(s"Precision ${formatPerc(precision)} recall ${formatPerc(recall)}")
    println("")

    PerTaxonMetrics(cmpTaxa.size, refTaxa.size, precision, recall)
  }

  def perReadComparison(cmpData: DataFrame, rank: Option[Rank]): PerReadMetrics = {
    //Right join, because we disregard reads that were present in the reference but not in the sample classified.
    //The reference is treated as a potential superset.
    val joint = referenceData.join(cmpData, referenceData("id") === cmpData("id"), "right").
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
    val hitCategories = refCmp.mapPartitions(p => {
      val tv = bctax.value
      p.map(x => {
        val cat = hitCategory(tv, x._1, x._2, rank)
        (cat.hitClass, cat.hitIndex)
      })
    }).toDF("category", "index").cache()
    val categoryBreakdown = hitCategories.groupBy("category").count().as[(String, Long)].cache()
    val avgIndex = hitCategories.where(isnotnull($"index")).agg(avg("index")).as[Double].collect()(0)

    val catMap = categoryBreakdown.collect().toMap.withDefaultValue(0L)

    val tp = catMap("TruePos")
    val fp = catMap("FalsePos")
    val vp = catMap("VaguePos")
    val fn = catMap("FalseNeg")
    val sensitivity = tp.toDouble / totalReads
    val ppv = if (tp + fp > 0)
      tp.toDouble / (tp + fp)
    else 0
    println(s"PPV ${formatPerc(ppv)} Sensitivity ${formatPerc(sensitivity)}")
    println("")
    PerReadMetrics(classified, totalReads, tp, fp, vp, fn, ppv, sensitivity, avgIndex)
  }

  def readKrakenFormat(location: String): Dataset[(SeqTitle, Taxon)] = {
    val bcTax = this.tax
    spark.read.option("sep", "\t").csv(location).
      filter(x => x.getString(0) == "C"). //keep only classified reads
      map(x => (x.getString(1), bcTax.value.primary(x.getString(2).toInt)))
  }

  def readCustomFormat(location: String, idCol: Int, taxonCol: Int):
    Dataset[(SeqTitle, Taxon)] = {
    val bcTax = this.tax
    spark.read.option("sep", "\t").option("header", skipHeader.toString).csv(location).
      filter(x => !x.getString(idCol -1).contains("/2")). //paired end
      map(x => (
        x.getString(idCol - 1).replaceAll("/1", ""),  //remove /1 in paired end reads
        bcTax.value.primary(x.getString(taxonCol - 1).toInt))
      )
  }
}

object MappingComparison {

  //For an explanation of these categories see the Kraken 2 paper
  //https://genomebiology.biomedcentral.com/articles/10.1186/s13059-019-1891-0#ethics
  //section "Evaluation of accuracy in strain exclusion experiments"
  sealed abstract class HitCategory(val hitClass: String) extends Serializable {

    /** Number of standardised steps from the expected taxon (reference) to any ancestor that it was classified as, if applicable.
     * If 0, then the read was classified correctly. If the read was unclassified,
     * then this is inapplicable.
     * If the read was a false positive, then the index is maximally large. */
    def hitIndex: Option[Int]
  }

  /** At or below the expected taxon */
  case object TruePos extends HitCategory("TruePos") {
    def hitIndex: Option[Int] = Some(0)
  }

  /** Ancestor of the expected taxon.
   * @param index Number of steps from the expected taxon to the ancestor the read was classified as */
  final case class VaguePos(index: Int) extends HitCategory("VaguePos") {
    def hitIndex: Option[Int] = Some(index)
  }

  /** Not classified, but should have been */
  case object FalseNeg extends HitCategory("FalseNeg") {
    def hitIndex: Option[Int] = None
  }

  /** Incorrectly classified */
  case object FalsePos extends HitCategory("FalsePos") {
    //Maximally wrong index
    def hitIndex: Option[Int] = Some(9)
  }

  def hitCategory(tax: Taxonomy, refTaxon: Option[Taxon], testTaxon: Option[Taxon], level: Option[Rank]): HitCategory = {
    (refTaxon, testTaxon) match {
      case (Some(ref), Some(test)) =>
        val refAncestor =
          level.flatMap(l => tax.standardAncestorAtLevel(ref, l)).getOrElse(ref)
        if (ref == test) TruePos
        else if (refAncestor != Taxonomy.ROOT && tax.hasAncestor(test, refAncestor)) TruePos
        else if (refAncestor == Taxonomy.ROOT || tax.hasAncestor(ref, test)) {
          //classified as some ancestor of ref
          VaguePos(tax.standardStepsToAncestor(ref, test))
        }
        else if (test == Taxonomy.ROOT) {
          //We never consider ROOT to be a TruePos since this contains no information
          VaguePos(tax.standardStepsToAncestor(ref, test))
        }
        else FalsePos
      case (Some(_), None) => FalseNeg
      case (None, Some(_)) =>
        throw new Exception("Found a read that has no corresponding taxon in the reference. Is the reference complete?")
      case (None, None) => ???
    }
  }
}