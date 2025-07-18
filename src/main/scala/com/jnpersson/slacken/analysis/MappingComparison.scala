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

import com.jnpersson.kmers._
import com.jnpersson.kmers.Helpers.formatPerc
import com.jnpersson.slacken.Taxonomy.{Genus, Rank, Species}
import com.jnpersson.slacken.{Taxon, Taxonomy}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.{avg, isnotnull, udf}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}

import scala.collection.mutable

/** A single line of per-taxon metrics, corresponding to the classification of one sample and a set of parmeters. */
final case class PerTaxonMetrics(classifiedCount: Long, refCount: Long, precision: Double, recall: Double) {
  def toTSVString: String = s"$classifiedCount\t$refCount\t$precision\t$recall"
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
  private val pattern1 = """(.*)/(.*)/(.+)_(\d+)_(\d+)_s(\d+)_c([\d.]+)_classified/sample=(.*)""".r

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

/** Compare a set of classifications against a reference.
 * @param tax The taxonomy
 * @param refIdCol Read ID column in reference, 1-based.
 * @param refTaxonCol Read taxon column in reference, 1-based.
 * @param withHeader Whether to skip a single header line in the reference
 * @param minCountTaxon Min read count for a taxon to be considered present
 * @param multiSample Whether the test data was generated in multi-sample mode
 * */
class MappingComparison(tax: Broadcast[Taxonomy],
                        refIdCol: Int, refTaxonCol: Int, withHeader: Boolean,
                        minCountTaxon: Long,
                        multiSample: Boolean)(implicit spark: SparkSession) {
  import MappingComparison._
  import spark.sqlContext.implicits._


  /** Process directories of multi-sample classifications.
   * A diretory prefix indicates where the reference mapping for each sample can be found.
   */
  def processDirectories(dirs: List[String], outputPrefix: String, referencePrefix: String): Unit = {
    /** Find subdirectories that match the pattern */
    val pattern = ".*sample=(.+)".r
    val allSamplesMetrics = for {
      dir <- dirs
      subdir <- HDFSUtil.subdirectories(dir)
      sampleMatch <- pattern.findFirstMatchIn(subdir)
      sample = sampleMatch.group(1)
      reference = s"$referencePrefix/sample$sample/reads_mapping.tsv"
    } yield allMetrics(s"$dir/$subdir", reference)

    HDFSUtil.writeTextLines(outputPrefix + "_metrics.tsv",
      Iterator(Metrics.header) ++ allSamplesMetrics.flatMap(_.flatMap(_.toTSVString)))
  }

  /** Process a set of classification files with a common reference mapping. */
  def processFiles(files: Iterable[String], outputPrefix: String, reference: String): Unit = {
    val allFilesMetrics = for {
      f <- files
      metrics <- allMetrics(f, reference)
    } yield metrics
    HDFSUtil.writeTextLines(outputPrefix + "_metrics.tsv",
      Iterator(Metrics.header) ++ allFilesMetrics.flatMap(_.toTSVString))
  }

  def unfilteredRefData(file: String): DataFrame =
    readCustomFormat(file, refIdCol, refTaxonCol).
    toDF("id", "refTaxon")

  def readReferenceData(file: String): DataFrame = {
    val bcTax = this.tax
    val unfiltered = unfilteredRefData(file)

    for {
      taxon <- unfiltered.select("refTaxon").distinct().as[Taxon].collect()
      if ! tax.value.isDefined(taxon)
    } {
      println(s"Reference contains unknown taxon, not known to taxonomy: $taxon. It will be filtered out.")
    }

    val contains = udf((x: Taxon) => bcTax.value.isDefined(x))
    unfiltered.filter(contains($"refTaxon"))
  }

  def allMetrics(dataFile: String, reference: String): Iterator[Metrics] = {
    val referenceData = readReferenceData(reference).cache()
    println(s"Filtered reference size: ${referenceData.count()}")

    try {
      println(s"--------$dataFile--------")
      Iterator(
        allMetrics(dataFile, Some(Genus), referenceData),
        allMetrics(dataFile, Some(Species), referenceData)
      )
    } finally {
      referenceData.unpersist()
    }
  }

  /** Produce metrics for one dataFile (single file or directory of TSV) */
  def allMetrics(dataFile: String, rank: Option[Rank], referenceData: DataFrame): Metrics = {
    val spl = dataFile.split("/")
    val title = if (multiSample) spl.takeRight(4).mkString("/") else spl.last

    //Inner join: filter out reads not present in the reference
    //(may have been removed due to unknown taxon).

    val cmpData = readKrakenFormat(dataFile).toDF("id", "testTaxon").
      join(referenceData, List("id")).select("id", "testTaxon").
      cache()
    try {
      Metrics(title, rank,
        perTaxonComparison(cmpData, rank, referenceData),
        perReadComparison(cmpData, rank, referenceData)
      )
    } finally {
      cmpData.unpersist()
    }
  }

  def perTaxonComparison(cmpDataRaw: DataFrame, rank: Option[Rank], referenceData: DataFrame): PerTaxonMetrics = {
    val tax = this.tax
    val ancestorAtLevel = udf((x: Taxon) =>
      rank.flatMap(r => tax.value.standardAncestorAtLevel(x, r)))
    val cmpData = cmpDataRaw.withColumn("taxon", ancestorAtLevel($"testTaxon")).
      where(isnotnull($"taxon").and($"taxon" =!= Taxonomy.NONE))

    val refClass = referenceData.
      withColumn("taxon", ancestorAtLevel($"refTaxon")).
      where(isnotnull($"taxon"))

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
    val falsePos = (cmpTaxa &~= refTaxa -- vagueTaxa).size
    val vaguePos = cmpTaxa.intersect(vagueTaxa).size
    val falseNeg = (refTaxa &~= cmpTaxa).size
    val precision = truePos.toDouble / (cmpTaxa -- vagueTaxa).size
    val recall = truePos.toDouble / refTaxa.size

    println(s"*** Per taxon comparison (minimum $minCountTaxon) at level $rank")
    println(s"Total ref taxa ${refTaxa.size}, total classified taxa ${cmpTaxa.size}")
    println(s"TP $truePos, VP $vaguePos FP $falsePos, FN $falseNeg}")
    println(s"Precision ${formatPerc(precision)} recall ${formatPerc(recall)}")
    println("")

    PerTaxonMetrics(cmpTaxa.size, refTaxa.size, precision, recall)
  }

  def perReadComparison(cmpData: DataFrame, rank: Option[Rank], referenceData: DataFrame): PerReadMetrics = {
    //Inner join (effectively an intersection here), reasoning:
    //1) disregard reads that were present in the reference but not in the sample classified.
    //The reference is treated as a potential superset.
    //2) disregard reads that have no mapping in the reference, as they can't be meaningfully checked.
    val joint = referenceData.join(cmpData, referenceData("id") === cmpData("id")).
      select("refTaxon", "testTaxon").as[(Taxon, Taxon)]

    val totalReads = joint.count()
    val classified = joint.filter($"testTaxon" =!= Taxonomy.NONE).count()

    println(s"Total reads $totalReads")
    println(s"Classified $classified ${formatPerc(classified.toDouble/totalReads)}")

    perReadComparison(joint, rank, classified, totalReads)
  }

  /** Per read comparison at a specific taxonomic level */
  def perReadComparison(refCmp: Dataset[(Taxon, Taxon)], rank: Option[Rank], classified: Long,
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
    val avgIndex = hitCategories.where(isnotnull($"index")).agg(avg("index")).as[Option[Double]].collect()(0).
      getOrElse(Double.NaN)

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
      map(x => (x.getString(1), bcTax.value.primary(x.getString(2).toInt)))
  }

  def readCustomFormat(location: String, idCol: Int, taxonCol: Int):
    Dataset[(SeqTitle, Taxon)] = {
    val bcTax = this.tax
    spark.read.option("sep", "\t").option("header", withHeader.toString).csv(location).
      filter(x => !x.getString(idCol - 1).contains("/2")). //paired end
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
    def hitIndex: Option[Int] = Some(9)
  }

  /** Incorrectly classified */
  case object FalsePos extends HitCategory("FalsePos") {
    //Maximally wrong index
    def hitIndex: Option[Int] = Some(9)
  }

  def hitCategory(tax: Taxonomy, refTaxon: Taxon, testTaxon: Taxon, level: Option[Rank]): HitCategory = {
    (refTaxon, testTaxon) match {
      case (rt, Taxonomy.NONE) => FalseNeg
      case (ref, test) =>
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
    }
  }
}