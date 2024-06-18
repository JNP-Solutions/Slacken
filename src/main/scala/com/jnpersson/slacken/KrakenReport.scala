/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */


package com.jnpersson.slacken

import com.jnpersson.slacken.Taxonomy.{NONE, ROOT, Rank, Root, Unclassified}

import java.io.PrintWriter
import scala.collection.mutable
import scala.collection.mutable.{Map => MMap}
import scala.collection.mutable.ListBuffer


/** Helper for aggregating per-taxon counts in the taxonomic tree */
class TreeAggregator(taxonomy: Taxonomy, counts: Array[(Taxon, Long)]) {
  def keys = taxonCounts.keys.toSeq

  val taxonCounts = (MMap.empty ++ counts).withDefaultValue(0L)

  val cladeTotals = MMap[Taxon, Long]().withDefaultValue(0L)
  for {
    (taxid, count) <- counts
    p <- taxonomy.pathToRoot(taxid)
  } cladeTotals(p) += count
}

class TotalKmerSizeAggregator(taxonomy: Taxonomy, genomeSizes: Array[(Taxon, Long)]) {
  val genomeSizesMap = genomeSizes.toMap
  val computedTreeMap: mutable.Map[Taxon, (Long, Long)] = computeFullTree()

  /**
   * Average kmer count among all leaf-children of that taxon.
   * (present in the report under the column header "TKC1-LeafOnly")
   * @param taxon
   * @return
   */
  def totKmerAverageS1(taxon: Taxon): Double = {
    val s1Agg = taxonomy.children(taxon).map(child => computedTreeMap(child)).
      reduceOption((aggSum, pair) => (aggSum._1 + pair._1, aggSum._2 + pair._2))
      .getOrElse(computedTreeMap(taxon))

    val s1AggWithTaxon = if(genomeSizesMap.contains(taxon)) (s1Agg._1 + genomeSizesMap(taxon),s1Agg._2 + 1) else s1Agg

    s1AggWithTaxon._1.toDouble / s1AggWithTaxon._2.toDouble
  }

  /**
   * Average kmer count of average kmer counts of all first (immediate) children of that taxon.
   * (present in the report under the column header "TKC2-FirstChildren")
   * @param taxon
   * @return
   */
  def totKmerAverageS2(taxon: Taxon): Double = {
    if (taxonomy.children(taxon).nonEmpty) {
      val s2Agg = taxonomy.children(taxon).map(child => computedTreeMap(child)).filter(_._2 > 0)
        .map(pair => pair._1.toDouble / pair._2.toDouble)
      val s2AggWithTaxon = if(genomeSizesMap.contains(taxon)) genomeSizesMap(taxon).toDouble::s2Agg else s2Agg

      s2AggWithTaxon.sum / s2AggWithTaxon.size.toDouble
    } else {
      val a = computedTreeMap(taxon)
      if (a._2 == 0) 0 else a._1 / a._2
    }
  }

  /**
   * Average kmer count among all children of that taxon.
   * (present in the report under the column header "TKC3-AllChildren ")
   * @param taxon
   * @return
   */
  def totKmerAverageS3(taxon: Taxon): Double = {
    val childrenNonZero = taxonomy.children(taxon).map(child => computedTreeMap(child)).filter(_._2 > 0)
    val s1Agg = childrenNonZero.reduceOption { (aggSum, pair) => (aggSum._1 + pair._1, aggSum._2 + pair._2) }
      .getOrElse(computedTreeMap(taxon))
    val nonZeroChildSize = childrenNonZero.size.toDouble

    if (s1Agg._2 + nonZeroChildSize == 0) 0 else {
      ((totKmerAverageS1(taxon) * s1Agg._2.toDouble) +
        (totKmerAverageS2(taxon) * nonZeroChildSize)) / (s1Agg._2.toDouble + nonZeroChildSize)
    }
  }

  def computeFullTree(): mutable.Map[Taxon, (Long, Long)] = {
    val results = mutable.Map[Taxon, (Long, Long)]()
    computeLeafAggAndCounts(ROOT, results)
    results
  }

  def computeLeafAggAndCounts(taxon: Taxon, results: mutable.Map[Taxon, (Long, Long)]): (Long, Long) = {
    val children = taxonomy.children(taxon)
    children match {
      case Nil =>
        if (genomeSizesMap.contains(taxon)) {
          results += (taxon -> (genomeSizesMap(taxon), 1))
          (genomeSizesMap(taxon), 1)
        } else {
          results += (taxon -> (0, 0))
          (0, 0)
        }

      case childList =>
        val genomeSizes = ListBuffer[Long]()
        val genomeCounts = ListBuffer[Long]()
        if (genomeSizesMap.contains(taxon)) {
          genomeSizes += genomeSizesMap(taxon)
          genomeCounts += 1
        }
        for {i <- childList
             leafCounts = computeLeafAggAndCounts(i, results)} {
          genomeSizes += leafCounts._1
          genomeCounts += leafCounts._2
        }
        results += (taxon -> (genomeSizes.sum, genomeCounts.sum))
        (genomeSizes.sum, genomeCounts.sum)

    }
  }

}

/** A Kraken 1/2 style taxonomic report with a tree structure.
 *
 * @param taxonomy         The taxonomy
 * @param counts           Number of hits (reads) for each taxon
 * @param compatibleFormat Traditional output format (no headers, identical to Kraken/Kraken 2 reports)
 */
class KrakenReport(taxonomy: Taxonomy, counts: Array[(Taxon, Long)], compatibleFormat: Boolean = false) {
  lazy val agg = new TreeAggregator(taxonomy, counts)
  lazy val cladeTotals = agg.cladeTotals
  lazy val taxonCounts = agg.taxonCounts
  lazy val totalSequences = counts.iterator.map(_._2).sum

  def dataColumnHeaders: String =
    "Perc\tAggregate\tIn taxon"

  def headers: String =
    s"${dataColumnHeaders}\tRank\tTaxon\tName"

  /** Data columns for each line in the report. This method can be overridden to add
   * additional columns. */
  def dataColumns(taxid: Taxon): String = {
    val cladeCount = cladeTotals(taxid) //aggregate
    val taxonCount = taxonCounts(taxid)
    val percent = "%6.2f".format(100.0 * cladeCount / totalSequences)
    s"$percent\t$cladeCount\t$taxonCount"
  }

  def reportLine(taxid: Taxon, rank: Rank, rankDepth: Int, depth: Int): String = {
    val depthString = if (rankDepth == 0) "" else rankDepth.toString
    val indent = "  " * depth
    val name = taxonomy.getName(taxid).getOrElse("")
    s"${dataColumns(taxid)}\t${rank.code}$depthString\t$taxid\t$indent$name"
  }

  /** Depth-first search to generate report lines and print them.
   * Mostly adapted from kraken 2's reports.cc.
   */
  def reportDFS(output: PrintWriter, reportZeros: Boolean, taxid: Taxon, rank: Rank, rankDepth: Int,
                depth: Int): Unit = {
    val (rankCodeNext, rankDepthNext) = taxonomy.getRank(taxid) match {
      case Some(r) => (r, 0)
      case _ => (rank, rankDepth + 1)
    }

    output.println(reportLine(taxid, rankCodeNext, rankDepthNext, depth))

    //sort by descending clade count
    //Because counts have already been aggregated upward, filtering > 0 will not remove any intermediate nodes
    val sortedChildren = taxonomy.children(taxid).toArray.
      map(c => (c, cladeTotals(c))).sortWith((a, b) => a._2 > b._2)
    for {
      (child, count) <- sortedChildren
      if reportZeros || count > 0
    } {
      reportDFS(output, reportZeros, child, rankCodeNext, rankDepthNext, depth + 1)
    }
  }

  def reportDFS(output: PrintWriter, reportZeros: Boolean): Unit = {
    if (!compatibleFormat) {
      output.println(headers)
    }
    val totalUnclassified = taxonCounts(NONE)
    if (totalUnclassified != 0 || reportZeros) {
      output.println(reportLine(NONE, Unclassified, 0, 0))
    }
    reportDFS(output, reportZeros, ROOT, Root, 0, 0)
  }

  def print(output: PrintWriter, reportZeros: Boolean = false): Unit =
    reportDFS(output, reportZeros)
}

class TotalKmerCountReport(taxonomy: Taxonomy, counts: Array[(Taxon, Long)], val genomeSizes: Array[(Taxon, Long)])
  extends KrakenReport(taxonomy, counts) {

  lazy val totMinAgg = new TotalKmerSizeAggregator(taxonomy, genomeSizes)

  override def dataColumnHeaders: String =
    s"${super.dataColumnHeaders}\tTKC1-LeafOnly\tTKC2-FirstChildren\tTKC3-AllChildren"

  override def dataColumns(taxid: Taxon): String = {
    val totMinSizeS1 = math.ceil(totMinAgg.totKmerAverageS1(taxid)).toLong
    val totMinSizeS2 = math.ceil(totMinAgg.totKmerAverageS2(taxid)).toLong
    val totMinSizeS3 = math.ceil(totMinAgg.totKmerAverageS3(taxid)).toLong
    s"${super.dataColumns(taxid)}\t$totMinSizeS1\t$totMinSizeS2\t$totMinSizeS3"

  }
}

object KrakenReport {
  def color(level: Int): String = {
    import Taxonomy._
    level match {
      case Root.depth => Console.BLUE
      case Superkingdom.depth => Console.RED
      case Kingdom.depth => Console.GREEN
      case Phylum.depth => Console.YELLOW
      case _ => Console.WHITE
    }
  }

  def numRankForCode(code: String): Int =
    Taxonomy.rankValues.find(_.code == code).
      getOrElse(Taxonomy.Unclassified).depth

  def main(args: Array[String]): Unit = {
    val level = if (args.length > 0) Some(args(0)) else None
    val minPercent = if (args.length > 1) Some(args(1).toDouble) else None

    val cutoff = level.map(l => numRankForCode(l.toUpperCase))
    val digits = "[0-9]+".r
    for {
      l <- scala.io.Source.stdin.getLines()
      if !l.startsWith("#")
      spl = l.split("\t")

      frac = spl(0).toDouble
      if minPercent.isEmpty || frac >= minPercent.get

      level = digits.replaceAllIn(spl(3), "")
      numLevel = numRankForCode(level)
      if cutoff.isEmpty || numLevel <= cutoff.get
    } {
      val col = color(numLevel)
      println(col + l + Console.RESET)
    }
  }
}
