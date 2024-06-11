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

class TreeAggregatorAvgGenomeSize(taxonomy: Taxonomy, genomeSizes: Array[(Taxon, Long)]) {
  val genomeSizesMap=genomeSizes.toMap
  val computedTreeMap: mutable.Map[Taxon, (Long, Long)] = computeFullTree()
  def genomeAverageS1(taxon: Taxon):Double ={
    val s1Agg = taxonomy.children(taxon).map(child => computedTreeMap(child))
      .foldLeft((0L,0L)){(aggSum, pair) => (aggSum._1+pair._1,aggSum._2+pair._2)}
    s1Agg._1.toDouble/s1Agg._2.toDouble
    }
  def genomeAverageS2(taxon: Taxon):Double ={
    val s2Agg = taxonomy.children(taxon).map(child => computedTreeMap(child)).filter(_._2>0)
      .map(pair => pair._1.toDouble/pair._2.toDouble)
    s2Agg.sum/s2Agg.size.toDouble
  }
  def genomeAverageS3(taxon: Taxon):Double ={
    val childrenNonZero = taxonomy.children(taxon).map(child => computedTreeMap(child)).filter(_._2>0)
    val s1Agg = childrenNonZero.foldLeft((0L,0L)){(aggSum, pair) => (aggSum._1+pair._1,aggSum._2+pair._2)}
    val nonZeroChildSize = childrenNonZero.size.toDouble

    ((genomeAverageS1(taxon)*s1Agg._2.toDouble)+
      (genomeAverageS2(taxon)*nonZeroChildSize))/(s1Agg._2.toDouble+nonZeroChildSize)
  }
  def computeFullTree():mutable.Map[Taxon,(Long,Long)]={
    val results = mutable.Map[Taxon,(Long,Long)]()
    computeLeafAggAndCounts(ROOT,results)
    results
  }

  def computeLeafAggAndCounts(taxon: Taxon, results: mutable.Map[Taxon,(Long,Long)]):(Long, Long)={
    val children = taxonomy.children(taxon)
    children match {
      case Nil =>
        if(genomeSizesMap.contains(taxon)) {
          results += (taxon -> (genomeSizesMap(taxon),1))
          (genomeSizesMap(taxon),1)
        }
        else {
          results += (taxon -> (0,0))
          (0,0)
        }

      case childList =>
        val aggCounts = (ListBuffer.empty[Long],ListBuffer.empty[Long])
        for{i <- childList
            leafCounts=computeLeafAggAndCounts(i,results)}
        {
          aggCounts._1 += leafCounts._1
          aggCounts._2 += leafCounts._2
        }
        results += (taxon -> (aggCounts._1.sum,aggCounts._2.sum))
        (aggCounts._1.sum,aggCounts._2.sum)
    }
  }

}

/** A Kraken 1/2 style taxonomic report with a tree structure.
 * @param taxonomy The taxonomy
 * @param counts Number of hits (reads) for each taxon
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
   *  Mostly adapted from kraken 2's reports.cc.
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

class GenomeLengthReport(taxonomy: Taxonomy, counts: Array[(Taxon, Long)], genomeSizes: Array[(Taxon, Long)])
  extends KrakenReport(taxonomy, counts){

  lazy val genomeAgg = new TreeAggregatorAvgGenomeSize(taxonomy, genomeSizes)

  override def dataColumnHeaders: String =
    s"${super.dataColumnHeaders}\tGS1-LeafOnly\tGS2-FirstChildren\tGS3-AllNodes"

  override def dataColumns(taxid: Taxon): String = {
    val genomeSizeS1 = genomeAgg.genomeAverageS1(taxid)
    val genomeSizeS2 = genomeAgg.genomeAverageS2(taxid)
    val genomeSizeS3 = genomeAgg.genomeAverageS3(taxid)
    s"${super.dataColumns(taxid)}\t$genomeSizeS1\t$genomeSizeS2\t$genomeSizeS3"

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
    val minPercent = if(args.length > 1) Some(args(1).toDouble) else None

    val cutoff = level.map(l => numRankForCode(l.toUpperCase))
    val digits = "[0-9]+".r
    for {
      l <- scala.io.Source.stdin.getLines()
      if ! l.startsWith("#")
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
