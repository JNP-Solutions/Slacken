/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */


package com.jnpersson.slacken

import com.jnpersson.slacken.Taxonomy.{NONE, ROOT, Rank, Root, Unclassified}

import java.io.PrintWriter
import scala.annotation.tailrec
import scala.collection.mutable.{Map => MMap}

/** A Kraken 1/2 style taxonomic report with a tree structure.
 * @param taxonomy The taxonomy
 * @param counts Number of hits (reads) for each taxon
 */
final class KrakenReport(taxonomy: Taxonomy, counts: Array[(Taxon, Long)]) {
  lazy val cladeCounts = aggregateCounts()
  lazy val countMap = MMap.empty ++ counts
  lazy val totalSequences = counts.iterator.map(_._2).sum


  /** Build a lookup map for aggregate taxon hit counts.
   * Adds counts recursively to ancestors, propagating up the tree. */
  def aggregateCounts(): MMap[Taxon, Long] = {
    val r = MMap[Taxon, Long]()
    for {
      (taxid, count) <- counts
    } {
      for { p <- taxonomy.pathToRoot(taxid) }
        r += (p -> (r.getOrElse(p, 0L) + count))
    }
    r
  }

  def reportLine(taxid: Taxon, rank: Rank, rankDepth: Int, depth: Int): String = {
    val cladeCount = cladeCounts.getOrElse(taxid, 0L) //aggregate
    val taxonCount = countMap.getOrElse(taxid, 0L)
    val percent = "%6.2f".format(100.0 * cladeCount / totalSequences)
    val depthString = if (rankDepth == 0) "" else rankDepth.toString
    val indent = "  " * depth
    val name = taxonomy.getName(taxid).getOrElse("")
    s"$percent\t$cladeCount\t$taxonCount\t${rank.code}$depthString\t$taxid\t$indent$name"
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
      map(c => (c, cladeCounts.getOrElse(c, 0L))).sortWith((a, b) => a._2 > b._2)
    for {
      (child, count) <- sortedChildren
      if reportZeros || count > 0
    } {
      reportDFS(output, reportZeros, child, rankCodeNext, rankDepthNext, depth + 1)
    }
  }

  def reportDFS(output: PrintWriter, reportZeros: Boolean): Unit = {
    val totalUnclassified = countMap.getOrElse(NONE, 0)
    if (totalUnclassified != 0 || reportZeros) {
      output.println(reportLine(NONE, Unclassified, 0, 0))
    }
    reportDFS(output, reportZeros, ROOT, Root, 0, 0)
  }

  def print(output: PrintWriter, reportZeros: Boolean = false): Unit =
    reportDFS(output, reportZeros)
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
