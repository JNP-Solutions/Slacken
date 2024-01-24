/*
 * This file is part of Hypercut. Copyright (c) 2023 Johan Nyström-Persson.
 */


package com.jnpersson.slacken

import com.jnpersson.slacken.Taxonomy.{NONE, ROOT, Rank, Root, Unclassified}

import java.io.PrintWriter
import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.{Map => MMap}

/** A Kraken 1/2 style taxonomic report with a tree structure.
 * @param taxonomy The taxonomy
 * @param counts Number of hits (reads) for each taxon
 */
final class KrakenReport(taxonomy: Taxonomy, counts: Array[(Taxon, Long)]) {
  lazy val cladeCounts = aggregateCounts()
  lazy val countMap = MMap.empty ++ counts
  lazy val totalSequences = counts.iterator.map(_._2).sum

  @tailrec
  def addParents(to: MMap[Taxon, Long], from: Taxon, count: Long): Unit = {
    val parent = taxonomy.parents(from)
    if (parent != NONE) {
      to += (parent -> (to.getOrElse(parent, 0L) + count))
      addParents(to, parent, count)
    }
  }

  /** Build a lookup map for taxon hit counts. Includes ancestors to build recursive aggregate counts. */
  def aggregateCounts(): MMap[Taxon, Long] = {
    val r = MMap[Taxon, Long]()
    for {
      (taxid, count) <- counts
    } {
      r += (taxid -> (r.getOrElse(taxid, 0L) + count))
      addParents(r, taxid, count)
    }
    r
  }

  def reportLine(taxid: Taxon, rank: Rank, rankDepth: Int, depth: Int): String = {
    val r = new mutable.StringBuilder
    val cladeCount = cladeCounts.getOrElse(taxid, 0L)
    r append "%6.2f\t".format(100.0 * cladeCount / totalSequences)
    r append cladeCount
    r append "\t"
    r append countMap.getOrElse(taxid, 0L)
    if (rankDepth == 0) {
      r append s"\t${rank.code}\t$taxid\t"
    } else {
      r append s"\t${rank.code}$rankDepth\t$taxid\t"
    }
    r append ("  " * depth)
    r.append(taxonomy.getName(taxid).getOrElse(""))
    r.result()
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
