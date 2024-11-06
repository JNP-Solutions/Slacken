/*
 *
 *  * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 *  *
 *  * Slacken is free software: you can redistribute it and/or modify
 *  * it under the terms of the GNU General Public License as published by
 *  * the Free Software Foundation, either version 3 of the License, or
 *  * (at your option) any later version.
 *  *
 *  * Slacken is distributed in the hope that it will be useful,
 *  * but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  * GNU General Public License for more details.
 *  *
 *  * You should have received a copy of the GNU General Public License
 *  * along with Slacken.  If not, see <https://www.gnu.org/licenses/>.
 *
 */


package com.jnpersson.slacken

import com.jnpersson.slacken.Taxonomy.{NONE, ROOT, Rank, Root, Unclassified}

import java.io.PrintWriter
import scala.collection.mutable.{Map => MMap}

/** Helper for aggregating per-taxon counts in the taxonomic tree */
class TreeAggregator(taxonomy: Taxonomy, counts: Array[(Taxon, Long)]) {
  def keys: Iterable[Taxon] = taxonCounts.keys

  val taxonCounts: MMap[Taxon, Long] =
    (MMap.empty ++ counts).withDefaultValue(0L)

  val cladeTotals: MMap[Taxon, Long] =
    MMap.empty.withDefaultValue(0L)

  for {
    (taxid, count) <- counts } {
    for {p <- taxonomy.pathToRoot(taxid)} cladeTotals(p) += count
    if (taxid == NONE) cladeTotals(taxid) = count //pathToRoot doesn't include NONE
  }
}

/** A Kraken 1/2 style taxonomic report with a tree structure.
 *
 * @param taxonomy         The taxonomy
 * @param counts           Number of hits (reads) for each taxon
 * @param compatibleFormat Traditional output format (no headers, identical to Kraken/Kraken 2 reports)
 */
class KrakenReport(taxonomy: Taxonomy, counts: Array[(Taxon, Long)], compatibleFormat: Boolean = false) {
  val agg = new TreeAggregator(taxonomy, counts)
  private val cladeTotals = agg.cladeTotals
  private val taxonCounts = agg.taxonCounts
  private val totalSequences = counts.iterator.map(_._2).sum

  def dataColumnHeaders: String =
    "#Perc\tAggregate\tIn taxon"

  def headers: String =
    s"$dataColumnHeaders\tRank\tTaxon\tName"

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
