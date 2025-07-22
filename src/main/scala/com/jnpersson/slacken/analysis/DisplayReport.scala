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

import com.jnpersson.slacken.Taxonomy
import com.jnpersson.slacken.Taxonomy._
import com.jnpersson.slacken.analysis.DisplayReport.{filterReport}

import scala.collection.BitSet


/** Helper tool for displaying a Kraken/Slacken report, filtering and colorizing the output */
object DisplayReport {
  def color(level: Int): String = {
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


  /** Filter report lines from stdin.
   *
   * @param levelCutoff lowest standardised level to display (see [[Rank]]), if any
   * @param minPercent minimum percentage to keep, if any
   * @param clades if given, include only taxa in this set and their descendants
   *               (based on indentation level of the filtered lines)
   */
  def filterReport(levelCutoff: Option[Int], minPercent: Option[Double], clades: Option[BitSet]): Unit = {
    val digits = "[0-9]+".r
    val whitespace = "\\s+".r
    var lastIndent = Integer.MAX_VALUE
    for {
      l <- scala.io.Source.stdin.getLines()
      if !l.startsWith("#")
      spl = l.split("\t")

      frac = spl(0).toDouble
      if minPercent.forall(frac >= _)

      level = digits.replaceAllIn(spl(3), "")
      numLevel = numRankForCode(level)
      if levelCutoff.forall(numLevel <= _)

      taxon = spl(4).toInt
      indent = whitespace.findFirstIn(spl(5)).getOrElse("").length
    } {
      if (clades.forall(_.contains(taxon)) &&
        lastIndent > indent) {
          //accept this taxon and the tree below it
          lastIndent = indent
          //do not change the indent level if not needed, as we might already be accepting an outer taxon
        }
      //Print line if either: clade set is empty (no filter), or clade set contains the taxon,
      //or we are indented (below a taxon we already accepted)
      if (clades.isEmpty || (clades.forall(_.contains(taxon)) || indent > lastIndent)) {
        println(l)
      } else {
        lastIndent = Integer.MAX_VALUE //indicates we are not accepting the current subtree
      }
    }
  }

  /**
   * Filter, color code and display a Kraken/Slacken report.
   * Arguments: 1. letter code for taxonomic rank cutoff, e.g. G for genus. See [[Rank]].
   * 2. Minimum percentage for cutoff, e.g. 1. If given, the rank must also be given.
   */
  def main(args: Array[String]): Unit = {
    val levelArg = if (args.length > 0) Some(args(0)) else None
    val minPercentArg = if (args.length > 1) Some(args(1).toDouble) else None

    val cutoff = levelArg.map(l => numRankForCode(l.toUpperCase))
    filterReport(cutoff, minPercentArg, None)
  }
}

/** Filter a Kraken/Slacken report by ancestor set and optionally by fraction.
 * Arguments: 1. ancestor set file (one taxon per line), 2. optionally a minimum percentage for cutoff.
 * The output will be color coded.
 */
object FilterReport {

  def main(args: Array[String]): Unit = {
    val ancestorSetFile = args(0)
    val minPercentArg = if (args.length > 1) Some(args(1).toDouble) else None
    val ancestorSet = BitSet() ++ scala.io.Source.fromFile(ancestorSetFile).getLines().
      map(_.toInt)

    filterReport(None, minPercentArg, Some(ancestorSet))
  }
}