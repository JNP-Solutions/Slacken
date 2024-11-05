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

package com.jnpersson.slacken.analysis

import com.jnpersson.slacken.Taxonomy
import com.jnpersson.slacken.Taxonomy._


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

  def main(args: Array[String]): Unit = {
    val levelArg = if (args.length > 0) Some(args(0)) else None
    val minPercentArg = if (args.length > 1) Some(args(1).toDouble) else None

    val cutoff = levelArg.map(l => numRankForCode(l.toUpperCase))
    val digits = "[0-9]+".r
    for {
      l <- scala.io.Source.stdin.getLines()
      if !l.startsWith("#")
      spl = l.split("\t")

      frac = spl(0).toDouble
      if minPercentArg.forall(frac >= _)

      level = digits.replaceAllIn(spl(3), "")
      numLevel = numRankForCode(level)
      if cutoff.forall(numLevel <= _)
    } {
      val col = color(numLevel)
      println(col + l + Console.RESET)
    }
  }
}
