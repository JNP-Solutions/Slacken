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
