/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */


package com.jnpersson.slacken

import com.jnpersson.slacken.Taxonomy.{NONE, ROOT, Rank, Root, Unclassified}

import java.io.PrintWriter
import scala.collection.mutable
import scala.collection.mutable.{Map => MMap}

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

class TreeAggregatorBreadthFirstS2(taxonomy: Taxonomy, counts: Array[(Taxon, Long)]) {
  val taxonDepthMap: Map[Int, Array[(Taxon, Long)]] = counts.groupBy { count => taxonomy.depth(count._1) }
  val allTaxa: Map[Int, mutable.BitSet] = taxonomy.taxaWithAncestors(Iterable(counts.map(x => x._1): _*)).groupBy(taxonomy.depth)
  val genomeSizeTotalsS2 = MMap[Taxon, Double]().withDefaultValue(0.0)
  for {i <- taxonDepthMap.keys.max to taxonDepthMap.keys.min by -1
       taxonAtLevel <- allTaxa(i)
    taxonDepthAtLevel = taxonDepthMap(i).toMap
       } {
    val children = taxonomy.taxaWithDescendants(Some(taxonAtLevel))
    val totalLength = children.foldLeft(0L) { (totalLength, child) =>
      totalLength + taxonDepthAtLevel.getOrElse(child, 0L)
    }
    val childrenSize = if(children.size > 1) children.size else 1
    genomeSizeTotalsS2(taxonAtLevel) = totalLength.toDouble/(childrenSize) // because we get taxon as well.
  }
}

class TreeAggregatorBreadthFirstS3(taxonomy: Taxonomy, counts: Array[(Taxon, Long)]) {
  val taxonDepthMap: Map[Int, Array[(Taxon, Long)]] = counts.groupBy { count => taxonomy.depth(count._1) }
  val allTaxa: Map[Int, mutable.BitSet] = taxonomy.taxaWithAncestors(Iterable(counts.map(x => x._1): _*)).groupBy(taxonomy.depth)
  val genomeSizeTotalsS3 = MMap[Taxon, Double]().withDefaultValue(0.0)
  for {i <- taxonDepthMap.keys.max to taxonDepthMap.keys.min by -1
       taxonAtLevel <- allTaxa(i)
       taxonDepthAtLevel = taxonDepthMap(i).toMap
       } {
    val children = if(i==taxonDepthMap.keys.max) taxonomy.taxaWithDescendants(Some(taxonAtLevel)) else
      taxonomy.taxaWithDescendants(Some(taxonAtLevel)).groupBy(taxonomy.depth)(i-1)
    val totalLength: Long = children.foldLeft(0L) { (totalLength, child) =>
      totalLength + taxonDepthAtLevel.getOrElse(child, 0L)
    }
    val childrenSize = if(children.size > 1) children.size else 1
    genomeSizeTotalsS3(taxonAtLevel) = totalLength.toDouble/childrenSize // because we get taxon as well.
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


  lazy val genomeAggS2 = new TreeAggregatorBreadthFirstS2(taxonomy, genomeSizes)
  lazy val genomeAggS3 = new TreeAggregatorBreadthFirstS3(taxonomy,genomeSizes)
  // for taxon in genomeAgg, legitimate_children= filter(taxon.children)
  // genomeAgg(taxon)=genomeAgg(taxon)/len(legitimate_children)

  lazy val genomeSizeS1Agg: Map[Taxon,Double] = taxonomy.taxa.map { tax =>
    val children: mutable.BitSet = taxonomy.taxaWithDescendants(Some(tax))
    val genomeSizesMap = genomeSizes.toMap
    val totalLength: Double = children.foldLeft(0L) { (totalLength, child) =>
      totalLength + genomeSizesMap.getOrElse(child, 0L)
    }
    val leafChildSize: Long = children.count(child => genomeSizesMap.contains(child))
    val averageLength: Double = if(leafChildSize > 0) totalLength.toDouble/leafChildSize else 0.0
    (tax,averageLength)
    }.toMap

  override def dataColumnHeaders: String =
//    s"${super.dataColumnHeaders}\tGS1-LeafOnly\tGS2-AllChildren\tGS3-FirstChildren"
    s"${super.dataColumnHeaders}\tGS1-LeafOnly\tGS2-AllChildren\tGS3-FirstChildren"

  override def dataColumns(taxid: Taxon): String = {
    //val genomeSize = genomeAgg.cladeTotals(taxid)
    val genomeSizeS1 = genomeSizeS1Agg(taxid)
    val genomeSizeS2 = genomeAggS2.genomeSizeTotalsS2(taxid)
    val genomeSizeS3 = genomeAggS3.genomeSizeTotalsS3(taxid)
//    s"${super.dataColumns(taxid)}\t$genomeSizeS1$genomeSizeS2$genomeSizeS3"
    s"${super.dataColumns(taxid)}\t$genomeSizeS1\t$genomeSizeS2$genomeSizeS3"

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
