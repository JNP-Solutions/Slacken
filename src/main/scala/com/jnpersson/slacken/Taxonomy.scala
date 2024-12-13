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

import com.jnpersson.kmers.HDFSUtil
import com.jnpersson.slacken.Taxonomy.Rank
import org.apache.spark.sql.SparkSession

import scala.annotation.tailrec
import scala.collection.mutable

object Taxonomy {
  final val NONE: Taxon = 0
  final val ROOT: Taxon = 1

  /** Levels in the taxonomic hierarchy, from general (higher) to specific (lower) */
  sealed abstract class Rank(val title: String, val code: String, val depth: Int) extends Serializable with Ordered[Rank] {
    def compare(that: Rank): Int =
      depth - that.depth
  }

  case object Unclassified extends Rank("unclassified", "U", -1)
  case object Root extends Rank("root", "R", 0)
  case object Superkingdom extends Rank("superkingdom", "D", 1)
  case object Kingdom extends Rank("kingdom", "K", 2)
  case object Phylum extends Rank("phylum", "P", 3)
  case object Class extends Rank("class", "C", 4)
  case object Order extends Rank("order", "O", 5)
  case object Family extends Rank("family", "F", 6)
  case object Genus extends Rank("genus", "G", 7)
  case object Species extends Rank("species", "S", 8)

  /** All Rank values except Unclassified. */
  val rankValues: List[Rank] = List(Root, Superkingdom, Kingdom, Phylum, Class, Order, Family, Genus, Species)
  val rankTitles: List[String] = rankValues.map(_.title)

  def rank(title: String): Option[Rank] = title match {
    case Unclassified.title => Some(Unclassified)
    case Root.title => Some(Root)
    case Superkingdom.title => Some(Superkingdom)
    case Kingdom.title => Some(Kingdom)
    case Phylum.title => Some(Phylum)
    case Class.title => Some(Class)
    case Order.title => Some(Order)
    case Family.title => Some(Family)
    case Genus.title => Some(Genus)
    case Species.title => Some(Species)
    case _ => None
  }

  def rankOrNull(title: String): Rank =
    rank(title).orNull

  def rankForDepth(d: Int): Option[Rank] =
    rankValues.find(_.depth == d)


  /**
   * Construct a Taxonomy from parsed NCBI style input data.
   * @param nodes triples of (taxid, parent taxid, rank (long name))
   * @param names tuples of (taxid, scientific name)
   * @param merged tuples of (secondary ID, primary ID)
   */
  def fromNodesAndNames(nodes: Iterable[(Taxon, Taxon, String)], names: Iterator[(Taxon, String)],
                        merged: Iterable[(Taxon, Taxon)]): Taxonomy = {
    val max1 = if (nodes.isEmpty) 0 else nodes.iterator.map(_._1).max + 1
    val max2 = if (merged.isEmpty) 0 else merged.iterator.map(_._1).max + 1
    val numEntries = if (max1 > max2) max1 else max2

    val scientificNames = new Array[String](numEntries)
    for { (taxon, name) <- names } {
      scientificNames(taxon) = name
    }
    scientificNames(NONE) = Unclassified.title

    val parents = Array.fill[Taxon](numEntries)(NONE)
    val ranks = new Array[Rank](numEntries)
    for { (taxon, parent, rankTitle) <- nodes } {
      parents(taxon) = parent
      ranks(taxon) = rank(rankTitle).orNull
    }

    val primary = Array.tabulate[Taxon](numEntries)(i => i)
    for { (secId, primId) <- merged} {
      primary(secId) = primId
    }

    parents(ROOT) = Taxonomy.NONE
    ranks(NONE) = Unclassified
    ranks(ROOT) = Root
    new Taxonomy(parents, ranks, scientificNames, primary)
  }

  /**
   * Read a taxonomy from a directory with NCBI nodes.dmp, merged.dmp, and names.dmp.
   * The files are expected to be small.
   * @return
   */
  def load(dir: String)(implicit spark: SparkSession): Taxonomy = {
    val nodes = HDFSUtil.getSource(s"$dir/nodes.dmp").
      getLines().map(_.split("\\|")).
      map(x => (x(0).trim.toInt, x(1).trim.toInt, x(2).trim))

    val names = HDFSUtil.getSource(s"$dir/names.dmp").
      getLines().map(_.split("\\|")).
      flatMap(x => {
        val nameType = x(3).trim
        if (nameType == "scientific name") {
          Some((x(0).trim.toInt, x(1).trim))
        } else None
      })

    val merged = if (HDFSUtil.fileExists(s"$dir/merged.dmp")) {
      HDFSUtil.getSource(s"$dir/merged.dmp").
        getLines().map(_.split("\\|")).
        map(x => (x(0).trim.toInt, x(1).trim.toInt))
    } else Iterator.empty

    Taxonomy.fromNodesAndNames(nodes.toArray[(Taxon, Taxon, String)], names, merged.toArray[(Taxon, Taxon)])
  }

  /**
   * Copy a taxonomy to a new location, including only the files actually used by Slacken.
   */
  def copyToLocation(fromDir: String, toDir: String)(implicit spark: SparkSession): Unit = {
    HDFSUtil.copyFile(s"$fromDir/nodes.dmp", s"$toDir/nodes.dmp")
    HDFSUtil.copyFile(s"$fromDir/names.dmp", s"$toDir/names.dmp")
    HDFSUtil.copyFile(s"$fromDir/merged.dmp", s"$toDir/merged.dmp")
  }
}

/**
 * Maps each taxon to its parent, rank and name.
 * The parent relationships should describe a tree structure (DAG).
 * Taxa are integers in the range 0..size. Some may be unused. Id 1 is always ROOT.
 * Id 0 is NONE. Except for ROOT, only unused taxa have NONE as a parent.
 * @param parents Lookup array mapping taxon ID (offset) to parent taxon ID
 * @param ranks Lookup array mapping taxon ID to rank. May be null.
 * @param scientificNames Lookup array mapping taxon ID to scientific names. May be null.
 * @param primary Lookup array mapping taxon ID (offset) to standard taxon ID. Reflects NCBI merged.dmp .
 */
final case class Taxonomy(parents: Array[Taxon], ranks: Array[Rank], scientificNames: Array[String],
                          primary: Array[Taxon]) {
  import Taxonomy._

  //Size of the range of this taxonomy, including unused taxa.
  //Size - 1 is the maximal taxon potentially used.
  def size: Taxon = parents.length

  /** All defined taxa, in the range [1, size - 1] */
  def taxa: Iterator[Taxon] =
    Iterator.range(1, size).filter(isDefined)

  def isLeafNode(taxon: Taxon): Boolean =
    children(taxon).isEmpty

  /** Whether the given taxon is defined in this taxonomy or not */
  def isDefined(taxon: Taxon): Boolean =
    parents(taxon) != NONE || taxon == ROOT

  /** Get the rank of a (potentially undefined) taxon as an Option */
  def getRank(taxon: Taxon): Option[Rank] =
    Option(ranks(taxon))

  /** Get the name of a (potentially undefined) taxon as an Option */
  def getName(taxon: Taxon): Option[String] =
    Option(scientificNames(taxon))

  override def toString: String = {
    val taxaString = taxa.take(20).map(t => s"($t, ${ranks(t)}, ${parents(t)})").mkString(", ")
    s"Taxonomy($taxaString ... (${taxa.size}))"
  }

  /** Lookup array mapping taxon ID to taxon IDs of children */
  @transient
  lazy val children: Array[List[Taxon]] = {
    val children: Array[List[Taxon]] = parents.map(_ => Nil)
    for { (parent, taxid) <- parents.zipWithIndex
          if isDefined(taxid) } {
      children(parent) = taxid :: children(parent)
    }

    children
  }

  /** Iterate all steps to ROOT from a starting taxon, including the taxon itself. */
  def pathToRoot(from: Taxon): Iterator[Taxon] =
    new Iterator[Taxon] {
      private var t = from
      override def hasNext: Boolean =
        t != NONE

      override def next(): Taxon = {
        val r = t
        t = parents(t)
        r
      }
    }

  /**
   * Numerical depth of a taxon. Standardised to correspond to ranks, so that 0 = root,
   * 1 = superkingdom etc. This need not correspond to the actual depth of the tree structure.
   */
  @tailrec
  def depth(tax: Taxon): Int = {
    if (tax == NONE) -1
    else Option(ranks(tax)) match {
      case Some(r) => r.depth
      case None => depth(parents(tax))
    }
  }

  /** Find whether the given taxon has the given ancestor (possibly with several steps)
   * Will also return true if tax == parent.
   * @param tax potential descendant node
   * @param ancestor potential ancestor node
   * @return true iff the ancestor relationship exists
   */
  def hasAncestor(tax: Taxon, ancestor: Taxon): Boolean =
    stepsToAncestor(tax, ancestor) != -1

  /** Number of levels between a taxon and a given ancestor. The result will always be -1
   * if the given ancestor is not in the lineage of the taxon. */
  def stepsToAncestor(tax: Taxon, ancestor: Taxon): Int = {
    val path = pathToRoot(tax)
    if (path.isEmpty) -1 else path.indexOf(ancestor)
  }

  /** Number of standardised levels between a taxon and a given ancestor of it. The result
   * will always be -1 if the given ancestor is not in the lineage of the taxon. */
  def standardStepsToAncestor(tax: Taxon, ancestor: Taxon): Int = {
    if (hasAncestor(tax, ancestor)) {
      val d1 = depth(tax)
      val d2 = depth(ancestor)
      d1 - d2
    } else -1
  }

  /** Count the sublevel that the given taxon is on. E.g. level S = 0,
   * level S1 = 1, S2 = 2 etc.
   */
  def sublevel(tax: Taxon): Int = {
    val d = depth(tax)
    pathToRoot(tax).takeWhile(t => depth(t) == d).size - 1
  }

  /** Find the ancestor of the query at the given level, if it exists. Searches upward.
   * If there are sub-levels such as S2, S1 etc, the first hit in the path to root will be returned.
   * @param query taxon to search from
   * @param rank rank to find ancestor at
   * @return ancestor at the given level, if it exists
   */
  def ancestorAtLevel(query: Taxon, rank: Rank): Option[Taxon] =
    pathToRoot(query).find(t => depth(t) == rank.depth)

  /** Get the standardised ancestor at level (e.g. S instead of S1 or S2)
   * This is the last hit in the path to root that satisfies the criteria.
   */
  def standardAncestorAtLevel(query: Taxon, rank: Rank): Option[Taxon] = {
    val below = pathToRoot(query).takeWhile(t => depth(t) >= rank.depth)
    below.toSeq.lastOption
  }

  /** Find the ancestor of the query at the given level, if it exists. Searches upward.
   * If it doesn't exist at the specified rank, then None will be returned.
   * @param query taxon to search from
   * @param rank rank to find ancestor at
   * @return ancestor at the given level, or None if none was found
   */
  def ancestorAtLevelStrict(query: Taxon, rank: Rank): Option[Taxon] =
    pathToRoot(query).find(t => depth(t) == rank.depth)

  /** By traversing the tree upward from a given starting set of leaf taxa, count the total number of distinct taxa
   * present in the entire tree.
   * @param taxa leaf taxa to start from
   * @return number of distinct taxa in the tree
   */
  def countDistinctTaxaWithAncestors(taxa: Iterable[Taxon]): Int =
    taxaWithAncestors(taxa).size

  /** For a given taxon, find which of the standard 8 levels are missing in its path to the root.
   */
  def missingStepsToRoot(taxon: Taxon): List[Int] = {
    val found = pathToRoot(taxon).toList.map(t => depth(t))
    (Superkingdom.depth to Species.depth).toList.
      filter(level => !found.contains(level))
  }

  /** Complete a taxonomic tree upwards to ROOT by including all ancestors */
  def taxaWithAncestors(taxa: Iterable[Taxon]): mutable.BitSet =
    taxa.foldLeft(mutable.BitSet.empty)((set, a) => {
      set ++= pathToRoot(a).takeWhile(e => ! set.contains(e))
    })

  /** Complete a taxonomic tree downward (entire clades) starting from the given set,
   * including all descendants */
  def taxaWithDescendants(taxa: Iterable[Taxon]): mutable.BitSet =
    taxa.foldLeft(mutable.BitSet.empty ++ taxa)(addDescendants)

  def addDescendants(to: mutable.BitSet, from: Taxon): mutable.BitSet = {
    to ++= children(from)
    children(from).foldLeft(to)(addDescendants)
  }

  @tailrec
  def debugTracePath(x: Taxon): Unit = {
    println(s"$x\t${ranks(x)}\t${scientificNames(x)}")
    if (x != ROOT && x != NONE) {
      debugTracePath(parents(x))
    }
  }
}
