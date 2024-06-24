/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */


package com.jnpersson.slacken

import com.jnpersson.slacken.Taxonomy.Rank

import scala.annotation.tailrec
import scala.collection.mutable

object Taxonomy {
  final val NONE: Taxon = 0
  final val ROOT: Taxon = 1

  /** Levels in the taxonomic hierarchy, from general (higher) to specific (lower) */
  sealed abstract class Rank(val title: String, val code: String, val depth: Int) extends Serializable {
    def isBelow(other: Rank): Boolean =
      depth > other.depth
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

  /**
   * Construct a Taxonomy from parsed NCBI style input data.
   * @param nodes triples of (taxid, parent taxid, rank (long name))
   * @param names tuples of (taxid, scientific name)
   */
  def fromNodesAndNames(nodes: Array[(Taxon, Taxon, String)], names: Iterator[(Taxon, String)]): Taxonomy = {
    val numEntries = nodes.iterator.map(_._1).max + 1
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

    parents(ROOT) = Taxonomy.NONE
    ranks(NONE) = Unclassified
    ranks(ROOT) = Root
    new Taxonomy(parents, ranks, scientificNames)
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
 */
final case class Taxonomy(parents: Array[Taxon], ranks: Array[Rank], scientificNames: Array[String]) {
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

  /** Find the ancestor of the query at the given level, if it exists. Searches upward.
   * If it doesn't exist, then ROOT will be returned.
   * If the level is too low, then the value itself will be returned.
   * @param query taxon to search from
   * @param rank rank to find ancestor at
   * @return ancestor at the given level, or ROOT if none was found
   */
  def ancestorAtLevel(query: Taxon, rank: Rank): Taxon =
    pathToRoot(query).find(t => depth(t) <= rank.depth).getOrElse(ROOT)

  /** Convenience function that optionally returns the query itself if no ancestor level is specified */
  def ancestorAtLevel(query: Taxon, rank: Option[Rank]): Taxon =
    rank match {
      case Some(r) => ancestorAtLevel(query, r)
      case None => query
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
