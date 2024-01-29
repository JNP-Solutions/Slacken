/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */


package com.jnpersson.slacken

import com.jnpersson.slacken.Taxonomy.Rank

import scala.annotation.tailrec
import scala.collection.mutable

object Taxonomy {
  val NONE: Taxon = 0
  val ROOT: Taxon = 1

  /** Levels in the taxonomic hierarchy, from general (higher) to specific (lower) */
  sealed abstract class Rank(val title: String, val code: String) extends Serializable
  case object Unclassified extends Rank("unclassified", "U")
  case object Root extends Rank("root", "R")
  case object Superkingdom extends Rank("superkingdom", "D")
  case object Kingdom extends Rank("kingdom", "K")
  case object Phylum extends Rank("phylum", "P")
  case object Class extends Rank("class", "C")
  case object Order extends Rank("order", "O")
  case object Family extends Rank("family", "F")
  case object Genus extends Rank("genus", "G")
  case object Species extends Rank("species", "S")

  /** All Rank values except Unclassified. */
  val ranks = List(Root, Superkingdom, Kingdom, Phylum, Class, Order, Family, Genus, Species)

  //See kraken2 reports.cc
  def rank(title: String): Option[Rank] = title match {
    case "unclassified" => Some(Unclassified)
    case "root" => Some(Root)
    case "superkingdom" => Some(Superkingdom)
    case "kingdom" => Some(Kingdom)
    case "phylum" => Some(Phylum)
    case "class" => Some(Class)
    case "order" => Some(Order)
    case "family" => Some(Family)
    case "genus" => Some(Genus)
    case "species" => Some(Species)
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

    val parents = new Array[Taxon](numEntries)
    val taxonRanks = new Array[Rank](numEntries)
    for { (taxon, parent, rankTitle) <- nodes } {
      parents(taxon) = parent
      taxonRanks(taxon) = rank(rankTitle).orNull
    }

    parents(ROOT) = Taxonomy.NONE
    taxonRanks(NONE) = Unclassified
    taxonRanks(ROOT) = Root
    scientificNames(NONE) = Unclassified.title

    new Taxonomy(parents, taxonRanks, scientificNames)
  }
}

/**
 * Maps each taxon to its parent.
 * @param parents Lookup array mapping taxon ID (offset) to parent taxon ID
 * @param taxonRanks Lookup array mapping taxon ID to rank. May be null.
 * @param scientificNames Lookup array mapping taxon ID to scientific names. May be null.
 */
final case class Taxonomy(parents: Array[Taxon], taxonRanks: Array[Rank], scientificNames: Array[String]) {
  import Taxonomy._

  def getRank(taxon: Taxon): Option[Rank] = Option(taxonRanks(taxon))
  def getName(taxon: Taxon): Option[String] = Option(scientificNames(taxon))

  /** Lookup array mapping taxon ID to taxon IDs of children */
  lazy val children: Array[List[Taxon]] = {
    val children: Array[List[Taxon]] = parents.map(i => Nil)
    for { (parent, taxid) <- parents.zipWithIndex } {
      children(parent) = taxid :: children(parent)
    }

    for { i <- children.indices } {
      children(i) = children(i).sorted
    }

    children
  }

  /**
   * Depth of a taxon (distance from the root of the tree)
   * @param tax taxon to look up
   * @param acc accumulator
   * @return number of levels from the root
   */
  @tailrec
  def depth(tax: Taxon, acc: Int = 0): Int = {
    if (tax == NONE) {
      acc
    } else {
      depth(parents(tax),  acc + 1)
    }
  }

  /** Find whether the given taxon has the given ancestor (possibly with several steps)
   * @param tax potential descendant node
   * @param parent potential ancestor node
   * @return true iff the ancestor relationship exists
   */
  def hasAncestor(tax: Taxon, parent: Taxon): Boolean =
    stepsToParent(tax, parent) != 0

  /** Number of levels between a taxon and a given parent. The result will always be zero
   * if the given parent is not in the lineage of the taxon. */
  @tailrec
  def stepsToParent(tax: Taxon, parent: Taxon, acc: Int = 0): Int = {
    if (tax == NONE) {
      0
    } else if (parent == tax) {
      acc
    } else {
      stepsToParent(parents(tax), parent, acc + 1)
    }
  }

  /** Find the ancestor of the query at the given level, if it exists. Searches upward.
   * If it doesn't exist (for example because the level is too low), then ROOT will be returned.
   * @param query taxon to search from
   * @param rank rank to find ancestor at
   * @return ancestor at the given level, or ROOT if none was found
   */
  def ancestorAtLevel(query: Taxon, rank: Rank): Taxon =
    ancestorAtLevel(query, query, rank)

  @tailrec
  private def ancestorAtLevel(query: Taxon, at: Taxon, level: Rank): Taxon = {
    if (taxonRanks(at) == level) {
      at
    } else {
      val p = parents(at)
      if (p == NONE | p == ROOT) {
//        println(s"Warning: no ancestor at level $level for taxon $query. Assigning ROOT=$ROOT")
        ROOT
      } else {
        ancestorAtLevel(query, p, level)
      }
    }
  }

  /**
   * Lowest common ancestor of two taxa. From taxonomy.cc in kraken2.
   * Logic here depends on higher nodes having smaller IDs
   * Idea: track two nodes, advance lower tracker up tree, trackers meet @ LCA
   * @param tax1 taxon 1
   * @param tax2 taxon 2
   * @return lca(tax1, tax2)
   */
  def lca(tax1: Taxon, tax2: Taxon): Taxon = {
    if (tax1 == NONE || tax2 == NONE) {
      return if (tax2 == NONE) tax1 else tax2
    }

    var a = tax1
    var b = tax2
    while (a != b) {
      if (a > b) a = parents(a)
      else b = parents(b)
    }
    a
  }

  /**
   * Take all hit taxa plus ancestors, then return the leaf of the highest weighted leaf-to-root path.
   * If the highest weighted path has a score below the confidence threshold, the taxon may move up in the tree to
   * increase the score of that clade.
   *
   * Based on the algorithm in Kraken 2 classify.cc.
   * @param hitSummary taxon hit counts
   * @param confidenceThreshold the minimum fraction of minimizers that must be included below the clade of the
   *                            matching taxon (if not, the taxon will move up in the tree)
   */
  def resolveTree(hitSummary: TaxonCounts, confidenceThreshold: Double): Taxon = {
    var maxTaxon = 0
    var maxScore = 0

    //the number of times each taxon was seen in a read, excluding ambiguous
    val hitCounts = hitSummary.toMap
    val requiredScore = Math.ceil(confidenceThreshold * hitSummary.totalTaxa)

    for { (taxon, _) <- hitCounts } {
      var node = taxon
      var score = 0
      //Accumulate score across this path to the root
      while (node != NONE) {
        score += hitCounts.getOrElse(node, 0)
        node = parents(node)
      }

      if (score > maxScore) {
        maxTaxon = taxon
        maxScore = score
      } else if (score == maxScore) {
        maxTaxon = lca(maxTaxon, taxon)
      }
    }

    //Gradually lift maxTaxon to try to achieve the required score
    maxScore = hitCounts.getOrElse(maxTaxon, 0)
    while (maxTaxon != NONE && maxScore < requiredScore) {
      maxScore = 0
      for {
        (taxon, score) <- hitCounts
        if hasAncestor(taxon, maxTaxon)
      } {
        //Add score if taxon is in max_taxon's clade
        maxScore += score
      }
      if (maxScore >= requiredScore) {
        return maxTaxon
      }
      //Move up the tree, yielding a higher total score.
      //Run off the tree (NONE) if the required score is never met
      maxTaxon = parents(maxTaxon)
    }
    maxTaxon
  }

  /** By traversing the tree upward from a given starting set of leaf taxa, count the total number of distinct taxa
   * present in the entire tree.
   * @param taxa leaf taxa to start from
   * @return number of distinct taxa in the tree
   */
  def countDistinctTaxaWithParents(taxa: Iterable[Taxon]): Int = {
    val r = mutable.BitSet.empty
    for { a <- taxa} {
      r += a
      var p = a
      while (p != ROOT && p != NONE) {
        p = parents(p)
        r += p
      }
    }
    r += ROOT
    r.size
  }
}
