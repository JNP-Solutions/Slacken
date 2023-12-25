/*
 * This file is part of Discount. Copyright (c) 2019-2023 Johan Nyström-Persson.
 *
 * Discount is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Discount is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Discount.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.jnpersson.slacken

import scala.annotation.tailrec
import scala.collection.mutable

object Taxonomy {
  val NONE = 0
  val ROOT = 1

  //See kraken2 reports.cc
  def rankCode(rank: String): Option[String] = rank match {
    case "superkingdom" => Some("D")
    case "kingdom" => Some("K")
    case "phylum" => Some("P")
    case "class" => Some("C")
    case "order" => Some("O")
    case "family" => Some("F")
    case "genus" => Some("G")
    case "species" => Some("S")
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
    val ranks = new Array[String](numEntries)
    for { (taxon, parent, rank) <- nodes } {
      parents(taxon) = parent
      ranks(taxon) = rankCode(rank).orNull
    }

    parents(ROOT) = Taxonomy.NONE
    ranks(NONE) = "U"
    ranks(ROOT) = "R"
    scientificNames(NONE) = "unclassified"

    new Taxonomy(parents, ranks, scientificNames)
  }
}

/**
 * Maps each taxon to its parent.
 * @param parents Lookup array mapping taxon ID (offset) to parent taxon ID
 * @param taxonRanks Lookup array mapping taxon ID to rank code. May be null.
 * @param scientificNames Lookup array mapping taxon ID to scientific names. May be null.
 */
final case class Taxonomy(parents: Array[Taxon], taxonRanks: Array[String],
                          scientificNames: Array[String]) {
  import Taxonomy._

  def getRank(taxon: Taxon): Option[String] = Option(taxonRanks(taxon))
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
   * @return
   */
  @tailrec
  def depth(tax: Taxon, acc: Int = 0): Int = {
    if (tax == NONE) {
      acc
    } else {
      depth(parents(tax),  acc + 1)
    }
  }

  /** Find whether the given taxon has the given parent.  */
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
   * If it doesn't exist (for example because the level is too low), then ROOT will be returned. */
  def ancestorAtLevel(query: Taxon, level: String): Taxon =
    ancestorAtLevel(query, query, level)

  @tailrec
  private def ancestorAtLevel(query: Taxon, at: Taxon, level: String): Taxon = {
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

  /** Find whether the given path contains the given item.
   * Paths are terminated by the NONE marker. Since path buffers are reused, the contents of the buffer
   * past NONE is undefined.
   */
  def pathContains(path: Array[Taxon], item: Int): Boolean = {
    var i = 0
    while (path(i) != NONE) { //end marker
      if (path(i) == item) return true
      i += 1
    }
    false
  }

  private val PATH_MAX_LENGTH = 256
  def newPathBuffer: Array[Taxon] = Array.fill(PATH_MAX_LENGTH)(NONE)

  /**
   * Lowest common ancestor of two taxa.
   * Algorithm from Kraken's krakenutil.cpp. This algorithm (no longer used in Kraken2)
   * has the advantage that parents do not need to have a taxid smaller than their children.
   * @param buffer Reusable buffer, created by the newPathBuffer method above
   * @param tax1
   * @param tax2
   * @return
   */
  def lca(buffer: Array[Taxon])(tax1: Taxon, tax2: Taxon): Taxon = {
    if (tax1 == NONE || tax2 == NONE) {
      return if (tax2 == NONE) tax1 else tax2
    }

    var a = tax1

    var i = 0
    while (a != NONE) {
      //The path length must never exceed the buffer size - if it does, increase PATH_MAX_LENGTH above
      buffer(i) = a
      i += 1
      a = parents(a)
    }
    buffer(i) = NONE // mark end
    val path1 = buffer

    //Note: this algorithm is quadratic in the average path length and would not scale well to deep
    //hierarchies
    var b = tax2
    while (b != NONE) {
      if (pathContains(path1, b)) {
        return b
      }
      b = parents(b)
    }
    NONE
  }

  /**
   * Take all hit taxa plus ancestors, then return the leaf of the highest weighted leaf-to-root path.
   * Based on the algorithm in Kraken 2 classify.cc, simplified.
   * @param hitCounts
   */
  def resolveTree(hitCounts: collection.Map[Taxon, Int]): Taxon = {
    val buffer = newPathBuffer
    var maxTaxon = 0
    var maxScore = 0
    val it = hitCounts.iterator

    while(it.hasNext) {
      val taxon = it.next._1
      if (taxon != AMBIGUOUS && taxon != MATE_PAIR_BORDER) {
        var node = taxon
        var score = 0
        while (node != NONE) {
          score += hitCounts.getOrElse(node, 0)
          node = parents(node)
        }

        if (score > maxScore) {
          maxTaxon = taxon
          maxScore = score
        } else if (score == maxScore) {
          maxTaxon = lca(buffer)(maxTaxon, taxon)
        }
      }
    }
    maxTaxon
  }

  /** By traversing the tree upward from a given starting set of leaf taxons, count the total number of taxons
   * present in the entire tree.
   */
  def countDistinctTaxaWithParents(taxons: Iterable[Taxon]): Int = {
    val r = mutable.BitSet.empty
    for { a <- taxons } {
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
