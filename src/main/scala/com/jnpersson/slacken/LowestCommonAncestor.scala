/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */

package com.jnpersson.slacken

import com.jnpersson.slacken.Taxonomy.NONE
import scala.collection.{Map => CMap}

/**
 * Lowest common ancestor algorithm. The calculation needs a data buffer,
 * so it is recommended to create one instance of this class in each thread and reuse it.
 */
final class LowestCommonAncestor(taxonomy: Taxonomy) {
  private val PATH_MAX_LENGTH = 256

  private val taxonPath: Array[Taxon] = Array.fill(PATH_MAX_LENGTH)(NONE)

  private val parents = taxonomy.parents

  /**
   * Lowest common ancestor of two taxa.
   * Algorithm from krakenutil.cpp (Kraken 1). This algorithm has the advantage that parents do not need to have a
   * taxid smaller than their children.
   * Note that this algorithm is quadratic in the average path length.
   * @param tax1 taxon 1
   * @param tax2 taxon 2
   * @return LCA(taxon1, taxon2)
   */
  def apply(tax1: Taxon, tax2: Taxon): Taxon = {
    if (tax1 == NONE || tax2 == NONE) {
      return if (tax2 == NONE) tax1 else tax2
    }

    var a = tax1

    //Step 1: store the entire path from taxon a to root in the buffer
    var i = 0
    while (a != NONE) {
      //The path length must never exceed the buffer size - if it does, increase PATH_MAX_LENGTH above
      taxonPath(i) = a
      i += 1
      a = parents(a)
    }
    taxonPath(i) = NONE // mark end

    //Step 2: traverse the path from b to root, checking whether each step is
    //contained in the path of a (in the buffer). If it is, we have found the LCA.
    var b = tax2
    while (b != NONE) {
      i = 0
      while (taxonPath(i) != NONE) {
        if (taxonPath(i) == b) return b
        i += 1
      }
      b = parents(b)
    }
    NONE
  }

  /**
   * For a given fragment to be classified, take all hit taxa plus ancestors, then return the leaf of the highest
   * weighted leaf-to-root path.
   * If the highest weighted path has a score below the confidence threshold, the taxon may move up in the tree to
   * increase the score of that clade.
   *
   * Based on the algorithm in Kraken 2's classify.cc.
   * @param hitSummary taxon hit counts
   * @param confidenceThreshold the minimum fraction of minimizers that must be included below the clade of the
   *                            matching taxon (if not, the taxon will move up in the tree)
   */
  def resolveTree(hitSummary: TaxonCounts, confidenceThreshold: Double): Taxon = {
    //the number of times each taxon was seen in a read, excluding ambiguous
    val hitCounts = hitSummary.toMap.withDefaultValue(0)
    val requiredScore = Math.ceil(confidenceThreshold * hitSummary.totalTaxa)
    resolveTreeInner(hitCounts, requiredScore)
  }

  def resolveTree(hitCounts: CMap[Taxon, Int], confidenceThreshold: Double): Taxon = {
    val requiredScore = Math.ceil(confidenceThreshold * hitCounts.iterator.map(_._2).sum)
    resolveTreeInner(hitCounts, requiredScore)
  }

  def resolveTreeInner(hitCounts: CMap[Taxon, Int], requiredScore: Double): Taxon = {
    var maxTaxon = 0
    var maxScore = 0

    for { (taxon, _) <- hitCounts } {
      var node = taxon
      var score = 0
      //Accumulate score across this path to the root
      while (node != NONE) {
        score += hitCounts(node)
        node = parents(node)
      }

      if (score > maxScore) {
        maxTaxon = taxon
        maxScore = score
      } else if (score == maxScore) {
        maxTaxon = apply(maxTaxon, taxon)
      }
    }

    //Gradually lift maxTaxon to try to achieve the required score
    maxScore = hitCounts(maxTaxon)
    while (maxTaxon != NONE && maxScore < requiredScore) {
      maxScore = 0
      for {
        (taxon, score) <- hitCounts
        if taxonomy.hasAncestor(taxon, maxTaxon)
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
}

