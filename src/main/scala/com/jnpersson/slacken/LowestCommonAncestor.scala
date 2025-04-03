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

package com.jnpersson.slacken

import com.jnpersson.slacken.Taxonomy.NONE
import com.jnpersson.slacken.Taxonomy.ROOT
import it.unimi.dsi.fastutil.ints.Int2IntMap
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator


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
    ROOT
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
    val hitCounts = hitSummary.toMap
    val requiredScore = Math.ceil(confidenceThreshold * hitSummary.totalKmers)
    resolveTree(hitCounts, requiredScore)
  }

  /** A version of resolveTree that operates on an Int2IntMap (fastutil).
   * Note that Int2IntMap has a default value of 0 for missing keys.
   */
  def resolveTree(hitCounts: Int2IntMap, requiredScore: Double): Taxon = {
    var maxTaxon = 0
    var maxScore = 0

    val it = hitCounts.keySet().iterator()
    while (it.hasNext) {
      val taxon = it.nextInt()
      var node = taxon
      var score = 0
      //Accumulate score across this path to the root
      while (node != NONE) {
        score += hitCounts.applyAsInt(node)
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
    maxScore = hitCounts.applyAsInt(maxTaxon)
    while (maxTaxon != NONE && maxScore < requiredScore) {
      maxScore = 0

      val it2 = hitCounts.keySet().iterator()
      while (it2.hasNext) {
        val taxon = it2.nextInt()
        val score = hitCounts.applyAsInt(taxon)
        if (taxonomy.hasAncestor(taxon, maxTaxon)) {
          //Add score if taxon is in max_taxon's clade
          maxScore += score
        }
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

/**
 * An aggregator that merges taxa of the same k-mer by applying the LCA function.
 */
final case class TaxonLCA(bcTaxonomy: Broadcast[Taxonomy]) extends Aggregator[Taxon, Taxon, Taxon] {
  override def zero: Taxon = Taxonomy.NONE

  @transient
  private lazy val taxonomy = bcTaxonomy.value

  @transient
  private lazy val lca = new LowestCommonAncestor(taxonomy)

  override def reduce(b: Taxon, a: Taxon): Taxon = lca(b, a)

  override def merge(b1: Taxon, b2: Taxon): Taxon = lca(b1, b2)

  override def finish(reduction: Taxon): Taxon = reduction

  override def bufferEncoder: Encoder[Taxon] = Encoders.scalaInt

  override def outputEncoder: Encoder[Taxon] = Encoders.scalaInt
}


