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

import org.scalacheck.Gen
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class LowestCommonAncestorProps extends AnyFunSuite with ScalaCheckPropertyChecks with Matchers {

  import Testing._

  /** Generate taxon hits from a read.
   * @param taxa Taxa for valid hits in the read
   * @param kmers total number of k-mers
   * @param invalidFrac fraction of invalid (NONE) k-mers in the read
   */
  def readHits(taxa: Array[Taxon], kmers: Int, invalidFrac: Double): Gen[Array[TaxonHit]] = {
    val invalidKmers = Math.floor(kmers * invalidFrac).toInt
    val validKmers = kmers - invalidKmers

    for {
      validPart <- pseudoRead(taxa, validKmers, 10)
      invalidPart <- pseudoRead(Array(Taxonomy.NONE), invalidKmers, 10)
      permuted <- permutations(validPart ++ invalidPart)
    } yield permuted.toArray
  }

  case class TaxonFraction(taxon: Taxon, fraction: Double)

  /** Fraction of k-mers in a pseudo-read occupied by a taxon and its ancestors */
  def fractionAbove(taxonomy: Taxonomy, taxon: Taxon, hits: Array[TaxonHit]): TaxonFraction = {
    val f = if (hits.isEmpty) 0 else
      hits.filter(h => taxonomy.hasAncestor(taxon, h.taxon)).map(_.count).sum.toDouble /
        hits.map(_.count).sum
    TaxonFraction(taxon, f)
  }

  /** Fraction of k-mers in a pseudo-read occupied by a taxon and its descendants */
  def fractionBelow(taxonomy: Taxonomy, taxon: Taxon, hits: Array[TaxonHit]): TaxonFraction = {
    val f = if (hits.isEmpty) 0 else
      hits.filter(h => taxonomy.hasAncestor(h.taxon, taxon)).map(_.count).sum.toDouble /
        hits.map(_.count).sum
    TaxonFraction(taxon, f)
  }

  //Calculate the expected result of resolveTree in a way that is (hopefully)
  //easy to trust and validate. Then check the actual (highly optimised)
  //resolveTree implementation against this.
  def correctClassification(lca: LowestCommonAncestor, taxonomy: Taxonomy,
                            hits: Array[TaxonHit], threshold: Double): Taxon = {
    //First, calculate the expected classification result for the generated pseudo-read.
    val distinctTaxa = hits.map(_.taxon).filter(_ != Taxonomy.NONE).distinct

    //Descending sort by fraction. Find most heavily weighted path.
    val bestHits = distinctTaxa.map(x => fractionAbove(taxonomy, x, hits)).
      sortBy(_.fraction).reverse.toList

    var bestHit = bestHits.headOption.getOrElse(TaxonFraction(Taxonomy.NONE, 0))
    var remainingHits = if (bestHits.nonEmpty) bestHits.tail else bestHits

    //Equal scores for multiple paths get resolved by LCA
    while (remainingHits.nonEmpty && remainingHits.head.fraction == bestHit.fraction) {
      bestHit = TaxonFraction(lca(bestHit.taxon, remainingHits.head.taxon), bestHit.fraction)
      remainingHits = remainingHits.tail
    }
    val bestTaxon = bestHit.taxon

    //Find the coverage fraction at each level in the bestTaxon's path to root
    val fractionLevels = taxonomy.pathToRoot(bestTaxon).map(tax => fractionBelow(taxonomy, tax, hits))
    //Find an ancestor in the path of bestHit that has a sufficient support fraction
    fractionLevels.dropWhile(_.fraction < threshold).toStream.
      map(_.taxon).
      headOption.getOrElse(Taxonomy.NONE)
  }

  test("resolveTree") {
    forAll(taxonomies(100), Gen.choose(10, 200).label("kmers"),
      Gen.choose(0.0, 1.0).label("invalidFrac"), Gen.choose(0.0, 1.0).label("threshold")) {
      (t, kmers, invalidFrac, threshold) =>
      whenever(kmers >= 0 && invalidFrac >= 0 && threshold >= 0) {
        val lca = new LowestCommonAncestor(t)
        forAll(readHits(t.taxa.toArray, kmers, invalidFrac)) { hits =>
          val correct = correctClassification(lca, t, hits, threshold)

          val r = lca.resolveTree(TaxonCounts.fromHits(hits), threshold)
          r should equal(correct)
        }
      }
    }
  }

}
