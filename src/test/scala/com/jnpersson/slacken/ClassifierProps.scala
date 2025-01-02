package com.jnpersson.slacken

import org.scalacheck.Gen
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ClassifierProps extends AnyFunSuite with ScalaCheckPropertyChecks with Matchers {

  import Testing._

  /** Generate taxon hits from a read.
   * @param taxa Taxa for valid hits in the read
   * @param kmers total number of k-mers
   * @param invalidFrac fraction of invalid (NONE) k-mers in the read
   */
  def readHits(taxa: Array[Taxon], kmers: Int, invalidFrac: Double): Gen[Array[TaxonHit]] = {
    val sizePerHit = 5
    val invalidKmers = Math.floor(kmers * invalidFrac).toInt
    val validKmers = kmers - invalidKmers

    for {
      validPart <- Gen.listOfN(validKmers / sizePerHit, taxonHits(taxa, sizePerHit))
      invalidPart <- Gen.listOfN(invalidKmers / sizePerHit, taxonHits(Array(Taxonomy.NONE), sizePerHit))
      permuted <- permutations(validPart ++ invalidPart)
    } yield permuted.toArray
  }

  /** Fraction of k-mers in a pseudo-read occupied by a taxon and its ancestors */
  def fractionAbove(taxonomy: Taxonomy, taxon: Taxon, hits: Array[TaxonHit]): Double =
    if (hits.isEmpty) 0 else
      hits.filter(h => taxonomy.hasAncestor(taxon, h.taxon)).map(_.count).sum.toDouble /
        hits.map(_.count).sum

  /** Fraction of k-mers in a pseudo-read occupied by a taxon and its descendants */
  def fractionBelow(taxonomy: Taxonomy, taxon: Taxon, hits: Array[TaxonHit]): Double =
    if (hits.isEmpty) 0 else
      hits.filter(h => taxonomy.hasAncestor(h.taxon, taxon)).map(_.count).sum.toDouble /
        hits.map(_.count).sum

  test("resolveTree") {
    forAll(taxonomies(100), Gen.choose(10, 100).label("kmers"),
      Gen.choose(0.0, 1.0).label("invalidFrac"), Gen.choose(0.0, 1.0).label("threshold")) {
      (t, kmers, invalidFrac, threshold) =>
      whenever(kmers >= 0 && invalidFrac >= 0 && threshold >= 0) {
        val lca = new LowestCommonAncestor(t)
        forAll(readHits(t.taxa.toArray, kmers, invalidFrac)) { hits =>
          //First, calculate the expected classification result for the generated pseudo-read.

          val distinctTaxa = hits.map(_.taxon).filter(_ != Taxonomy.NONE).distinct

          //Descending sort by fraction. Find most heavily weighted path.
          val bestHits = distinctTaxa.map(x => (fractionAbove(t, x, hits), x)).
            sortBy(x => x._1).reverse.toList

          var bestHit = bestHits.headOption.getOrElse((0, Taxonomy.NONE))
          var remainingHits = if (bestHits.nonEmpty) bestHits.tail else bestHits

          //Equal scores for multiple paths get resolved by LCA
          while (remainingHits.nonEmpty && remainingHits.head._1 == bestHit._1) {
            bestHit = (bestHit._1, lca(bestHit._2, remainingHits.head._2))
            remainingHits = remainingHits.tail
          }
          val bestTaxon = bestHit._2

          //Find the coverage fraction at each level in the bestTaxon's path to root
          val fractionLevels = t.pathToRoot(bestTaxon).map(tax => fractionBelow(t, tax, hits))
          //Find an ancestor in the path of bestHit that has a sufficient support fraction
          val sufficientFraction = t.pathToRoot(bestTaxon).zip(fractionLevels).dropWhile(_._2 < threshold).toStream.
            map(_._1).
            headOption.getOrElse(Taxonomy.NONE)

//        println(s"$bestHit ${cladeFraction(t, bestHit._2, hits)} ${TaxonCounts.fromHits(hits)}")
          val r = lca.resolveTree(TaxonCounts.fromHits(hits), threshold)
          r should equal(sufficientFraction)
        }
      }
    }
  }

}
