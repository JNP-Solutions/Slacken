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

  test("resolveTree") {
    forAll(taxonomies(100), Gen.choose(10, 200).label("kmers"),
      Gen.choose(0.0, 1.0).label("invalidFrac"), Gen.choose(0.0, 1.0).label("threshold")) {
      (t, kmers, invalidFrac, threshold) =>
      whenever(kmers >= 0 && invalidFrac >= 0 && threshold >= 0) {
        val lca = new LowestCommonAncestor(t)
        forAll(readHits(t.taxa.toArray, kmers, invalidFrac)) { hits =>
          //First, calculate the expected classification result for the generated pseudo-read.
          val distinctTaxa = hits.map(_.taxon).filter(_ != Taxonomy.NONE).distinct

          //Descending sort by fraction. Find most heavily weighted path.
          val bestHits = distinctTaxa.map(x => fractionAbove(t, x, hits)).
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
          val fractionLevels = t.pathToRoot(bestTaxon).map(tax => fractionBelow(t, tax, hits))
          //Find an ancestor in the path of bestHit that has a sufficient support fraction
          val sufficientFraction = fractionLevels.dropWhile(_.fraction < threshold).toStream.
            map(_.taxon).
            headOption.getOrElse(Taxonomy.NONE)

//        println(s"$bestHit ${fractionBelow(t, bestHit.taxon, hits)} ${TaxonCounts.fromHits(hits)}")
          val r = lca.resolveTree(TaxonCounts.fromHits(hits), threshold)
          r should equal(sufficientFraction)
        }
      }
    }
  }

}
