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

  def readHitsLineage(taxonomy: Taxonomy, taxon: Taxon, kmers: Int, invalidFrac: Double): Gen[Array[TaxonHit]] = {
    if (taxon == Taxonomy.NONE) {
      readHits(Array(Taxonomy.NONE), kmers, invalidFrac)
    } else {
      val lineage = taxonomy.pathToRoot(taxon).toArray
      readHits(lineage, kmers, invalidFrac)
    }
  }

  /** Fraction of k-mers in a pseudo-read occupied by a taxon and its ancestors */
  def fractionPath(taxonomy: Taxonomy, taxon: Taxon, hits: Array[TaxonHit]): Double =
    if (hits.isEmpty) 0 else
      hits.filter(h => taxonomy.hasAncestor(taxon, h.taxon)).map(_.count).sum.toDouble /
        hits.map(_.count).sum

  /** Fraction of k-mers in a pseudo-read occupied by just the taxon */
  def fraction(taxon: Taxon, hits: Array[TaxonHit]): Double =
    if (hits.isEmpty) 0 else
      hits.filter(h => h.taxon == taxon).map(_.count).sum.toDouble /
        hits.map(_.count).sum

  def validFraction(hits: Array[TaxonHit]): Double =
    hits.filter(h => h.taxon != Taxonomy.NONE).map(_.count).sum.toDouble /
      hits.map(_.count).sum

  test("resolveTree") {
    forAll(taxonomies(100), Gen.choose(10, 100).label("kmers"),
      Gen.choose(1, 99).label("taxon"),
      Gen.choose(0.0, 1.0).label("invalidFrac"), Gen.choose(0.0, 1.0).label("threshold")) {
      (t, kmers, taxon, invalidFrac, threshold) =>
      whenever(taxon > 0 && taxon < t.size && kmers >= 0 && invalidFrac >= 0 && threshold >= 0) {
        val lca = new LowestCommonAncestor(t)
        forAll(readHitsLineage(t, taxon, kmers, invalidFrac)) { hits =>
          val measuredValidFraction = validFraction(hits)

          val r = lca.resolveTree(TaxonCounts.fromHits(hits), threshold)
          val distinctTaxa = hits.map(_.taxon).distinct

          //Descending sort by fraction. Find most heavily weighted path.
          val bestHit = distinctTaxa.map(x => (fractionPath(t, x, hits), x)).
            sortBy(x => x._1).reverse.headOption.map(_._2).getOrElse(Taxonomy.NONE)

          val fractionLevels = t.pathToRoot(bestHit).
            scanLeft[Double](0.0)((score, t) => score + fraction(t, hits)).drop(1)
          //Find an ancestor in the path of bestHit that has a sufficient support fraction
          val sufficientFraction = t.pathToRoot(bestHit).zip(fractionLevels).dropWhile(_._2 < threshold).toStream.
            map(_._1).
            headOption.getOrElse(Taxonomy.NONE)

//        println(s"$bestHit ${fractionPath(t, bestHit, hits)} ${TaxonCounts.fromHits(hits)}")
          r should equal(sufficientFraction)
        }
      }
    }
  }

}
