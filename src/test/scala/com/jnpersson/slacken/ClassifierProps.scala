package com.jnpersson.slacken

import org.scalacheck.Gen
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ClassifierProps extends AnyFunSuite with ScalaCheckPropertyChecks with Matchers {

  import Testing._

  /** Generate taxon hits from a read.
   * @param taxon Taxon for valid hits
   * @param kmers total number of k-mers
   * @param invalidFrac fraction of invalid (NONE) k-mers in the read
   */
  def readHits(taxon: Taxon, kmers: Int, invalidFrac: Double): Gen[Array[TaxonHit]] = {
    val sizePerHit = 5
    val invalidKmers = Math.floor(kmers * invalidFrac).toInt
    val validKmers = kmers - invalidKmers

    for {
      validPart <- Gen.listOfN(validKmers / sizePerHit, taxonHits(Array(taxon), sizePerHit))
      invalidPart <- Gen.listOfN(invalidKmers / sizePerHit, taxonHits(Array(Taxonomy.NONE), sizePerHit))
      permuted <- permutations(validPart ++ invalidPart)
    } yield permuted.toArray
  }

  def readHitsLineage(taxonomy: Taxonomy, taxon: Taxon, kmers: Int): Gen[Array[TaxonHit]] = {
    val sizePerHit = 5
    if (taxon == Taxonomy.NONE) {
      readHits(Taxonomy.NONE, kmers, 0)
    } else {
      val lineage = taxonomy.pathToRoot(taxon).toArray
      Gen.listOfN(kmers / sizePerHit, taxonHits(lineage, sizePerHit)).map(_.toArray)
    }
  }

  test("resolveTree") {
    forAll(taxonomies(100), Gen.choose(10, 100).label("kmers"),
      Gen.choose(1, 99).label("taxon"),
      Gen.choose(0.0, 1.0).label("invalidFrac"), Gen.choose(0.0, 1.0).label("threshold")) {
      (t, kmers, taxon, invalidFrac, threshold) =>
      whenever(taxon > 0 && taxon < t.size && kmers >= 0 && invalidFrac >= 0 && threshold >= 0) {
        val lca = new LowestCommonAncestor(t)
        forAll(readHits(taxon, kmers, invalidFrac)) { hits =>
          val measuredValidCount = hits.filter(_.taxon != Taxonomy.NONE).map(_.count).sum
          //We can not precisely guarantee the valid fraction in the generated data, so it's easier to measure it
          val measuredValidFraction = if (hits.isEmpty) 0 else measuredValidCount.toDouble / hits.map(_.count).sum

          val r = lca.resolveTree(TaxonCounts.fromHits(hits), threshold)
          if (hits.size == 0 || measuredValidFraction < threshold)
            r should equal(Taxonomy.NONE)
          else
            r should equal(taxon)
        }
      }
    }
  }

  test("resolveTree lineage") {
    forAll(taxonomies(100), Gen.choose(10, 100), Gen.choose(1, 99)) { (t, k, taxon) =>
      // prevent taxon from shrinking below 1
      whenever(taxon > 0 && taxon < t.size) {
        val lca = new LowestCommonAncestor(t)
        forAll(readHitsLineage(t, taxon, k)) { hits =>
          val r = lca.resolveTree(TaxonCounts.fromHits(hits), 0)
          if (hits.size == 0)
            r should equal(Taxonomy.NONE)
          else {
            val lowestHit = hits.map(_.taxon).maxBy(t.depth)
            r should equal(lowestHit)
          }
        }
      }
    }
  }

}
