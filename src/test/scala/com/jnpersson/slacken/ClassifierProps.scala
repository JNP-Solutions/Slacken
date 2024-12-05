package com.jnpersson.slacken

import org.scalacheck.Gen
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ClassifierProps extends AnyFunSuite with ScalaCheckPropertyChecks with Matchers {

  import Testing._

  def readHits(taxon: Taxon, kmers: Int): Gen[Array[TaxonHit]] = {
    val sizePerHit = 5
    Gen.listOfN(kmers / sizePerHit, taxonHits(Array(taxon), sizePerHit)).map(_.toArray)
  }

  def readHitsLineage(taxonomy: Taxonomy, taxon: Taxon, kmers: Int): Gen[Array[TaxonHit]] = {
    val sizePerHit = 5
    val lineage = taxonomy.pathToRoot(taxon).toArray
    Gen.listOfN(kmers / sizePerHit, taxonHits(lineage, sizePerHit)).map(_.toArray)
  }

  test("resolveTree") {
    forAll(taxonomies(100), Gen.choose(10, 100), Gen.choose(1, 99)) { (t, k, taxon) =>
      whenever(taxon > 0) {
        val lca = new LowestCommonAncestor(t)
        forAll(readHits(taxon, k)) { hits =>
          val r = lca.resolveTree(TaxonCounts.fromHits(hits), 0)
          if (hits.size == 0)
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
      whenever(taxon > 0) {
        val lca = new LowestCommonAncestor(t)
        forAll(readHitsLineage(t, taxon, k)) { hits =>
          val lowestHit = hits.sortBy(hit => t.depth(hit.taxon)).last.taxon
          val r = lca.resolveTree(TaxonCounts.fromHits(hits), 0)
          if (hits.size == 0)
            r should equal(Taxonomy.NONE)
          else
            r should equal(lowestHit)
        }
      }
    }
  }

}
