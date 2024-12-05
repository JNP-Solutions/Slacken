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

  test("resolveTree") {
    forAll(taxonomies(10), Gen.choose(10, 100)) { (t, k) =>
      val taxon = t.taxa.next()
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
