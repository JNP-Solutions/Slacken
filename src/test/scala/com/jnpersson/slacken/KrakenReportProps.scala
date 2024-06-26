/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */

package com.jnpersson.slacken

import com.jnpersson.slacken.Testing.taxonomies
import org.scalacheck.{Gen, Shrink}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class KrakenReportProps extends AnyFunSuite with ScalaCheckPropertyChecks with Matchers {

  //Prevent actual values in the (taxon, count) list from shrinking. We only want it to shrink by
  //removing elements from it.
  implicit def shrinkTaxonCount: Shrink[(Taxon, Long)] = Shrink(_ => Stream.empty)

  test("Clade totals and taxon counts make sense") {
    forAll(taxonomies(100)) { tax =>
      val taxa = tax.taxa.toSeq :+ Taxonomy.NONE
      forAll(
        Gen.someOf(taxa).flatMap(ts =>
          Gen.listOfN(ts.size, Gen.choose(0L, 100L)).map(cs => ts.zip(cs)))
      ) { taxaCounts =>
        val rep = new KrakenReport(tax, taxaCounts.toArray)
        rep.taxonCounts.toSeq.sorted should equal(taxaCounts.sorted)
        for { (t, c) <- rep.cladeTotals } {
          c should be >= rep.taxonCounts(t)
        }
        for { (t, c) <- taxaCounts } {
          rep.cladeTotals(t) should be >= c
        }
      }
    }

  }
}
