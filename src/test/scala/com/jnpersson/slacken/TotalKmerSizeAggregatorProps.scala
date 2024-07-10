/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */

package com.jnpersson.slacken

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class TotalKmerSizeAggregatorProps extends AnyFunSuite with ScalaCheckPropertyChecks with Matchers {
  import Testing._

  def genomeSizes(tax: Taxonomy): Array[(Taxon, Long)] = {
    val genomeLengthMax = 1000000000
    val leafNodes = tax.taxa.filter(t => tax.isLeafNode(t))
    leafNodes.toArray.map(t => (t, scala.util.Random.nextInt(genomeLengthMax).toLong))
  }

  test("leaf node total k-mer counts are correctly measured") {
    forAll(taxonomies(100)) { tax =>
      val sizes = genomeSizes(tax)
      val sizeMap = sizes.toMap
      val agg = new TotalKmerSizeAggregator(tax, sizes)
      for { t <- tax.taxa } {
        if (tax.isLeafNode(t)) {
          sizeMap(t) should equal(agg.totKmerAverageS1(t))
          sizeMap(t) should equal(agg.totKmerAverageS2(t))
          sizeMap(t) should equal(agg.totKmerAverageS3(t))
        }
      }
    }
  }

}

