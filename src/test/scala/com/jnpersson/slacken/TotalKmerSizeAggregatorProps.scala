/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */

package com.jnpersson.slacken

import com.jnpersson.slacken.Taxonomy.ROOT
import org.apache.spark.sql.functions
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

  def testDataTaxonomy =
    Taxonomy.fromNodesAndNames(
      Array((455631, ROOT, "strain"),
        (526997, ROOT, "strain")),
      Iterator((455631, "Clostridioides difficile QCD-66c26"),
        (526997, "Bacillus mycoides DSM 2048"))
    )

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

