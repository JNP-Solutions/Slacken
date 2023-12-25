/*
 * This file is part of Hypercut. Copyright (c) 2023 Johan Nyström-Persson.
 */


package com.jnpersson.slacken

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should._
import scala.collection.mutable.{IndexedSeq => MISeq}
import scala.collection.mutable.Map

class TaxonSummaryTest extends AnyFunSuite with Matchers {

  def append(t1: TaxonSummary, t2: TaxonSummary) = {
    TaxonSummary.concatenate(MISeq(t1, t2))
  }

  test("concatenate") {
    append(TaxonSummary(1, MISeq(1, 3), MISeq(2, 1)),
      TaxonSummary(2, MISeq(3, 2), MISeq(1, 1))) should equal(
        TaxonSummary(1,  MISeq(1, 3, 2), MISeq(2, 2, 1)))

    append(TaxonSummary(1, MISeq(1, 3), MISeq(2, 1)),
      TaxonSummary(2, MISeq(4, 2), MISeq(1, 1))) should equal(
      TaxonSummary(1,  MISeq(1, 3, 4, 2), MISeq(2, 1, 1, 1)))

    append(TaxonSummary(1, MISeq(1), MISeq(1)),
      TaxonSummary(2, MISeq(1), MISeq(1))) should equal(
      TaxonSummary(1,  MISeq(1), MISeq(2)))
  }

  test("mergeHits") {
    val testVals = List(
      TaxonSummary(1, MISeq(1, 3), MISeq(1, 1)),
      TaxonSummary(2, MISeq(1, 2), MISeq(1, 1)),
      TaxonSummary(3, MISeq(1, 4), MISeq(1, 1))
    )
    TaxonSummary.hitCountsToMap(testVals) should equal (Map(1 -> 3, 2 -> 1, 3 -> 1, 4 -> 1))

    val testVals2 = List(
      TaxonSummary(1, MISeq(1), MISeq(1)),
      TaxonSummary(2, MISeq(1), MISeq(1))
    )
    TaxonSummary.hitCountsToMap(testVals2) should equal (Map(1 -> 2))
  }
}
