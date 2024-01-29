/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */


package com.jnpersson.slacken

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should._
import scala.collection.mutable.{IndexedSeq => MISeq}
import scala.collection.mutable.Map

class TaxonCountsTest extends AnyFunSuite with Matchers {

  def append(t1: TaxonCounts, t2: TaxonCounts) = {
    TaxonCounts.concatenate(MISeq(t1, t2))
  }

  test("concatenate") {
    append(TaxonCounts(1, MISeq(1, 3), MISeq(2, 1)),
      TaxonCounts(2, MISeq(3, 2), MISeq(1, 1))) should equal(
        TaxonCounts(1,  MISeq(1, 3, 2), MISeq(2, 2, 1)))

    append(TaxonCounts(1, MISeq(1, 3), MISeq(2, 1)),
      TaxonCounts(2, MISeq(4, 2), MISeq(1, 1))) should equal(
      TaxonCounts(1,  MISeq(1, 3, 4, 2), MISeq(2, 1, 1, 1)))

    append(TaxonCounts(1, MISeq(1), MISeq(1)),
      TaxonCounts(2, MISeq(1), MISeq(1))) should equal(
      TaxonCounts(1,  MISeq(1), MISeq(2)))
  }
}
