/*
 * This file is part of Discount. Copyright (c) 2019-2023 Johan Nyström-Persson.
 *
 * Discount is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Discount is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Discount.  If not, see <https://www.gnu.org/licenses/>.
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
