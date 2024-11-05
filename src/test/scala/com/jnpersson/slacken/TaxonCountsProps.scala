/*
 *
 *  * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 *  *
 *  * Slacken is free software: you can redistribute it and/or modify
 *  * it under the terms of the GNU General Public License as published by
 *  * the Free Software Foundation, either version 3 of the License, or
 *  * (at your option) any later version.
 *  *
 *  * Slacken is distributed in the hope that it will be useful,
 *  * but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  * GNU General Public License for more details.
 *  *
 *  * You should have received a copy of the GNU General Public License
 *  * along with Slacken.  If not, see <https://www.gnu.org/licenses/>.
 *
 */


package com.jnpersson.slacken

import org.scalacheck._
import Shrink._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.collection.mutable.{IndexedSeq => MISeq}

class TaxonCountsProps extends AnyFunSuite with ScalaCheckPropertyChecks with Matchers {

  def taxonCounts: Gen[TaxonCounts] =
    for { taxa <- Gen.listOfN(3, Gen.choose(1, 10))
         counts <- Gen.listOfN(3, Gen.choose(1, 10))
         } yield TaxonCounts(taxa.to[MISeq], counts.to[MISeq])

  def taxonCountsShrink: Shrink[TaxonCounts] =
    Shrink { tc =>
      shrink(tc.asPairs.to[MISeq]).map(ps => TaxonCounts.fromPairs(ps))
    }

  test("concatenate") {
    forAll(taxonCounts, taxonCounts) { case (tc1, tc2) =>
      val concat = TaxonCounts.concatenate(List(tc1, tc2))

      //Iterator of concatenated values
      val cit = concat.asPairs
      val it = tc1.asPairs ++ tc2.asPairs

      //Check that the concatenated iterator retains the same values in the same order,
      //although two adjacent values for the same taxon may be combined
      while (it.hasNext) {
        val v = it.next()
        val c = cit.next()
        if (v == c) {
          //ok
        } else {
          it.hasNext should be(true)
          val v2 = it.next()
          c should equal ((v._1, v._2 + v2._2))
          v2._1 should equal(v._1)
        }
      }
    }
  }
}
