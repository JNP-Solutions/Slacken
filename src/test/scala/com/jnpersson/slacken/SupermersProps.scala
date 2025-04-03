/*
 * This file is part of Slacken. Copyright (c) 2019-2025 Johan Nyström-Persson.
 *
 * Slacken is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 *  Slacken is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 * along with Slacken.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.jnpersson.slacken

import com.jnpersson.kmers.TestGenerators._
import com.jnpersson.kmers.minimizer._
import org.scalacheck.Gen
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class SupermersProps extends AnyFunSuite with ScalaCheckPropertyChecks with Matchers {
  import Testing._
  import com.jnpersson.kmers.TestGenerators.shrinkNTSeq
  import com.jnpersson.kmers.TestGenerators.shrinkMAndK

  val nonDnaRnaChars = (('A' to 'Z') ++ ('a' to 'z')).
    filter(x => !dnaRnaCharsMixedCase.contains(x))

  def ambiguousSequence(minLen: Int, maxLen: Int): Gen[String] = for {
    length <- Gen.choose(minLen, maxLen)
    s <- Gen.stringOfN(length, Gen.oneOf(nonDnaRnaChars))
  } yield s

  test("splitByAmbiguity") {
    forAll((mAndKPairs, "mk")) { case (m, k) =>
      forAll(minimizerPriorities(m),
        dnaStrings(1, 2 * k), ambiguousSequence(1, 2 * k),
        dnaStrings(1, 2 * k), ambiguousSequence(1, 2 * k)
      ) { (mp, ua1, a1, ua2, a2) =>
        val sm = new Supermers(MinSplitter(mp, k), 1)
        val test = s"$ua1$a1$ua2$a2"
        val (ambig, unambig) = sm.splitByAmbiguity(test).partition(_._2 == AMBIGUOUS_FLAG)

        unambig.toList.map(_._1) should equal(List(ua1, ua2).filter(_.length >= k))
        ambig.toList.map(_._1).mkString("") should equal(
          List(Some(ua1).filter(_.length < k), Some(a1), Some(ua2).filter(_.length < k), Some(a2)).flatten.
            mkString("")
        )
      }
    }
  }
}
