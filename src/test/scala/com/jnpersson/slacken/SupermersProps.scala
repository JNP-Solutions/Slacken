/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */

package com.jnpersson.slacken

import com.jnpersson.discount.TestGenerators._
import com.jnpersson.discount.hash.MinSplitter
import org.scalacheck.Gen
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class SupermersProps extends AnyFunSuite with ScalaCheckPropertyChecks with Matchers {
  import Testing._
  import com.jnpersson.discount.TestGenerators.shrinkNTSeq
  import com.jnpersson.discount.TestGenerators.shrinkMAndK

  val nonDnaRnaChars = (('A' to 'Z') ++ ('a' to 'z')).
    filter(x => !dnaRnaCharsArrayMixedCase.contains(x)).toArray

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
        val sm = new Supermers(MinSplitter(mp, k))
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
