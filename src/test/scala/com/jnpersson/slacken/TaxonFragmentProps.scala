/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */

package com.jnpersson.slacken

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import com.jnpersson.kmers.TestGenerators._
import org.scalacheck.Gen

class TaxonFragmentProps extends AnyFunSuite with ScalaCheckPropertyChecks with Matchers {
  def fragments = dnaStrings(10000, 100000)
  val ks = Gen.choose(50, 200)
  val splitSize = Gen.choose(100, 10000)

  test("splits preserve all reads") {
    forAll(ks, fragments, splitSize) { (k, f, ss) =>
      whenever(f.length >= k && k >= 2 && k <= ss) {
        val tf = TaxonFragment(0, f, "")
        val split = tf.splitToMaxLength(ss, k)
        val sfs = split.map(_.nucleotides).toArray
        for { pair <- sfs.sliding(2)
              if pair.length == 2 } {
          pair(0).takeRight(k - 1) should equal(pair(1).take(k - 1))
        }
        sfs.map(x => x.substring(0, x.length - (k - 1))).mkString("") + sfs.last.takeRight(k - 1) should equal(f)
      }
    }

  }
}
