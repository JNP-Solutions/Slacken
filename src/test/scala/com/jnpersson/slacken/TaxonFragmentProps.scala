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
        val tf = TaxonFragment(0, f, "", 0)
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
