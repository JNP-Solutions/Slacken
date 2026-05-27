/*
 * This file is part of Slacken. Copyright (c) 2019-2026 Johan Nyström-Persson.
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

package com.jnpersson.discount.spark

import com.jnpersson.kmers.SparkSessionTestWrapper
import com.jnpersson.kmers.minimizer.Lexicographic
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Test for Discount CLI parsing. */
class DiscountConfTest extends AnyFunSuite with SparkSessionTestWrapper with Matchers {
  implicit val sp = spark

  def testConf(args: String) = {
    val s = new DiscountConf(args.split(" "))
    s.verify()
    s
  }

  test("basic") {
    val conf = testConf("-k 31 -m 10 -o lexicographic input.fasta stats")
    conf.k() should equal(31)
    conf.minimizerWidth() should equal(10)
    conf.ordering() should equal(Lexicographic)
    conf.inputFiles() should equal(List("input.fasta"))
  }

}