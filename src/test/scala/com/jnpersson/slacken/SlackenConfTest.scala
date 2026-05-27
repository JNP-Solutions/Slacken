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

package com.jnpersson.slacken

import com.jnpersson.kmers.{HDFSUtil, SparkSessionTestWrapper}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Test for Slacken CLI parsing. */
class SlackenConfTest extends AnyFunSuite with SparkSessionTestWrapper with Matchers {
  implicit val sp = spark

  /** Test a set of command line arguments to Slacken. */
  def testArgs(args: String) = {
    val s = new SlackenConf(args.trim.split("\\s+"))
    s.verify()
    s
  }

  test("basic") {
    val conf = testArgs("classify -i inputIndex -o output file1.fasta")
    conf.classify.output() should equal("output")
    conf.classify.index() should equal(HDFSUtil.makeQualified("inputIndex"))
  }

  test("various") {
    testArgs(
      """
        |classify -i /data/standard-224c/std_35_31_s7 -p -o /data/sample0 -c 0.15
        |/data/anonymous_reads.part_001.fq /data/anonymous_reads.part_002.fq
        |""".stripMargin)

    testArgs(
      """
        |classify2 -i /data/standard-224c/std_35_31_s7
        | -o /data/sample0_R100 -c 0.15 -p
        |  --reads 100 --bracken-length 150
        |  -l /data/standard-224c
        |  /data/anonymous_reads.part_001.fq /data/anonymous_reads.part_002.fq
        |  """.stripMargin)

    testArgs(
      """
        |classify2 -i /data/standard-224c/std_35_31_s7 -p
        | -o test_class
        |  --classify-with-gold -g goldSet.txt --library /data/standard-224c --bracken-length 150
        |  sample01.1.fq sample01.2.fq sample02.1.fq sample02.2.fq
        |  """.stripMargin)

    testArgs(
      """
        | -p 2000 build -i mySlackenLib -k 35 -m 31 -t k2/taxonomy
        |  -l k2
        |  """.stripMargin)

    testArgs(
      """
        |bracken-build -i /data/standard-224c/std_35_31_s7
        | -l /data/standard-224c --read-len 150
        | """.stripMargin)
  }
}

