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

import com.jnpersson.kmers.SparkSessionTestWrapper
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable

class BrackenWeightsTest extends AnyFunSuite with SparkSessionTestWrapper with Matchers {
  implicit val sp = spark
  import spark.sqlContext.implicits._

  test("Read counts from the underlying genomes are correct") {
    val k = 35
    val m = 31
    val s = 7
    val readLen = 100
    val idx = TestData.indexWithRecords(k, m, s, None)

    val bw = new BrackenWeights(idx, readLen)
    val sourceDestCounts = bw.buildWeights(TestData.library(readLen), mutable.BitSet.empty ++ TestData.taxonomy.taxa)
    try {
      sourceDestCounts.as[(Int, Int, Long)].collect() should contain theSameElementsAs(TestData.brackenWeightsLength100)
    } finally {
      sourceDestCounts.unpersist()
    }
  }
}
