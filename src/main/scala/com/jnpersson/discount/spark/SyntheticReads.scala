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

package com.jnpersson.discount.spark

import com.jnpersson.discount.NTSeq
import com.jnpersson.discount.util.{NTBitArray}
import org.apache.spark.sql.{Dataset, SparkSession}

object SyntheticReads {

  /** Generate synthetic, random reads with unique ID numbers.
   * @param length the length of each read
   * @param n the number of reads to generate */
  def generateWithID(length: Int, n: Long)(implicit spark: SparkSession): Dataset[(NTSeq, Long)] = {
    import spark.sqlContext.implicits._
    val longs = length / 32 + 1
    val size = length
    val start = (0 until longs).toArray

    val part = spark.conf.get("spark.sql.shuffle.partitions").toInt
    spark.range(0, n, 1, part).mapPartitions(it => {
      val decoder = NTBitArray.fixedSizeDecoder(size)
      it.map(i => {
        val data = start.map(i => scala.util.Random.nextLong())
        (decoder.longsToString(data, 0, size), i)
      })
    }).as[(NTSeq, Long)]
  }

  /** Generate synthetic, random reads.
   * @param length the length of each read
   * @param n the number of reads to generate */
  def generate(length: Int, n: Long)(implicit spark: SparkSession): Dataset[NTSeq] = {
    import spark.sqlContext.implicits._
    generateWithID(length, n).map(_._1)
  }

}
