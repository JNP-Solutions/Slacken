/*
 * This file is part of Discount. Copyright (c) 2019-2024 Johan Nyström-Persson.
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

import org.apache.spark.sql.functions.{col, element_at, expr}
import org.apache.spark.sql.{Column, Encoder, Encoders, SparkSession}

/** Disk format of an index with key-value pairs.
 * The key is of variable width depending on the format (as a fixed number of Long columns).
 * The value type is specified by the subclass.
 */

trait IndexFormat {
  val spark: SparkSession

  def idColumnsTypes: String = idColumnNames.map(c => s"$c long").mkString(",")

  /** All key columns as a single string */
  def idColumnsString: String = idColumnNames.mkString(",")

  /** Names of the key columns */
  def idColumnNames: List[String]

  /** All key columns */
  def idColumns: List[Column] = idColumnNames.map(col)

  def numIdColumns: Int = idColumns.size

  def idColumnsFromMinimizer: List[Column]
}

/** An index where the key is one Long column. */
trait IndexFormat1 extends IndexFormat {
  import spark.sqlContext.implicits._

  def idColumnNames = List("id1")

  def idColumnsFromMinimizer =
    List(element_at($"minimizer", 1).as("id1"))
}

/** An index where the key is two Long columns. */
trait IndexFormat2 extends IndexFormat {
  import spark.sqlContext.implicits._

  def idColumnNames = List("id1", "id2")

  def idColumnsFromMinimizer =
    List(element_at($"minimizer", 1).as("id1"),
      element_at($"minimizer", 2).as("id2"))
}

/** An index where the key is four Long columns. */
trait IndexFormat4 extends IndexFormat {
  import spark.sqlContext.implicits._

  def idColumnNames = List("id1", "id2", "id3", "id4")

  def idColumnsFromMinimizer =
    List(element_at($"minimizer", 1).as("id1"),
      element_at($"minimizer", 2).as("id2"),
      element_at($"minimizer", 3).as("id3"),
      element_at($"minimizer", 4).as("id4"))
}

