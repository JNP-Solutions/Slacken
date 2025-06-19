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

package com.jnpersson.kmers.input

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{collect_list, floor, lit, monotonically_increasing_id, row_number, slice}

/** A reader for fastq files that uses Spark's text file input support for transparent decompression.
 * This allows support for .gz and .bz2 codecs, among others. The downside is that there will be poor
 * parallelism, as each input file will have to be processed in a single partition.
 */
object FastqReader {
  def read(file: String)(implicit spark: SparkSession): Dataset[Array[String]] = {
    import spark.implicits._

      spark.read.text(file).
        withColumn("file", lit(file)).
        withColumn("rowId", monotonically_increasing_id()).
        withColumn("recId", //group every 4 rows and give them the same recId
          floor(
            (row_number().over(Window.partitionBy("file").orderBy("rowId")) - 1) / 4) //monotonically_incr starts at 1
          ).
        groupBy("file", "recId").agg(collect_list($"value").as("value")).
        select(slice($"value", 1, 2)).as[Array[String]] //Currently preserves only the header and the nucleotide string
  }
}
