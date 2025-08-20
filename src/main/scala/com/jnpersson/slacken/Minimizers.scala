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

import com.jnpersson.kmers.NTSeq
import org.apache.spark.sql.{DataFrame, Dataset}

/** Algorithms for finding and manipulating minimizers intended for a given KeyValueIndex.
 */
abstract class Minimizers(index: KeyValueIndex) {

  /** Find (minimizer, taxon) pairs in the given pairs of (taxon, NT sequence).
   * @param seqTaxa pairs of (taxon, NT sequence)
   * @return pairs of (minimizer, taxon). The number of minimizer columns depends on the minimizer width.
   */
  def find(seqTaxa: Dataset[(Taxon, NTSeq)]): DataFrame
}

/** Minimizers based on the MinSplitter. */
class SplitterMinimizers(index: KeyValueIndex) extends Minimizers(index) {
  implicit val spark = index.spark
  import spark.sqlContext.implicits._

  /** Find (minimizer, taxon) pairs in the given pairs of (taxon, NT sequence).
   * @param seqTaxa pairs of (taxon, NT sequence)
   * @return pairs of (minimizer, taxon).
   */
  def find(seqTaxa: Dataset[(Taxon, NTSeq)]): DataFrame = {
    val bcSplit = index.bcSplit
    val recordColumnNames = index.recordColumnNames

    index.numIdColumns match {
      case 1 =>
        seqTaxa.flatMap(r =>
          bcSplit.value.superkmerPositions(r._2).map { min =>
            (min.rank(0), r._1)
          }
        ).toDF(recordColumnNames: _*)
      case 2 =>
        seqTaxa.flatMap(r =>
          bcSplit.value.superkmerPositions(r._2).map { min =>
            (min.rank(0), min.rank(1), r._1)
          }
        ).toDF(recordColumnNames: _*)
      case 3 =>
        seqTaxa.flatMap(r =>
          bcSplit.value.superkmerPositions(r._2).map { min =>
            (min.rank(0), min.rank(1), min.rank(2), r._1)
          }
        ).toDF(recordColumnNames: _*)
      case 4 =>
        seqTaxa.flatMap(r =>
          bcSplit.value.superkmerPositions(r._2).map { min =>
            (min.rank(0), min.rank(1), min.rank(2), min.rank(3), r._1)
          }
        ).toDF(recordColumnNames: _*)
      case _ =>
        //In case of minimizers wider than 128 bp (4 longs), expand this section
        ???
    }
  }
}
