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

package com.jnpersson.slacken.seqslab

import com.jnpersson.slacken.Slacken
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Slacken API entry point for Seqslab.
 *
 * @param indexLocation minimizer-LCA index location
 * @param detailed      whether to generate detailed output (otherwise, only reports will be generated)
 * @param sampleRegex   regular expression to group reads by sample. Applied to read header to extract sample ID.
 * @param confidence    confidence score to classify for (the default value is 0)
 * @param minHitGroups  minimum number of hit groups (the default value is 2)
 * @param unclassified  whether to include unclassified reads in the result
 * @param spark
 * @return
 */
class SlackenSeqslab(indexLocation: String,
                     detailed: Boolean,
                     sampleRegex: Option[String],
                     confidence: Double, minHitGroups: Int,
                     unclassified: Boolean)(implicit spark: SparkSession)
  extends Slacken(indexLocation, detailed, sampleRegex, confidence, minHitGroups, unclassified) {

  /** Convenience constructor for SeqsLab (temporary fix, this constructor will no longer
   * be needed when Seqslab can process Option parameters in the scala plugin)
   */
  def this(indexLocation: String, detailed: Boolean, sampleRegex: String, confidence: Double, minHitGroups: Int,
           unclassified: Boolean)(implicit spark: SparkSession) =
    this(indexLocation, detailed, Some(sampleRegex), confidence, minHitGroups, unclassified)

  /** Convenience constructor for SeqsLab. Temporary fix as above.
   */
  def this(indexLocation: String, detailed: Boolean, confidence: Double, minHitGroups: Int,
           unclassified: Boolean)(implicit spark: SparkSession) =
    this(indexLocation, detailed, None, confidence, minHitGroups, unclassified)

  /**
   * Classify reads. This method renames the Seqslab standard column names (id, seq) to
   * Slacken column names (header, nucleotides).
   *
   * @param reads  reads to classify (R1 or singles)
   * @param reads2 optionally, R2 reads to classify in the case of paired-end reads.
   * @return a dataframe populated with [[ClassifiedRead]] objects.
   */
  override def classifyReads(reads: DataFrame, reads2: Option[DataFrame]): DataFrame = {
    val nameScheme = Map("id" -> "header", "seq" -> "nucleotides")
    val renamedR1 = reads.withColumnsRenamed(nameScheme)
    val renamedR2 = reads2.map(_.withColumnsRenamed(nameScheme))

    super.classifyReads(renamedR1, renamedR2)
  }
}
