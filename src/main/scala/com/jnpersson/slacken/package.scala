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


package com.jnpersson

import com.jnpersson.kmers._
import com.jnpersson.kmers.minimizer._
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
 * Routines for taxonomic classification of reads from metagenomic datasets.
 */
package object slacken {
  type Taxon = Int
  type SegmentFlag = Int

  val AMBIGUOUS_SPAN: Taxon = -1
  val MATE_PAIR_BORDER: Taxon = -2

  //Special information about a segment, whether it is normal (sequence), ambiguous sequence,
  //or a mate pair border (between paired reads)
  //This should really be a trait/case object hierarchy, but spark can't serialize those currently.
  val SEQUENCE_FLAG: SegmentFlag = 1
  val AMBIGUOUS_FLAG: SegmentFlag = 2
  val MATE_PAIR_BORDER_FLAG: SegmentFlag = 3

  /**
   * A super-mer with sequence data with an ordinal from a named sequence.
   * By tracking the ordinal and the sequence title, the original sequences can be reconstructed.
   * @param segment the super-mer
   * @param flag ambiguous flag
   * @param ordinal the relative position of this super-mer in the original sequence
   * @param seqTitle title of the original sequence
   */
  final case class OrdinalSupermer(segment: Supermer, flag: SegmentFlag, ordinal: Int, seqTitle: SeqTitle)

  /**
   * A super-mer with a specific minimizer, which is potentially ambiguous, but without sequence data (so just a span)
   * By tracking the ordinal and the sequence title, the original sequences can be reconstructed.
   * @param minimizer minimizer
   * @param kmers number of k-mers in this span
   * @param flag ambiguous flag
   * @param ordinal the relative position of this span in the original sequence
   * @param seqTitle title of the original sequence
   * */
  final case class OrdinalSpan(minimizer: Array[Long], kmers: Int, flag: SegmentFlag, ordinal: Int,
                               seqTitle: SeqTitle) {

    /** For a super-mer with a given minimizer, assign a taxon hit, handling ambiguity flags correctly
     * @param taxon  The minimizer's LCA taxon
     * */
    def toHit(taxon: Option[Taxon]): TaxonHit = {
      val reportTaxon =
        if (flag == AMBIGUOUS_FLAG) AMBIGUOUS_SPAN
        else if (flag == MATE_PAIR_BORDER_FLAG) MATE_PAIR_BORDER
        else taxon.getOrElse(Taxonomy.NONE)

      TaxonHit(minimizer, ordinal, reportTaxon, kmers)
    }
  }

}
