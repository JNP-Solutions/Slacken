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

package com.jnpersson

import com.jnpersson.discount.SeqTitle
import com.jnpersson.discount.hash.BucketId

/**
 * Routines for taxonomic classification of reads from metagenomic datasets.
 */
package object slacken {
  type Taxon = Int
  type SegmentFlag = Int

  val AMBIGUOUS: Taxon = -1
  val MATE_PAIR_BORDER: Taxon = -2

  //Special information about a segment, whether it is normal (sequence), ambiguous sequence,
  //or a mate pair border (between paired reads)
  //This should really be a trait/case object hierarchy, but spark can't serialize those currently.
  val SEQUENCE_FLAG: SegmentFlag = 1
  val AMBIGUOUS_FLAG: SegmentFlag = 2
  val MATE_PAIR_BORDER_FLAG: SegmentFlag = 3

  /** A super-mer with sequence data with an ordinal from a named sequence */
  final case class OrdinalSegmentWithSequence(segment: HashSegment, flag: SegmentFlag, ordinal: Int, seqTitle: SeqTitle)

  /** A super-mer with a specific hash, potentially ambiguous, without sequence data */
  final case class OrdinalSegment(hash: BucketId, kmers: Int, flag: SegmentFlag, ordinal: Int, seqTitle: SeqTitle)

  /** As above, but with a 128-bit hash, for Slacken 2 */
  final case class S2OrdinalSegment(id1: Long, id2: Long, kmers: Int, flag: SegmentFlag, ordinal: Int,
                                    seqTitle: SeqTitle)
}
