/*
 * This file is part of Hypercut. Copyright (c) 2023 Johan Nyström-Persson.
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

  val AMBIGUOUS_SPAN: Taxon = -1
  val MATE_PAIR_BORDER: Taxon = -2

  //Special information about a segment, whether it is normal (sequence), ambiguous sequence,
  //or a mate pair border (between paired reads)
  //This should really be a trait/case object hierarchy, but spark can't serialize those currently.
  val SEQUENCE_FLAG: SegmentFlag = 1
  val AMBIGUOUS_FLAG: SegmentFlag = 2
  val MATE_PAIR_BORDER_FLAG: SegmentFlag = 3

  /**
   * A super-mer with sequence data with an ordinal from a named sequence
   * @param segment the super-mer
   * @param flag ambiguous flag
   * @param ordinal the relative position of this segment in the original sequence
   * @param seqTitle title of the original sequence
   */
  final case class OrdinalSegmentWithSequence(segment: HashSegment, flag: SegmentFlag, ordinal: Int, seqTitle: SeqTitle)

  /**
   * A super-mer with a specific minimizer, potentially ambiguous, without sequence data
   * @param id1 first part of the minimizer
   * @param id2 second part of the minimizer
   * @param kmers number of k-mers in this segment
   * @param flag ambiguous flag
   * @param ordinal the relative position of this segment in the original sequence
   * @param seqTitle title of the original sequence
   * */
  final case class OrdinalSegment(id1: Long, id2: Long, kmers: Int, flag: SegmentFlag, ordinal: Int,
                                  seqTitle: SeqTitle)
}
