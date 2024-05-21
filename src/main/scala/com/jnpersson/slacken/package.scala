/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */


package com.jnpersson

import com.jnpersson.discount.SeqTitle
import com.jnpersson.discount.spark.Inputs

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
   * @param ordinal the relative position of this super-mer in the original sequence
   * @param seqTitle title of the original sequence
   */
  final case class OrdinalSupermer(segment: Supermer, flag: SegmentFlag, ordinal: Int, seqTitle: SeqTitle)

  /**
   * A super-mer with a specific minimizer, which is potentially ambiguous, but without sequence data (so just a span)
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

  /** A library containing some number of genomes (or scaffolds etc) labelled with taxa.
   * @param inputs Input genome sequence files
   * @param labelFile Path to a file labelling each sequence with a taxon (2-column TSV)
   */
  final case class GenomeLibrary(inputs: Inputs, labelFile: String)

}
