/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */


package com.jnpersson.slacken

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object TaxonCounts {

  /** Construct a TaxonCounts object from an ordinal and (taxon, count) pairs */
  def fromPairs(ordinal: Int, pairs: Iterable[(Taxon, Int)]): TaxonCounts = {
    val (taxa, counts) = pairs.toArray.unzip
    TaxonCounts(ordinal, taxa, counts)
  }

  /** Concatenate adjacent TaxonCounts (in order corresponding to the subject sequence)
   * into a single TaxonCounts object
   */
  def concatenate(summaries: Iterable[TaxonCounts]): TaxonCounts = {
    val maxSize = summaries.map(_.counts.size).sum
    val taxonRet = new ArrayBuffer[Taxon]()
    taxonRet.sizeHint(maxSize)
    val countRet = new ArrayBuffer[Int]()
    countRet.sizeHint(maxSize)

    for {
      s <- summaries
      if s.taxa.nonEmpty
    } {
      if (taxonRet.nonEmpty && taxonRet(taxonRet.size - 1) == s.taxa(0)) {
        //Overlap between two TaxonCounts
        countRet(countRet.size - 1) += s.counts(0)
      } else {
        taxonRet += s.taxa(0)
        countRet += s.counts(0)
      }
      for (i <- 1 until s.taxa.length) {
        taxonRet += s.taxa(i)
        countRet += s.counts(i)
      }
    }
    new TaxonCounts(summaries.headOption.map(_.ordinal).getOrElse(0), taxonRet, countRet)
  }
}

/**
 * Information about classified k-mers for a consecutive segment of a subject sequence.
 *
 * @param ordinal the relative position of this summary in a list of summaries for a subject sequence
 *              (not same as position in the query sequence)
 * @param taxa taxa for each classified region (may be repeated, but two consecutive taxa should not be the same)
 * @param counts k-mer counts for each taxon in the taxa array
 *
 */
final case class TaxonCounts(ordinal: Int, taxa: mutable.IndexedSeq[Taxon], counts: mutable.IndexedSeq[Int]) {

  /** Obtain counts for each taxon as pairs */
  def asPairs: Iterator[(Taxon, Int)] =
    taxa.iterator zip counts.iterator

  /** Convert TaxonCounts to a lookup map that maps each taxon to its
   * total hit count.
   * Will omit special taxa like AMBIGUOUS and MATE_PAIR_BORDER.
   */
  def toMap: mutable.Map[Taxon, Int] = {
    val r = mutable.Map.empty[Taxon, Int]
    for {
      (taxon, count) <- asPairs
      if taxon != AMBIGUOUS_SPAN && taxon != MATE_PAIR_BORDER
    } {
      if (r.contains(taxon)) {
        r(taxon) = r(taxon) + count
      } else {
        r(taxon) = count
      }
    }
    r
  }

  /** The total number of taxa counted here, including ambiguous spans, but not including any mate pair border. */
  def totalTaxa: Int =
    asPairs.
      filter(_._1 != MATE_PAIR_BORDER).
      map(_._2).sum


  private def taxonRepr(t: Taxon): String =
    if (t == AMBIGUOUS_SPAN) "A" else s"$t"

  /** Obtain a string representation from (taxon, count) pairs, suitable for the report */
  def pairsInOrderString: String = {
    val sb = new StringBuilder
    val pairs = asPairs
    for ((t, c) <- pairs) {
      if (t == MATE_PAIR_BORDER) {
        sb.append("|:|")
      } else {
        sb.append(taxonRepr(t))
        sb.append(":")
        sb.append(c)
      }
      if (pairs.hasNext) {
        sb.append(" ")
      }
    }
    sb.toString()
  }

  /** A string that summarises the length of these taxon counts, including potential mate pair separator.
   * Suitable for the report. */
  def lengthString(k: Int): String = {
    val border = taxa.indexOf(MATE_PAIR_BORDER)
    if (border == -1) {
      (counts.sum + (k - 1)).toString
    } else {
      (counts.take(border).sum + (k-1)) + "|" + (counts.drop(border + 1).sum + (k-1))
    }
  }

}
