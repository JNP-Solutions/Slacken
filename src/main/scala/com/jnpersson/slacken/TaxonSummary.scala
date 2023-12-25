/*
 * This file is part of Hypercut. Copyright (c) 2023 Johan Nyström-Persson.
 */


package com.jnpersson.slacken

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object TaxonSummary {

  /** Convert a collection of TaxonSummaries to a lookup map that maps each taxon to its
   * total hit count. */
  def hitCountsToMap(summaries: Iterable[TaxonSummary]): mutable.Map[Taxon, Int] = {
    val r = mutable.Map.empty[Taxon, Int]
    for {
      x <- summaries
      (taxon, count) <- x.asPairs
    } {
      if (r.contains(taxon)) {
        r(taxon) = r(taxon) + count
      } else {
        r(taxon) = count
      }
    }
    r
  }

  /** Concatenate adjacent TaxonSummaries (in order corresponding to the subject sequence)
   * into a single TaxonSummary object
   */
  def concatenate(summaries: Iterable[TaxonSummary]): TaxonSummary = {
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
    new TaxonSummary(summaries.head.ordinal, taxonRet, countRet)
  }

  def forTaxon(ordinal: Int, taxon: Taxon, kmers: Int): TaxonSummary =
    TaxonSummary(ordinal, Array(taxon), Array(kmers))

  def taxonRepr(t: Taxon): String =
    if (t == AMBIGUOUS) "A" else s"$t"

  def stringFromPairs(pairs: Iterator[(Taxon, Int)]): String = {
    val sb = new StringBuilder
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
}

/**
 * Information about classified k-mers for a consecutive segment of a subject sequence.
 *
 * @param ordinal the position of this summary in a list of summaries for a subject sequence
 *              (not same as position in the query sequence)
 * @param taxa taxa for each classified region (may be repeated)
 * @param counts counts for each taxon
 *
 */
final case class TaxonSummary(ordinal: Int, taxa: mutable.IndexedSeq[Taxon], counts: mutable.IndexedSeq[Int]) {

   def groupsInOrder: String =
    TaxonSummary.stringFromPairs(taxa.iterator zip counts.iterator)

  def lengthString(k: Int): String = {
    val border = taxa.indexOf(MATE_PAIR_BORDER)
    if (border == -1) {
      (counts.sum + (k - 1)).toString
    } else {
      (counts.take(border).sum + (k-1)) + "|" + (counts.drop(border + 1).sum + (k-1))
    }
  }

  def asPairs: Iterator[(Taxon, Int)] = taxa.iterator zip counts.iterator
}
