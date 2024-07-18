/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */


package com.jnpersson.slacken

import com.jnpersson.kmers._
import com.jnpersson.kmers.minimizer._
import com.jnpersson.kmers.util.NTBitArray

import scala.util.Random
import scala.util.matching.Regex

/** A super-mer with a single minimizer.
 * @param minimizer the minimizer
 * @param segment Sequence data
 */
final case class Supermer(minimizer: Array[Long], segment: NTBitArray)

/** Helper functions for splitting segments into supermers in the presence of ambiguous data. */
final class Supermers(splitter: AnyMinSplitter, idLongs: Int) extends Serializable {
  val k = splitter.k

  def randomMinimizer: Array[BucketId] = {
    val r = Array.fill(idLongs)(0L)
    //leave the tail longs at zero to increase compressibility in the case of narrow m
    //(fewer values in the redundant columns)
    r(0) = Random.nextLong()
    r
  }

  /**
    * Splits reads by hash (minimizer), including an ordinal, so that the ordering inside a read can be reconstructed
    * later. Also includes ambiguous segments at the correct location.
    * If we are processing a mate pair, a pseudo-sequence will indicate this at the correct location.
    */
  def splitFragment(sequence: InputFragment): Iterator[OrdinalSupermer] = {
    val flaggedSegments = sequence.nucleotides2 match {
      case Some(nt2) =>
        //mate pair, insert the flag in the correct location and process both sides
        val emptySequence = NTBitArray(Array(), 0)
        val emptySupermer = Supermer(Array(Random.nextLong()), emptySequence)
        splitFragment(sequence.nucleotides) ++
          Iterator((emptySupermer, MATE_PAIR_BORDER_FLAG)) ++
            splitFragment(nt2)
      case None =>
        //single read
        splitFragment(sequence.nucleotides)
    }

    for {
      ((segment, flag), index) <- flaggedSegments.zipWithIndex
    } yield OrdinalSupermer(segment, flag, index, sequence.header)
  }

  /** Ensure that the minimizer has the required length */
  private def padMinimizer(min: NTBitArray) = {
    val r = new Array[Long](idLongs)
    var i = 0
    while (i < idLongs) {
      r(i) = min.dataOrBlank(i)
      i += 1
    }
    r
  }

  /**
   * Split a fragment into super-mers (by minimizer).
   * Ambiguous segments get a random minimizer. They will not be processed during classification,
   * but are needed at the end to create complete output
   * @param sequence the fragment to split.
   * @return Pairs of (segment, flag) where flag indicates whether the segment was ambiguous.
   */
  def splitFragment(sequence: NTSeq): Iterator[(Supermer, SegmentFlag)] =
    for {
      (ntseq, flag, _) <- splitByAmbiguity(sequence)
      if ntseq.length >= k
      sm <- flag match {
        case AMBIGUOUS_FLAG =>
          Iterator((Supermer(randomMinimizer, NTBitArray(Array(), ntseq.length)), AMBIGUOUS_FLAG))
        case SEQUENCE_FLAG =>
          for {
            (_, hash, segment, _) <- splitter.splitEncode(ntseq)
          } yield (Supermer(padMinimizer(hash), segment), SEQUENCE_FLAG)
      }
    } yield sm

  private val nonAmbiguousRegex = s"[actguACTGU]{$k,}".r

  /**
   * Split a sequence into maximally long segments that are either unambiguous or ambiguous.
   *
   * @param sequence the sequence to split.
   * @return Tuples of fragments, their sequence flag, and their position
   *         ([[AMBIGUOUS_FLAG]] if the fragment contains ambiguous nucleotides or is shorter than k,
   *         otherwise [[SEQUENCE_FLAG]]). The fragments will be returned in order.
   */
  def splitByAmbiguity(sequence: NTSeq): Iterator[(NTSeq, SegmentFlag, Int)] =
    Supermers.splitByAmbiguity(sequence, nonAmbiguousRegex)

}

object Supermers {
  def nonAmbiguousRegex(k: Int) = s"[actguACTGU]{$k,}".r

  def splitByAmbiguity(sequence: NTSeq, regex: Regex): Iterator[(NTSeq, SegmentFlag, Int)] = {

    new Iterator[(NTSeq, SegmentFlag, Int)]  {
      private val matches = regex.findAllMatchIn(sequence).buffered
      private var at = 0
      def hasNext: Boolean =
        at < sequence.length

      def next: (NTSeq, SegmentFlag, Int) = {
        if (matches.hasNext && matches.head.start == at) {
          val m = matches.next()
          at = m.end
          (m.toString(), SEQUENCE_FLAG, m.start)
        } else if (matches.hasNext) {
          val m = matches.head
          val r = (sequence.substring(at, m.start), AMBIGUOUS_FLAG, at)
          at = m.start
          r
        } else {
          val r = (sequence.substring(at, sequence.length), AMBIGUOUS_FLAG, at)
          at = sequence.length
          r
        }
      }
    }
  }
}