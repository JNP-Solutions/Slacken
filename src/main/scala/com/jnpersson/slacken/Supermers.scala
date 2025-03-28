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


package com.jnpersson.slacken

import com.jnpersson.kmers._
import com.jnpersson.kmers.minimizer._
import com.jnpersson.kmers.util.{BitRepresentation, NTBitArray}

import java.util
import scala.util.Random
import scala.util.matching.Regex

/** Helper functions for splitting segments into supermers in the presence of ambiguous data. */
final class Supermers(splitter: AnyMinSplitter, idLongs: Int) extends Serializable {
  val k = splitter.k

  private def randomMinimizer: Array[BucketId] =
    Array.fill(idLongs)(Random.nextLong())

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
        val emptySupermer = Supermer(Array(Random.nextLong()), emptySequence, 0)
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

  /** Convert an iterator of supermers into an iterator of ordinal spans, tracking distinct minimizers.
   */
  def spans(sms: Iterator[OrdinalSupermer], withTitle: Boolean, k: Int): Iterator[OrdinalSpan] =
    new Iterator[OrdinalSpan] {
      private var first = true
      private var lastMinimizer = Array[Long]()

      override def hasNext: Boolean =
        sms.hasNext

      override def next(): OrdinalSpan = {
        val x = sms.next()
        val flag = x.flag

        //Track whether two consecutive valid minimizers are distinct. This allows us to count the number of
        //hit groups later.
        val distinct =
          flag != AMBIGUOUS_FLAG && flag != MATE_PAIR_BORDER_FLAG &&
            (first || !util.Arrays.equals(x.segment.rank, lastMinimizer))
        val title = if (withTitle) x.seqTitle else null
        if (flag != AMBIGUOUS_FLAG && flag != MATE_PAIR_BORDER_FLAG) {
          lastMinimizer = x.segment.rank
        }

        first = false

        OrdinalSpan(x.segment.rank, distinct, x.segment.nucleotides.size - (k - 1),
          x.flag, x.ordinal, title)
      }
  }

  /** Ensure that the minimizer has the required length */
  private def padMinimizer(min: Array[Long]) = {
    if (min.length == idLongs) min else
      java.util.Arrays.copyOf(min, idLongs)
  }

  /**
   * Split a fragment into super-mers (by minimizer).
   * Ambiguous segments get a random minimizer (to distribute them well). They will not be processed during classification,
   * but are needed at the end to create complete output.
   * The array length of minimizers will be standardised.
   * @param sequence the fragment to split.
   * @return Pairs of (segment, flag) where flag indicates whether the segment was ambiguous.
   */
  def splitFragment(sequence: NTSeq): Iterator[(Supermer, SegmentFlag)] =
    for {
      (ntseq, flag, pos) <- splitByAmbiguity(sequence)
      if ntseq.length >= k
      sm <- flag match {
        case AMBIGUOUS_FLAG =>
          Iterator((Supermer(randomMinimizer, NTBitArray(Array(), ntseq.length), pos), AMBIGUOUS_FLAG))
        case SEQUENCE_FLAG =>
          for {
            Supermer(hash, segment, loc) <- splitter.splitEncode(ntseq)
          } yield (Supermer(padMinimizer(hash), segment, loc + pos), SEQUENCE_FLAG)
      }
    } yield sm

  /**
   * Split a sequence into maximally long segments that are either unambiguous or ambiguous.
   *
   * @param sequence the sequence to split.
   * @return Tuples of fragments, their sequence flag, and their position
   *         ([[AMBIGUOUS_FLAG]] if the fragment contains ambiguous nucleotides or is shorter than k,
   *         otherwise [[SEQUENCE_FLAG]]).
   */
  def splitByAmbiguity(sequence: NTSeq): Iterator[(NTSeq, SegmentFlag, Int)] =
    Supermers.splitByAmbiguity(sequence, k)

}

object Supermers {
  val nonAmbiguousRegex = "[actguACTGU\n\r]+".r

  /**
   * Split a sequence into maximally long segments that are either unambiguous with at least one k-mer, or ambiguous.
   *
   * @param sequence the sequence to split.
   * @param regex Regular expression to use for detecting non-ambiguous segments
   * @return Tuples of fragments, their sequence flag, and their position
   *         ([[AMBIGUOUS_FLAG]] for ambiguous segments, otherwise [[SEQUENCE_FLAG]]).
   */
  def splitByAmbiguity(sequence: NTSeq, k: Int): Iterator[(NTSeq, SegmentFlag, Int)] =
    new Iterator[(NTSeq, SegmentFlag, Int)]  {
      private val matches = nonAmbiguousRegex.findAllMatchIn(sequence).buffered
      private var at = 0
      def hasNext: Boolean =
        at < sequence.length

      def next: (NTSeq, SegmentFlag, Int) = {
        if (matches.hasNext && matches.head.start == at) {
          val m = matches.next()
          at = m.end
          val matchStr = m.toString()

          //We have to count the non-newline characters here since newlines can occur anywhere. Attempting
          //to count in the regex causes backtracking and stack overflow.
          val sufficient = enoughValidChars(matchStr, k)
          (matchStr, if (sufficient) SEQUENCE_FLAG else AMBIGUOUS_FLAG, m.start)
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

  def enoughValidChars(test: String, min: Int): Boolean = {
    var i = 0
    var c = 0
    while (i < test.length) {
      if (BitRepresentation.isValid(test.charAt(i))) c += 1
      if (c == min) return true
      i +=1
    }
    false
  }
}