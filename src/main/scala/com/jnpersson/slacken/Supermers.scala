/*
 * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 */


package com.jnpersson.slacken

import com.jnpersson.discount.NTSeq
import com.jnpersson.discount.hash.{BucketId, InputFragment}
import com.jnpersson.discount.spark.AnyMinSplitter
import com.jnpersson.discount.util.NTBitArray

import scala.annotation.{switch, tailrec}
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/** A super-mer with a single minimizer.
 * @param id1 First part of the minimizer
 * @param id2 Second part of the minimizer
 * @param segment Sequence data
 */
final case class Supermer(id1: BucketId, id2: BucketId, segment: NTBitArray)

object Supermers {

  /**
   * Split a fragment into segments (by minimizer).
   * Ambiguous segments get a random minimizer. They will not be processed during classification,
   * but are needed at the end to create complete output
   * @return Pairs of (segment, flag) where flag indicates whether the segment was ambiguous.
   */
  def splitFragment(sequence: NTSeq, splitter: AnyMinSplitter): Iterator[(Supermer, SegmentFlag)] = {
    splitByAmbiguity(sequence, splitter.k).flatMap { case (ntseq, flag) =>
      flag match {
        case AMBIGUOUS_FLAG =>
          val numKmers = ntseq.length - (splitter.k - 1)
          Iterator((Supermer(Random.nextLong(), 0, NTBitArray(Array(), numKmers)), AMBIGUOUS_FLAG))
        case SEQUENCE_FLAG =>
          for {
            (_, hash, segment, _) <- splitter.splitEncode(ntseq)
          } yield (Supermer(hash.data(0), hash.dataOrBlank(1), segment), SEQUENCE_FLAG)
      }
    }
  }
   /**
    * Splits reads by hash (minimizer), including an ordinal, so that the ordering of a read can be reconstructed later,
    * after shuffling.
    * Also includes ambiguous segments at the correct location.
    * If we are processing a mate pair, a pseudo-sequence will indicate this at the correct location.
    */
  def splitFragment(f: InputFragment, splitter: AnyMinSplitter): Iterator[OrdinalSupermer] = {
    val tag = f.header
    val flaggedSegments = f.nucleotides2 match {
      case Some(nt2) =>
        //mate pair
        val emptySequence = NTBitArray(Array(), 0)
        val emptySupermer = Supermer(Random.nextLong(), 0, emptySequence)
        splitFragment(f.nucleotides, splitter) ++
          Iterator((emptySupermer, MATE_PAIR_BORDER_FLAG)) ++
            splitFragment(nt2, splitter)
      case None =>
        splitFragment(f.nucleotides, splitter)
    }

    for {
      ((segment, flag), index) <- flaggedSegments.zipWithIndex
      osws = OrdinalSupermer(segment, flag, index, tag)
    } yield osws
  }

  def isAmbiguous(c: Char): Boolean = {
    (c: @switch) match {
      case 'C' | 'T' | 'A' | 'G' | 'U' | 'c' | 't' | 'a' | 'g' | 'u' => false
      case _ => true
    }
  }

  /**
   * Split a read into maximally long fragments overlapping by (k-1) bases,
   * flagging those which contain ambiguous nucleotides. The purpose of this is to separate non-ambiguous
   * super-mers from ambiguous ones.
   *
   * @return Tuples of fragments and the sequence flag
   *         ([[AMBIGUOUS_FLAG]] if the fragment contains ambiguous nucleotides, otherwise [[SEQUENCE_FLAG]]).
   *         The fragments will be returned in order.
   */
  def splitByAmbiguity(r: NTSeq, k: Int): Iterator[(NTSeq, SegmentFlag)] =
    splitByAmbiguity(r, k, "", SEQUENCE_FLAG).iterator.
      filter(_._1.length >= k)

  /**
   * Split a read into fragments overlapping by (k-1 bases)
   *
   * @param r         Remaining subject to be classified. First character has not yet been judged to be
   *                  ambiguous/nonambiguous
   * @param k         Length of k-mers
   * @param building  Fragment currently being built (prior to 'r')
   * @param currentFlag Whether currently built fragment is ambiguous ([[AMBIGUOUS_FLAG]] or [[SEQUENCE_FLAG]])
   * @param acc       Result accumulator
   * @return Pairs of (sequence, ambiguous flag)
   */
  @tailrec
  def splitByAmbiguity(r: NTSeq, k: Int, building: NTSeq, currentFlag: Int,
                       acc: ArrayBuffer[(NTSeq, SegmentFlag)] = ArrayBuffer.empty): ArrayBuffer[(NTSeq, SegmentFlag)] = {
    if (r.isEmpty) {
      acc += ((building, currentFlag))
    } else {
      val i = r.indexWhere(isAmbiguous)
      if (i < k && i != -1) {
        //Not enough data for a k-mer before the next ambiguous letter.
        //Enter / stay in ambiguous mode
        splitByAmbiguity(r.substring(i + 1), k, building + r.substring(0, i + 1), AMBIGUOUS_FLAG, acc)
      } else if (currentFlag == AMBIGUOUS_FLAG) {
        //we have i >= k || i == -1, so we can get at least one k-mer.
        //finish the current ambiguous super-mer and revisit r in unambiguous mode.
        val endPart = (if (r.length >= (k - 1)) r.substring(0, k - 1) else r)
        splitByAmbiguity(r, k, "", SEQUENCE_FLAG, acc += ((building + endPart, AMBIGUOUS_FLAG)))
      } else if (i >= k) {
        //not in ambiguous mode
        //we found an ambiguous letter. Consume the unambiguous part and switch to ambiguous
        val splitAt = i - (k - 1)
        splitByAmbiguity(r.substring(i + 1), k, r.substring(splitAt, splitAt + k),
          AMBIGUOUS_FLAG, acc += ((building + r.substring(0, i), SEQUENCE_FLAG)))
      } else {
        //!ambiguous && i == -1
        //no ambiguous letter. We can add all of r to the current super-mer and terminate.
        acc += ((building + r, SEQUENCE_FLAG))
      }
    }
  }
}
